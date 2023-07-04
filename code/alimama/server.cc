#include <cmath>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <semaphore.h>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include <etcd/Client.hpp>
#include <etcd/Response.hpp>

// #ifdef BAZEL_BUILD
// #include "examples/protos/alimama.grpc.pb.h"
// #else
#include "alimama.grpc.pb.h"
// #endif

using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;

using alimama::proto::AdgroupResp;
using alimama::proto::Request;
using alimama::proto::Response;
using alimama::proto::ResponseLocal;
using alimama::proto::SearchLocalService;
using alimama::proto::SearchService;

#include "util.h"

// Search Servers的ports
static const int PORTS[3] = {50051, 50052, 50053};
// SearchLocal Servers的ports
static const int PORTS2[3] = {50061, 50062, 50063};

static int port = -1;
static int NODE_ID = 1;
// 其他
static std::string searchServersAddr[3];
static std::string searchLocalServersAddr[3];

static std::string getCurrentSearchServerAddr() {
  return getLocalIP() + ":" + std::to_string(port);
}

static std::string getCurrentSearchLocalServerAddr() {
  return getLocalIP() + ":" + std::to_string(port + 10);
}

static std::string getSearchLocalServerAddr(int index) {
  return searchLocalServersAddr[index];
}

// mmap文件指针
extern void *fileData;

// keyword -> 文件中offset
// using IndexType = std::unordered_multimap<uint64_t, uint64_t>;
extern IndexType kw2offset;

// 两浮点数在1e-6误差范围内认为是相等
auto floatEqual = [](float f1, float f2) -> bool {
  return std::abs(f1 - f2) < 1e-6;
};

/**
 * 两个server
 * SearchService将Request分割发给不同servers，然后再合起来，排序返回
 * SearchLocalService收到的Request里面keyword一定在本地，返回个map，里面是不同广告单元(相当于是每个keyword的广告)以及之后排序需要的信息
 * TODO(scn): 一个权衡 - 是rpc请求/回复中字段多一些 还是 本地查询更多一些
 * 就是网络带宽和本地磁盘带宽的比拼
 * 目前我选择SearchLocal将返回所有最终排序里所需要的内容
 * SearchLocal返回各个keywords排名topn+1的字段
 */

class SearchLocalServiceImpl final : public SearchLocalService::Service {
  // 处理确保所有Request中的keyword一定在本地存在
  // 需要返回的是topn+1的keyword+排序分数
  Status SearchLocal(ServerContext *context, const Request *request,
                     ResponseLocal *response) override {
    std::cout << "SearchLocalServer:" << getCurrentSearchLocalServerAddr()
              << " receive request" << std::endl;

    // 已经确保所有Request中的keywords都是在本地
    uint64_t hour = request->hour(), topn = request->topn();
    float context_vec[2] = {request->context_vector(0),
                            request->context_vector(1)};

    // TODO(scn): 多个关键词甚至可以并行执行，反正都是只读的！

    // 反正各种关键词匹配到的广告都搜集起来，然后再合并选出topN的返回
    // 所以其实各个关键词只需要自己各自匹配topn个就够了
    std::vector<DataScore> preResult;
    for (int i = 0; i < request->keywords().size(); i++) {
      uint64_t keyword = request->keywords(i);

      auto vec = CalcAdgroupId(keyword, hour, topn, context_vec);
      preResult.reserve(preResult.size() + vec.size());
      for (auto &ds : vec) {
        preResult.emplace_back(ds);
      }
    }

    // 先按照分数排序
    std::sort(preResult.begin(), preResult.end(),
              [](const DataScore &ds1, const DataScore &ds2) {
                // 排序分数高的在前 -> 排序分数相同则出价低的在前 ->
                // 否则adgroup_id大的在前
                if (!floatEqual(ds1.score, ds2.score))
                  return ds1.score > ds2.score;
                else if (ds1.data.keyword_prices != ds2.data.keyword_prices)
                  return ds1.data.keyword_prices < ds2.data.keyword_prices;
                else
                  return ds1.data.adgroup_id > ds2.data.adgroup_id;
              });

    // 从preResult中选出topN - 要去重
    std::vector<DataScore> result;
    result.reserve(topn + 1);

    std::set<uint64_t> exist_adgroup_ids;
    int count = 0;
    for (int i = 0; i < preResult.size() && count < topn + 1; i++) {
      const DataScore &ds = preResult[i];
      if (!exist_adgroup_ids.count(ds.data.adgroup_id)) {
        // 没有重复的广告单元
        exist_adgroup_ids.insert(ds.data.adgroup_id);
        count++;
        result.push_back(ds);
      }
    }

    // 不用再计算最后的出价，直接把result排序号的最多topn+1个元素返回过去
    for (auto res : result) {
      AdgroupResp *adgroup_resp = response->add_array();
      adgroup_resp->set_adgroup_id(res.data.adgroup_id);
      adgroup_resp->set_ctr(GetCTR(res.data, context_vec[0], context_vec[1]));
      adgroup_resp->set_score(res.score);
      adgroup_resp->set_price(res.data.keyword_prices);
    }
    return Status::OK;
  }
};

// SearchLocal返回的结果
struct LocalResult {
  uint64_t adgroup_id;
  float ctr;
  float score;
  uint16_t price;
  void print() {
    std::cout << "adgroup_id=" << adgroup_id << " ctr=" << ctr
              << " score=" << score << " price=" << price << std::endl;
  }
};

class SearchServiceImpl final : public SearchService::Service {
  // 处理Request形成Response的函数
  Status Search(ServerContext *context, const Request *request,
                Response *response) override {
    std::cout << "SearchServer:" << getCurrentSearchServerAddr()
              << " receive request" << std::endl;

    uint64_t hour = request->hour(), topn = request->topn();
    float context_vec[2] = {request->context_vector(0),
                            request->context_vector(1)};

    std::vector<uint64_t> keywords[3]; // 分给三个server的
    keywords[0].reserve(request->keywords().size());
    keywords[1].reserve(request->keywords().size());
    keywords[2].reserve(request->keywords().size());

    for (int i = 0; i < request->keywords().size(); i++) {
      uint64_t keyword = request->keywords(i);
      keywords[hashKeyword(keyword)].push_back(keyword);
    }

    ResponseLocal resp_local[3];

    std::vector<std::thread> threads;
    threads.reserve(3);

    for (int i = 0; i < 3; i++) {
      // 形成三个Request用SearchLocal分别调用三个server的rpc
      // TODO(scn): 引入线程池 - 通过性能瓶颈分析后决定是否进行
      if (keywords[i].empty())
        continue;
      threads.emplace_back(
          [](int i, uint64_t hour, uint64_t topn, float context_vec[2],
             const std::vector<uint64_t> &keywords, ResponseLocal &resp) {
            Request req;

            for (auto keyword : keywords)
              req.add_keywords(keyword);
            req.add_context_vector(context_vec[0]);
            req.add_context_vector(context_vec[1]);
            req.set_hour(hour);
            req.set_topn(topn);

            ClientContext context;
            auto remote_addr = getSearchLocalServerAddr(i);
            std::unique_ptr<SearchLocalService::Stub> stub(
                SearchLocalService::NewStub(grpc::CreateChannel(
                    remote_addr, grpc::InsecureChannelCredentials())));
            Status status = stub->SearchLocal(&context, req, &resp);
            if (!status.ok()) {
              std::cout << getCurrentSearchServerAddr() << " subrequest -> "
                        << remote_addr << " SearchLocal RPC failed"
                        << std::endl;
            } else {
              std::cout << getCurrentSearchServerAddr() << " subrequest -> "
                        << remote_addr << " SearchLocal RPC ok" << std::endl;
            }
          },
          i, hour, topn, context_vec, std::cref(keywords[i]),
          std::ref(resp_local[i]));
      // 线程中引用参数一定要用std::cref和std::ref传递，这个经常忘了
    }

    for (auto &t : threads) {
      t.join();
    }

    // 三个节点上取出的所有广告单元进行排序
    std::vector<LocalResult> preResult;
    preResult.reserve(3 * (topn + 1));
    for (int i = 0; i < 3; i++) {
      auto &resp = resp_local[i];
      const int sz = resp.array_size();
      if (sz == 0)
        continue;

      std::cout << "SearchLocal respLocal[" << i << "] size=" << sz
                << std::endl;
      for (int i = 0; i < sz; i++) {
        preResult.emplace_back(
            LocalResult{resp.array(i).adgroup_id(), resp.array(i).ctr(),
                        resp.array(i).score(),
                        static_cast<uint16_t>(resp.array(i).price())});
      }
    }

    // 先按照排序分数排序
    // 其实跟SearchLocal里排序方式一样，只不过是对象类型不一样
    // 先按照分数排序
    std::sort(preResult.begin(), preResult.end(),
              [](const LocalResult &ds1, const LocalResult &ds2) {
                // 排序分数高的在前 -> 排序分数相同则出价低的在前 ->
                // 否则adgroup_id大的在前
                if (!floatEqual(ds1.score, ds2.score))
                  return ds1.score > ds2.score;
                else if (ds1.price != ds2.price)
                  return ds1.price < ds2.price;
                else
                  return ds1.adgroup_id > ds2.adgroup_id;
              });

    // 去重并选出最后topn+1(可能不足topn+1)
    std::vector<LocalResult> result;
    result.reserve(topn + 1);

    std::set<uint64_t> exist_adgroup_ids;
    int count = 0;
    for (int i = 0; i < preResult.size() && count < topn + 1; i++) {
      const LocalResult &ds = preResult[i];
      if (!exist_adgroup_ids.count(ds.adgroup_id)) {
        // 没有重复的广告单元
        exist_adgroup_ids.insert(ds.adgroup_id);
        count++;
        result.push_back(ds);
      }
    }

    // 重新计算出价结果（原先的score只是排序分数）
    // 先确定最后一个元素的计费价格
    std::vector<float> prices; // 最后的出价
    prices.resize(result.size());
    if (!prices.empty()) {
      if (result.size() <= topn) {
        // 若召回的广告集合少于等于请求的topn时，最后一名的计费价格使用其自身的出价
        prices.back() = result.back().price;
      } else {
        assert(result.size() == topn + 1);
        // 否则直接用topn+1名的排序分数
        prices.back() = result.back().score;
      }
    }

    // 最后一名分数确定了，其他的依次计算
    for (int i = ((int)prices.size() - 2); i >= 0; i--) {
      prices[i] = result[i + 1].score / result[i].ctr;
    }

    // 所有参与排序的分数结果
    std::cout << "sort size = " << prices.size() << std::endl;
    for (int i = 0; i < prices.size(); i++) {
      result[i].print();
    }

    for (int i = 0; i < prices.size() && i < topn; i++) {
      response->add_adgroup_ids(result[i].adgroup_id);
      response->add_prices(static_cast<uint64_t>(std::round(prices[i])));
    }

    return Status::OK;
  }
};

void RunServers(int port) {
  sem_t sem;
  sem_init(&sem, 0, 0);

  // 运行两个rpc server - t1是会split
  // Request然后分发给不同server的，t2是确保Request一定都是本地的keywords的
  std::thread t1(
      [port](sem_t *sem) {
        std::string server_address = getCurrentSearchServerAddr();

        SearchServiceImpl service;
        ServerBuilder builder;
        builder.AddListeningPort(server_address,
                                 grpc::InsecureServerCredentials());
        builder.RegisterService(&service);

        std::unique_ptr<Server> server(builder.BuildAndStart());

        std::cout << "Search Server listening on " << server_address
                  << std::endl;

        // 信号量同步点，也就是说线程中执行到这个位置，RunServers才能继续
        sem_post(sem);

        server->Wait();
      },
      &sem);

  std::thread t2(
      [port](sem_t *sem) {
        std::string server_address = getCurrentSearchLocalServerAddr();

        SearchLocalServiceImpl service;
        ServerBuilder builder;
        builder.AddListeningPort(server_address,
                                 grpc::InsecureServerCredentials());
        builder.RegisterService(&service);

        std::unique_ptr<Server> server(builder.BuildAndStart());

        std::cout << "SearchLocal Server listening on " << server_address
                  << std::endl;

        // 信号量同步点，也就是说线程中执行到这个位置，RunServers才能继续
        sem_post(sem);

        server->Wait();
      },
      &sem);

  // 同步点：等两个线程都执行到同步位置才能继续进行注册
  sem_wait(&sem);
  sem_wait(&sem);

  // server运行起来，可以注册了
  // 创建一个etcd客户端
  etcd::Client etcd("http://etcd:2379");
  std::string key = "/node" + std::to_string(NODE_ID);
  EtcdSetKV(etcd, key,
            getCurrentSearchServerAddr() + " " +
                getCurrentSearchLocalServerAddr());

  std::cout << "server-" << port << " registeration success" << std::endl;

  splitStr(EtcdGetKVWait(etcd, "/node1"), searchServersAddr[0],
           searchLocalServersAddr[0]);
  splitStr(EtcdGetKVWait(etcd, "/node2"), searchServersAddr[1],
           searchLocalServersAddr[1]);
  splitStr(EtcdGetKVWait(etcd, "/node3"), searchServersAddr[2],
           searchLocalServersAddr[2]);

  t1.join();
  t2.join();
}

int main(int argc, char **argv) {
  port = getPort(argc, argv);

  auto str = std::getenv("NODE_ID");
  if (str) {
    NODE_ID = atoi(str);
  }

  // TODO(scn): 数据处理逻辑
  // 将csv中对应数据读取出来 保存到磁盘上 同时建立内存索引

  std::string ifilename = "/data/data.csv";
  std::string ofilename = std::string("savedFile") + std::to_string(NODE_ID);

  prepareData(NODE_ID, kw2offset, ifilename, ofilename);

  // 打印下内存索引
  for (auto it : kw2offset) {
    std::cout << it.first << " -> " << it.second << std::endl;
  }

  // 生成mmap
  int fd = open(ofilename.c_str(), O_RDONLY);
  if (fd < 0) {
    std::cerr << "Error opening file: " << ofilename << std::endl;
    return -1;
  }

  struct stat fileInfo;
  if (fstat(fd, &fileInfo) < 0) {
    std::cerr << "Error getting file size." << std::endl;
    close(fd);
    return -1;
  }
  off_t fileSize = fileInfo.st_size;
  fileData = mmap(nullptr, fileSize, PROT_READ, MAP_PRIVATE, fd, 0);
  if (fileData == MAP_FAILED) {
    std::cerr << "Error mapping file to memory." << std::endl;
    close(fd);
    return -1;
  }

  // 运行server，接受请求
  RunServers(port);

  return 0;
}
