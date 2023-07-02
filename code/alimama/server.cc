#include <cmath>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include <etcd/Client.hpp>
#include <etcd/Response.hpp>

#ifdef BAZEL_BUILD
#include "examples/protos/alimama.grpc.pb.h"
#else
#include "alimama.grpc.pb.h"
#endif

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;

using alimama::proto::Request;
using alimama::proto::Response;
using alimama::proto::SearchService;

#include "util.h"

static int port = -1;
static int NODE_ID = 1;

// mmap文件指针
extern void *fileData;

// keyword -> 文件中offset
// using IndexType = std::unordered_multimap<uint64_t, uint64_t>;
extern IndexType kw2offset;

class SearchServiceImpl final : public SearchService::Service {
  // 处理Request形成Response的函数
  Status Search(ServerContext *context, const Request *request,
                Response *response) override {
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
                if (ds1.score != ds2.score)
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

    // 重新计算出价结果（原先的score只是排序分数）
    // 先确定最后一个元素的计费价格
    std::vector<float> prices; // 最后的出价
    prices.resize(result.size());
    if (!prices.empty()) {
      if (result.size() <= topn) {
        // 若召回的广告集合少于等于请求的topn时，最后一名的计费价格使用其自身的出价
        prices.back() = result.back().data.keyword_prices;
      } else {
        assert(result.size() == topn + 1);
        // 否则直接用topn+1名的排序分数
        prices.back() = result.back().score;
      }
    }

    // 最后一名分数确定了，其他的依次计算
    for (int i = ((int)prices.size() - 2); i >= 0; i--) {
      prices[i] = prices[i + 1] /
                  GetCTR(result[i].data, context_vec[0], context_vec[1]);
    }

    for (int i = 0; i < prices.size() && i < topn; i++) {
      response->add_adgroup_ids(result[i].data.adgroup_id);
      response->add_prices(static_cast<uint64_t>(std::round(prices[i])));
    }

    return Status::OK;
  }
};

void RunServer(int port) {
  std::string server_address(std::string("0.0.0.0:") + std::to_string(port));

  SearchServiceImpl service;
  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::unique_ptr<Server> server(builder.BuildAndStart());

  std::cout << "Server listening on " << server_address << std::endl;

  // server运行起来，可以注册了
  // 创建一个etcd客户端
  etcd::Client etcd("http://etcd:2379");
  std::string key = "/node" + std::to_string(NODE_ID);
  EtcdSetKV(etcd, key, "");

  server->Wait();
}

int main(int argc, char **argv) {
  port = getPort(argc, argv);

  auto str = std::getenv("NODE_ID");
  if (str) {
    NODE_ID = atoi(str);
  }

  // TODO(scn): 数据处理逻辑
  // 将csv中对应数据读取出来 保存到磁盘上 同时建立内存索引
  // std::string ifilename = "../../data/data.csv";
  std::string ifilename = "/data/data.csv";
  std::string ofilename = "savedFile";

  prepareData(NODE_ID, kw2offset, ifilename, ofilename);

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
  RunServer(port);

  return 0;
}
