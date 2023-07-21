#include <bits/stdc++.h>

#include <cstdlib>
#include <semaphore.h>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include <etcd/Client.hpp>
#include <etcd/Response.hpp>

#include "alimama.grpc.pb.h"

#include "async_server.h"

using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;

using alimama::proto::AdgroupResp;
using alimama::proto::Request;
using alimama::proto::Response;
using alimama::proto::ResponseLocal;
using alimama::proto::SearchService;

#include "util.h"

// 我任务两种任务类型的线程池分开比较好，不然两种任务可能相互阻塞
// io密集型的任务(doSearch)的线程池 - 数量大一些，毕竟大多数都是阻塞的
ThreadPool tp_io(tp_io_num);
// cpu密集型的任务(doSearchLocal)的线程池 - 数量不超过cpu个数
ThreadPool tp_cpu(tp_cpu_num);

// Search Servers的ports
static const int PORTS[3] = {50051, 50052, 50053};

extern int port;
static int NODE_ID = 1;

extern std::string serverAddr[3];

// 内存索引 keyword -> Data
// using IndexType = std::unordered_multimap<uint64_t, Data>;
extern IndexType kw2index;

// 两浮点数在1e-6误差范围内认为是相等
auto floatEqual = [](float f1, float f2) -> bool {
  return std::abs(f1 - f2) < 1e-6;
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

void doSearchLocal(const Request *request, ResponseLocal *response);
void doSearchMerge(const Request *request, Response *response,
                   ResponseLocal resp_local[3]);

int main(int argc, char **argv) {
  // TODO(scn): 加上个keyword的话容量暴涨，从21->29，能不能列存？
  static_assert(sizeof(Data) == 29);
  // 紧凑的保存Data

  port = getPort(argc, argv);

  auto str = std::getenv("NODE_ID");
  if (str) {
    NODE_ID = atoi(str);
  }

  // TODO(scn): 这里假设每个节点大概4亿行数据
  datas.reserve(400000000);

  // TODO(scn): 数据处理逻辑
  // 将csv中对应数据读取出来 保存到磁盘上 同时建立内存索引

  std::string ifilename = "/data/data.csv";

  prepareData(NODE_ID, ifilename, datas);

  // datas排序并建立kw2index
  sort(datas.begin(), datas.end(),
       [](const Data &d1, const Data &d2) { return d1.keyword < d2.keyword; });
  kw2index.reserve(datas.size());

  if (!datas.empty())
    kw2index[datas[0].keyword] = 0;
  uint64_t last_keyword = datas[0].keyword;
  for (int i = 1; i < datas.size(); i++) {
    auto keyword = datas[i].keyword;
    if (keyword != last_keyword) {
      kw2index[keyword] = i;
      last_keyword = keyword;
    }
  }

  // 打印下内容 - keyword->数组下标
  // std::cout << "datas:" << std::endl;
  // for (auto entry : datas) {
  //   entry.print();
  // }
  // std::cout << "kw2index:" << std::endl;
  // for (auto it : kw2index) {
  //   std::cout << it.first << " -> " << it.second << std::endl;
  // }

#if RUN_REMOTE
  // 首先知道自己的ip:port和其他节点的ip:port
  etcd::Client etcd("http://etcd:2379");
  std::string key = "/node" + std::to_string(NODE_ID);
  EtcdSetKV(etcd, key, getCurrentServerAddr());

  std::cout << "server-" << port << " registeration success" << std::endl;

  serverAddr[0] = EtcdGetKVWait(etcd, "/node1");
  serverAddr[1] = EtcdGetKVWait(etcd, "/node2");
  serverAddr[2] = EtcdGetKVWait(etcd, "/node3");
#endif

  MakeupStubs();

  for (int i = 0; i < 3; i++) {
    std::cout << "server" << i << " " << serverAddr[i] << std::endl;
  }

  // 运行个异步rpc server
  AsyncServerImpl server;
  server.Run(port);

  return 0;
}

void doSearchMerge(const Request *request, Response *response,
                   ResponseLocal resp_local[3]) {
  // topn可能会出现随机值！说明request可能已经没了
  auto topn = request->topn();
  std::vector<LocalResult> result;
  std::vector<float> prices;

  // 三个节点上取出的所有广告单元进行排序
  std::vector<LocalResult> preResult;
  preResult.reserve(3 * (topn + 1));
  for (int idx = 0; idx < 3; idx++) {
    auto &resp = resp_local[idx];
    const int sz = resp.array_size();
    if (sz == 0)
      continue;

    for (int i = 0; i < sz; i++) {
      preResult.emplace_back(LocalResult{
          resp.array(i).adgroup_id(), resp.array(i).ctr(),
          resp.array(i).score(), static_cast<uint16_t>(resp.array(i).price())});
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
  result.reserve(topn + 1);

  absl::flat_hash_set<uint64_t> exist_adgroup_ids;
  exist_adgroup_ids.reserve(topn + 1);
  int count = 0;
  for (int i = 0; i < preResult.size() && count < topn + 1; i++) {
    const LocalResult &ds = preResult[i];
    if (!exist_adgroup_ids.count(ds.adgroup_id)) {
      // 没有重复的广告单元
      exist_adgroup_ids.insert(ds.adgroup_id);
      count++;
      result.emplace_back(ds);
    }
  }

  // 重新计算出价结果（原先的score只是排序分数）
  // 先确定最后一个元素的计费价格
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

  // 打印所有参与排序的分数结果
  // std::cout << "sort size = " << prices.size() << std::endl;
  // for (int i = 0; i < prices.size(); i++) {
  //   result[i].print();
  // }

  int num = std::min(prices.size(), topn);
  response->mutable_adgroup_ids()->Reserve(num);
  response->mutable_prices()->Reserve(num);
  for (int i = 0; i < num; i++) {
    response->add_adgroup_ids(result[i].adgroup_id);
    response->add_prices(static_cast<uint64_t>(std::round(prices[i])));
  }
}

void doSearchLocal(const Request *request, ResponseLocal *response) {
  // 不在本地的keywords直接跳过
  uint64_t hour = request->hour(), topn = request->topn();
  float context_vec[2] = {request->context_vector(0),
                          request->context_vector(1)};

  // TODO(scn): 多个关键词甚至可以并行执行，反正都是只读的！

  // 反正各种关键词匹配到的广告都搜集起来，然后再合并选出topN的返回
  // 所以其实各个关键词只需要自己各自匹配topn个就够了
  // 毕竟keywords可达上百个
  std::vector<DataScore> preResult;
  for (int i = 0; i < request->keywords().size(); i++) {
    uint64_t keyword = request->keywords(i);
    // TODO(scn): 先用bloom fitler过滤调那些不存在的keyword
    // 毕竟有2/3的keywords不存在
    if (kw2index.find(keyword) == kw2index.end())
      continue;
    std::vector<DataScore> vec;
    CalcAdgroupId(keyword, hour, topn, context_vec, vec);
    // 不需要手动reserve
    preResult.insert(preResult.end(), vec.begin(), vec.end());
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

  absl::flat_hash_set<uint64_t> exist_adgroup_ids;
  exist_adgroup_ids.reserve(topn + 1);
  int count = 0;
  for (int i = 0; i < preResult.size() && count < topn + 1; i++) {
    const DataScore &ds = preResult[i];
    if (!exist_adgroup_ids.count(ds.data.adgroup_id)) {
      // 没有重复的广告单元
      exist_adgroup_ids.insert(ds.data.adgroup_id);
      count++;
      result.emplace_back(ds);
    }
  }

  // 不用再计算最后的出价，直接把result排序号的最多topn+1个元素返回过去
  response->mutable_array()->Reserve(result.size());
  for (auto &res : result) {
    AdgroupResp *adgroup_resp = response->add_array();
    adgroup_resp->set_adgroup_id(res.data.adgroup_id);
    adgroup_resp->set_ctr(GetCTR(res.data, context_vec[0], context_vec[1]));
    adgroup_resp->set_score(res.score);
    adgroup_resp->set_price(res.data.keyword_prices);
  }
}
