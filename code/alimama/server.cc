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

#include "alimama.grpc.pb.h"

#include "async_server.h"

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
using alimama::proto::SearchService;

#include "util.h"

// Search Servers的ports
static const int PORTS[3] = {50051, 50052, 50053};

static int port = -1;
static int NODE_ID = 1;
// 其他
static std::string serverAddr[3];

static std::string getCurrentServerAddr() {
  return getLocalIP() + ":" + std::to_string(port);
}
static std::string getSearchServerAddr(int index) { return serverAddr[index]; }

// 内存索引 keyword -> Data
// using IndexType = std::unordered_multimap<uint64_t, Data>;
extern IndexType kw2data;

// 两浮点数在1e-6误差范围内认为是相等
auto floatEqual = [](float f1, float f2) -> bool {
  return std::abs(f1 - f2) < 1e-6;
};

/**
 * 吃了没怎么学grpc的亏，两个rpc服务在一个server中就能运行，不需要搞两套
 * Search将Request发给三个节点(使用SearchLocal方法)，SearchLocal就只需要查找本地有的那些keywords，找出最多topn+1个返回
 * 最终Search里将三个节点返回的合并选出最后topn
 * TODO(scn): 一个权衡 - 是rpc请求/回复中字段多一些 还是 本地查询更多一些
 * 就是网络带宽和本地磁盘带宽的比拼 -
 * 尤其如果之后索引能直接保存很多数据的话，那rpc(LocalResult)少带点信息就行
 * 目前我选择SearchLocal将返回所有最终排序里所需要的内容
 * SearchLocal返回各个keywords排名topn+1的字段
 */

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

std::unique_ptr<SearchService::Stub> stubs[3];

void doSearch(const Request *request, Response *response);
void doSearchLocal(const Request *request, ResponseLocal *response);

int main(int argc, char **argv) {
  static_assert(sizeof(Data) == 22);
  // 紧凑的保存Data

  port = getPort(argc, argv);

  auto str = std::getenv("NODE_ID");
  if (str) {
    NODE_ID = atoi(str);
  }

  // TODO(scn): 这里假设每个节点大概1.5亿行数据
  kw2data.reserve(150000000);

  // TODO(scn): 数据处理逻辑
  // 将csv中对应数据读取出来 保存到磁盘上 同时建立内存索引

  std::string ifilename = "/data/data.csv";

  prepareData(NODE_ID, kw2data, ifilename);

  // 打印下内存索引
  // for (auto it : kw2data) {
  //   std::cout << it.first << " -> " << std::endl;
  //   it.second.print();
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

  stubs[0] = SearchService::NewStub(grpc::CreateChannel(
      getSearchServerAddr(0), grpc::InsecureChannelCredentials()));
  stubs[1] = SearchService::NewStub(grpc::CreateChannel(
      getSearchServerAddr(1), grpc::InsecureChannelCredentials()));
  stubs[2] = SearchService::NewStub(grpc::CreateChannel(
      getSearchServerAddr(2), grpc::InsecureChannelCredentials()));

  for (int i = 0; i < 3; i++) {
    std::cout << "server" << i << " " << serverAddr[i] << std::endl;
  }

  // 运行个异步rpc server
  AsyncServerImpl server;
  server.Run(port);

  return 0;
}

ThreadPool pool(64);

void doSearch(const Request *request, Response *response) {
  auto topn = request->topn();

  ResponseLocal resp_local[3];

  std::vector<std::future<void>> futures;
  futures.reserve(3);

  for (int i = 0; i < 3; i++) {
    futures.push_back(pool.enqueue(
        [](int i, const Request *request, ResponseLocal &resp) {
          ClientContext context;
          Status status = stubs[i]->SearchLocal(&context, *request, &resp);
          if (!status.ok()) {
            std::cout << getCurrentServerAddr() << " subrequest -> "
                      << getSearchServerAddr(i) << " SearchLocal RPC failed"
                      << std::endl;
          }
        },
        i, request, std::ref(resp_local[i])));
  }

  for (auto &future : futures) {
    future.get();
  }

  std::vector<LocalResult> result;
  std::vector<float> prices;

  // 三个节点上取出的所有广告单元进行排序
  std::vector<LocalResult> preResult;
  preResult.reserve(3 * (topn + 1));
  for (int i = 0; i < 3; i++) {
    auto &resp = resp_local[i];
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

  for (int i = 0; i < prices.size() && i < topn; i++) {
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
    if (kw2data.find(keyword) == kw2data.end())
      continue;
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
}
