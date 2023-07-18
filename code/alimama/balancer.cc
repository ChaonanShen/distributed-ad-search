#include <iostream>
#include <memory>
#include <string>

#include <etcd/Client.hpp>
#include <grpcpp/grpcpp.h>

#include "alimama.grpc.pb.h"

#include "util.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;

using alimama::proto::Request;
using alimama::proto::Response;
using alimama::proto::SearchService;

// TODO(scn): 貌似grpc直接提供balancer方法

// server1 ip1:50051 server2 ip2:50052 server3 ip3:50053
static std::string searchServerAddrs[3];

#if RUN_REMOTE
// 创建一个etcd客户端
etcd::Client etcd_client("http://etcd:2379");
#endif

// 一个stub&channel背后其实有很多物理连接
std::vector<std::unique_ptr<SearchService::Stub>> lb_stubs;

class LoadBalancerImpl final : public SearchService::Service {
  Status Search(ServerContext *context, const Request *request,
                Response *response) override {
    // 轮流转发给不同的机器
    static std::atomic<int> i = 0;
    i = (i + 1) % lb_stubs.size();

    // TODO(scn): 这里也是同步的，改成异步
    grpc::ClientContext client_context;
    auto status = lb_stubs[i]->Search(&client_context, *request, response);
    if (!status.ok()) {
      // TODO(scn): 如果检测到是channel状态出问题，就重新更换！
      // 目前好像就遇到过一次channel出错的
      std::cout << "balancer RPC Failed" << std::endl;
    }

    return status;
  }
};

void RunLoadBalancer() {
  std::string local_ip = getLocalIP();
  std::cout << "local_ip " << local_ip << std::endl;

  constexpr int kPort = 56789;
  // external_address暴露给外界的ip:port，server_address就是0.0.0.0:port
  std::string external_address =
      local_ip + std::string(":") + std::to_string(kPort);
  std::string server_address("0.0.0.0:" + std::to_string(kPort));

  LoadBalancerImpl service;
  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::unique_ptr<Server> server(builder.BuildAndStart());

#if RUN_REMOTE
  // 将服务地址注册到etcd中
  // 相当于 etcdctl put /services/searchservice ip:port
  std::string key = std::string("/services/searchservice");
  auto response = etcd_client.set(key, external_address).get();
  if (response.is_ok()) {
    std::cout << "Service registration successful.\n";
  } else {
    std::cerr << "Service registration failed: " << response.error_message()
              << "\n";
  }
#endif

  std::cout << "Balancer listening on " << server_address << std::endl;
  server->Wait();
}

int main() {
#if RUN_REMOTE
  // 等待三个server将数据都准备好
  searchServerAddrs[0] = EtcdGetKVWait(etcd_client, "/node1");
  searchServerAddrs[1] = EtcdGetKVWait(etcd_client, "/node2");
  searchServerAddrs[2] = EtcdGetKVWait(etcd_client, "/node3");
#endif

  for (int i = 0; i < 3; i++) {
    std::cout << "balancer found server" << i << " " << searchServerAddrs[i]
              << std::endl;
  }

  for (int i = 0; i < 3; i++) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
        searchServerAddrs[i], grpc::InsecureChannelCredentials());
    lb_stubs.emplace_back(SearchService::NewStub(channel));
  }

  RunLoadBalancer();

  return 0;
}