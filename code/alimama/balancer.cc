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

// server1 ip1:50051 server2 ip2:50052 server3 ip3:50053
static std::string searchServerAddrs[3];

class LoadBalancerImpl final : public SearchService::Service {
  Status Search(ServerContext *context, const Request *request,
                Response *response) override {

    // 轮流转发给不同的机器
    static int i = 0;
    std::string server_address = searchServerAddrs[i];
    i = (i + 1) % 3;

    std::unique_ptr<SearchService::Stub> stub(
        SearchService::NewStub(grpc::CreateChannel(
            server_address, grpc::InsecureChannelCredentials())));

    grpc::ClientContext client_context;
    auto status = stub->Search(&client_context, *request, response);
    if (!status.ok()) {
      std::cout << "balancer receive response RPC failed" << std::endl;
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
  std::string key = std::string("/services/searchservice");

  LoadBalancerImpl service;
  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::unique_ptr<Server> server(builder.BuildAndStart());

  // TODO(scn)： 本地测试和线上测试这里要修改
  // 创建一个etcd客户端
  etcd::Client etcd("http://etcd:2379");

  // 等待三个server将数据都准备好再注册
  std::string dummy_str;
  splitStr(EtcdGetKVWait(etcd, "/node1"), searchServerAddrs[0], dummy_str);
  splitStr(EtcdGetKVWait(etcd, "/node2"), searchServerAddrs[1], dummy_str);
  splitStr(EtcdGetKVWait(etcd, "/node3"), searchServerAddrs[2], dummy_str);

  // 将服务地址注册到etcd中
  // 相当于 etcdctl put /services/searchservice ip:port
  auto response = etcd.set(key, external_address).get();
  if (response.is_ok()) {
    std::cout << "Service registration successful.\n";
  } else {
    std::cerr << "Service registration failed: " << response.error_message()
              << "\n";
  }

  std::cout << "Balancer listening on " << server_address << std::endl;
  server->Wait();
}

int main() {
  RunLoadBalancer();

  return 0;
}