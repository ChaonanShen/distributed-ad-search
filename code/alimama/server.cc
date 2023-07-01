#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

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

#include <cmath>
#include <vector>

#include "util.h"

static int port = -1;

class SearchServiceImpl final : public SearchService::Service {
  // 处理Request形成Response的函数
  Status Search(ServerContext *context, const Request *request,
                Response *response) override {
    // 作为示例，我们只是简单地返回一些假数据。
    // 假设我们找到了两个广告单元
    response->add_adgroup_ids(12345);
    response->add_adgroup_ids(67890);

    std::cout << "server-" << port << " receive request" << std::endl;

    // 对应的价格
    response->add_prices(100);
    response->add_prices(200);
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

  server->Wait();
}

int main(int argc, char **argv) {
  port = getPort(argc, argv);
  RunServer(port);

  return 0;
}
