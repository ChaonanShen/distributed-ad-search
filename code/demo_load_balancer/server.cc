#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <unistd.h>

#include <grpcpp/grpcpp.h>

#include "service.grpc.pb.h"

static int port = 50051;

class AddOneImpl final : public service::AddOne::Service {
  grpc::Status AddOneMethod(grpc::ServerContext *context,
                            const service::Request *request,
                            service::Response *reply) override {
    reply->set_number(request->number() + 1);
    std::cout << "server-" << port << " receive request" << std::endl;
    return grpc::Status::OK;
  }
};

void RunServer() {
  std::string server_address("0.0.0.0:");
  server_address += std::to_string(port);
  AddOneImpl service;

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  server->Wait();
}

// 接收-p 50051这样的指定端口的参数解析
int getPort(int argc, char **argv) {
  int opt;
  while ((opt = getopt(argc, argv, "p:")) != -1) {
    switch (opt) {
    case 'p':
      port = std::atoi(optarg);
      break;
    default: /* '?' */
      std::cerr << "Usage: " << argv[0] << " [-p port]" << std::endl;
      exit(EXIT_FAILURE);
    }
  }
  return port;
}

int main(int argc, char **argv) {
  port = getPort(argc, argv);

  RunServer();

  return 0;
}
