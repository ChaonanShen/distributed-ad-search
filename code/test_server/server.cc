#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "service.grpc.pb.h"

class AddOneImpl final : public service::AddOne::Service {
  grpc::Status AddOneMethod(grpc::ServerContext *context,
                            const service::Request *request,
                            service::Response *reply) override {
    reply->set_number(request->number() + 1);
    return grpc::Status::OK;
  }
};

void RunServer() {
  std::string server_address("0.0.0.0:50051");
  AddOneImpl service;

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  server->Wait();
}

int main(int argc, char **argv) {
  RunServer();

  return 0;
}
