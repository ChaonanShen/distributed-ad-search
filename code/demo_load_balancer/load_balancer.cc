#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "service.grpc.pb.h"

static int port[3] = {50051, 50052, 50053};

class LoadBalancerImpl final : public service::AddOne::Service {
  grpc::Status AddOneMethod(grpc::ServerContext *context,
                            const service::Request *request,
                            service::Response *reply) override {
    static int i = 0;
    std::string server_address("0.0.0.0:");
    server_address += std::to_string(port[i]);
    i = (i + 1) % 3;

    std::unique_ptr<service::AddOne::Stub> stub(
        service::AddOne::NewStub(grpc::CreateChannel(
            server_address, grpc::InsecureChannelCredentials())));

    grpc::ClientContext client_context;

    return stub->AddOneMethod(&client_context, *request, reply);
  }
};

void RunLoadBalancer() {
  std::string server_address("0.0.0.0:6666");
  LoadBalancerImpl service;

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << "Load balancer listening on " << server_address << std::endl;

  server->Wait();
}

int main(int argc, char **argv) {
  RunLoadBalancer();

  return 0;
}
