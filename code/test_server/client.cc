#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "service.grpc.pb.h"

void RunClient() {
  std::string target_address("0.0.0.0:50051");
  service::Request request;
  request.set_number(5);

  service::Response reply;

  grpc::ClientContext context;

  std::unique_ptr<service::AddOne::Stub> stub(service::AddOne::NewStub(
      grpc::CreateChannel(target_address, grpc::InsecureChannelCredentials())));

  grpc::Status status = stub->AddOneMethod(&context, request, &reply);

  if (status.ok()) {
    std::cout << "AddOneMethod response: " << reply.number() << std::endl;
  } else {
    std::cout << "AddOneMethod failed." << std::endl;
  }
}

int main(int argc, char **argv) {
  RunClient();

  return 0;
}
