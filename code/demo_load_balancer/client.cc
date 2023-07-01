#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "service.grpc.pb.h"

void RunClient() {
  while (1) {
    std::string target_address("0.0.0.0:6666");

    service::Request request;
    request.set_number(5);

    service::Response reply;

    grpc::ClientContext context;

    std::unique_ptr<service::AddOne::Stub> stub(
        service::AddOne::NewStub(grpc::CreateChannel(
            target_address, grpc::InsecureChannelCredentials())));

    grpc::Status status = stub->AddOneMethod(&context, request, &reply);

    if (status.ok()) {
      std::cout << "AddOneMethod response: " << reply.number() << std::endl;
    } else {
      std::cout << "AddOneMethod failed." << std::endl;
    }

    sleep(1);
  }
}

int main(int argc, char **argv) {
  RunClient();

  return 0;
}
