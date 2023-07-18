#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "alimama.grpc.pb.h"
#include <boost/lockfree/queue.hpp>
#include <etcd/Client.hpp>
#include <grpcpp/grpcpp.h>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using alimama::proto::Request;
using alimama::proto::Response;
using alimama::proto::SearchService;

class SearchClient {
public:
  SearchClient(std::shared_ptr<Channel> channel)
      : stub_(SearchService::NewStub(channel)) {}

  void Search(const std::vector<uint64_t> &keywords,
              const std::vector<float> &context_vector, uint64_t hour,
              uint64_t topn) {
    Request request;

    for (auto keyword : keywords) {
      request.add_keywords(keyword);
    }

    for (auto value : context_vector) {
      request.add_context_vector(value);
    }

    request.set_hour(hour);
    request.set_topn(topn);

    Response response;
    ClientContext context;

    Status status = stub_->Search(&context, request, &response);

    if (status.ok()) {
      std::cout << "RPC Ok, resp size=" << response.adgroup_ids_size()
                << std::endl;
      for (int i = 0; i < response.adgroup_ids_size(); i++) {
        std::cout << "Adgroup ID: " << response.adgroup_ids(i)
                  << ", Price: " << response.prices(i) << std::endl;
      }
    } else {
      std::cout << "RPC failed" << std::endl;
    }
  }

private:
  std::unique_ptr<SearchService::Stub> stub_;
};

int main(int argc, char **argv) {
  // 创建一个etcd客户端
  etcd::Client etcd("http://etcd:2379");

  // 从etcd中获取服务地址
  auto response = etcd.get("/services/searchservice").get();
  if (response.is_ok()) {
    std::cout << "Service connected successful.\n";
  } else {
    std::cerr << "Service connected failed: " << response.error_message()
              << "\n";
    return -1;
  }
  std::string server_address = response.value().as_string();

  // std::string server_address("0.0.0.0:50051");

  std::cout << "server_address " << server_address << std::endl;
  SearchClient client(
      grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()));

  std::vector<uint64_t> keywords = {2916200016};
  std::vector<float> context_vector = {0.825999f, 0.563671f};
  uint64_t hour = 1;
  uint64_t topn = 10;

  client.Search(keywords, context_vector, hour, topn);

  return 0;
}
