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

/**
 * 用于本地测试，不经过balancer，直接连到三台服务器
 */

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
  // *** 注意这就是在本地测试进行的，在同一个阿里妈妈容器中运行balancer &
  // servers ***

  bool use_balancer = true;
  // 不使用balancer就直接发往对应server，否则就发往balancer
  if (use_balancer) {
    // 三个请求分开单独进行就没事，但是一起就会有事
    // 难道是线程之间相互干扰？？？但是没啥全局变量啊

    std::string balancer_address("0.0.0.0:56789");
    std::cout << "balancer_address " << balancer_address << std::endl;
    SearchClient client(grpc::CreateChannel(
        balancer_address, grpc::InsecureChannelCredentials()));
    for (int i = 0; i < 10000; i++) {
      {
        // 2916200016 12210106372 4803367238 0.975501,0.219997 0 2
        // 1496148419000,1371046260120 37007,27489
        std::vector<uint64_t> keywords = {2916200016, 12210106372, 4803367238};
        std::vector<float> context_vector = {0.975501, 0.219997};
        uint64_t hour = 0;
        uint64_t topn = 2;
        client.Search(keywords, context_vector, hour, topn);
      }

      // 下面三个是单独的一个keyword的检索，最基本的检索
      {
        // 2916200016	0.351177,0.936309	7	2
        // 644960096148,1710671559561	27435,39778
        std::cout << "Request keyword: " << 2916200016 << std::endl;
        std::vector<uint64_t> keywords = {2916200016};
        std::vector<float> context_vector = {0.351177, 0.936309};
        uint64_t hour = 7;
        uint64_t topn = 2;
        client.Search(keywords, context_vector, hour, topn);
      }

      {
        // 12210106372	0.975501,0.219997	16	3
        // 1804714034430	41953
        std::cout << "Request keyword: " << 12210106372 << std::endl;
        std::vector<uint64_t> keywords = {12210106372};
        std::vector<float> context_vector = {0.975501, 0.219997};
        uint64_t hour = 16;
        uint64_t topn = 3;
        client.Search(keywords, context_vector, hour, topn);
      }

      {
        // 4803367238	0.552321,0.833632	20	1
        // 722542970812	17934
        std::cout << "Request keyword: " << 4803367238 << std::endl;
        std::vector<uint64_t> keywords = {4803367238};
        std::vector<float> context_vector = {0.552321, 0.833632};
        uint64_t hour = 20;
        uint64_t topn = 1;
        client.Search(keywords, context_vector, hour, topn);
      }
    }
  } else {
    // 现在单独发给各个servers的请求都没有问题
    {
      std::string server_address("0.0.0.0:50051");
      std::cout << "server_address " << server_address << std::endl;
      SearchClient client(grpc::CreateChannel(
          server_address, grpc::InsecureChannelCredentials()));
      std::cout << "Request keyword: " << 2916200016 << std::endl;
      std::vector<uint64_t> keywords = {2916200016};
      std::vector<float> context_vector = {0.351177, 0.936309};
      uint64_t hour = 7;
      uint64_t topn = 2;
      client.Search(keywords, context_vector, hour, topn);
    }

    {
      std::string server_address("0.0.0.0:50052");
      std::cout << "server_address " << server_address << std::endl;
      SearchClient client(grpc::CreateChannel(
          server_address, grpc::InsecureChannelCredentials()));
      std::cout << "Request keyword: " << 12210106372 << std::endl;
      std::vector<uint64_t> keywords = {12210106372};
      std::vector<float> context_vector = {0.975501, 0.219997};
      uint64_t hour = 16;
      uint64_t topn = 3;
      client.Search(keywords, context_vector, hour, topn);
    }

    {
      std::string server_address("0.0.0.0:50053");
      std::cout << "server_address " << server_address << std::endl;
      SearchClient client(grpc::CreateChannel(
          server_address, grpc::InsecureChannelCredentials()));
      std::cout << "Request keyword: " << 4803367238 << std::endl;
      std::vector<uint64_t> keywords = {4803367238};
      std::vector<float> context_vector = {0.552321, 0.833632};
      uint64_t hour = 20;
      uint64_t topn = 1;
      client.Search(keywords, context_vector, hour, topn);
    }
  }

  return 0;
}
