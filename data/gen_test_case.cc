#include <bits/stdc++.h>

#include "alimama.grpc.pb.h"
#include <grpcpp/grpcpp.h>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using alimama::proto::Request;
using alimama::proto::Response;
using alimama::proto::SearchService;

using namespace std;

std::vector<uint64_t> genIndex(int num, int low, int high) {
  std::random_device rd;
  std::mt19937_64 generator(rd());

  std::uniform_int_distribution<uint64_t> indexDist(low, high);
  std::unordered_set<uint64_t> indexes;
  while (indexes.size() < num) {
    indexes.insert(indexDist(generator));
  }
  std::vector<uint64_t> ret;
  ret.assign(indexes.begin(), indexes.end());
  return ret;
}

std::vector<uint64_t> loadNumbers(const std::string &filename) {
  std::ifstream file(filename); // 打开文件
  if (!file.is_open()) {
    throw std::runtime_error("Could not open file");
  }

  std::vector<uint64_t> numbers; // 创建vector
  uint64_t number;
  while (file >> number) {       // 从文件中读取每个数字
    numbers.push_back(number);   // 添加到vector中
  }

  return numbers;
}

// 从vector中随机挑出n个数
std::vector<uint64_t> randomChoose(std::vector<uint64_t> &keywords, int num) {
  auto indexes = genIndex(num, 0, keywords.size());
  std::vector<uint64_t> ret;
  for (auto n : indexes) {
    ret.push_back(keywords[n]);
  }
  return ret;
}

class SearchClient {
public:
  SearchClient(std::shared_ptr<Channel> channel)
      : stub_(SearchService::NewStub(channel)) {}

  void Search(const std::vector<uint64_t> &keywords,
              const std::vector<float> &context_vector, uint64_t hour,
              uint64_t topn, Response *response) {
    Request request;

    for (auto keyword : keywords) {
      request.add_keywords(keyword);
    }

    for (auto value : context_vector) {
      request.add_context_vector(value);
    }

    request.set_hour(hour);
    request.set_topn(topn);

    ClientContext context;

    Status status = stub_->Search(&context, request, response);

    if (status.ok()) {
      std::cout << "RPC Ok, resp size=" << response->adgroup_ids_size()
                << std::endl;
    } else {
      std::cout << "RPC failed" << std::endl;
    }
  }

private:
  std::unique_ptr<SearchService::Stub> stub_;
};

int main() {
  auto keyword0 = loadNumbers("keyword0.txt");
  auto keyword1 = loadNumbers("keyword1.txt");
  auto keyword2 = loadNumbers("keyword2.txt");

  cout << keyword0.size() << " " << keyword1.size() << " " << keyword2.size()
       << endl;

  // 随机数生成器
  std::random_device rd;
  std::mt19937_64 generator(rd());
  std::uniform_real_distribution<float> vectorDist(0.0, 1.0);
  std::uniform_int_distribution<> hourDist(0, 23);

  auto filename = "real_test_case_dev.csv";
  std::ofstream file(filename);
  if (!file) {
    std::cerr << "无法打开文件：" << filename << std::endl;
    return -1;
  }

  // 在node-1容器中运行
  std::string server_address("0.0.0.0:50051");

  // 每个keywords中挑40个，生成一组testcase
  const int num_lines = 10000;
  for (int i = 0; i < num_lines; i++) {
    auto v1 = randomChoose(keyword0, 40);
    auto v2 = randomChoose(keyword1, 40);
    auto v3 = randomChoose(keyword2, 40);

    std::vector<uint64_t> keywords;
    keywords.insert(keywords.end(), v1.begin(), v1.end());
    keywords.insert(keywords.end(), v2.begin(), v2.end());
    keywords.insert(keywords.end(), v3.begin(), v3.end());

    std::random_shuffle(keywords.begin(), keywords.end());

    for (int i = 0; i < keywords.size(); i++) {
      if (i != 0)
        file << ",";
      file << keywords[i];
    }
    file << "\t";

    float vec1 = vectorDist(generator);
    float vec2 = vectorDist(generator);
    float ss = sqrt(vec1 * vec1 + vec2 * vec2);
    vec1 = vec1 / ss;
    vec2 = vec2 / ss;

    std::vector<float> context_vector = {vec1, vec2};

    uint64_t hour = hourDist(generator);
    // topn完全设置成50
    uint64_t topn = 50;

    // 生成一个request发送过去，得到结果后写入文件
    Response response;
    SearchClient client(grpc::CreateChannel(
        server_address, grpc::InsecureChannelCredentials()));

    client.Search(keywords, context_vector, hour, topn, &response);

    std::cout << "response size=" << response.adgroup_ids_size() << std::endl;

    file << vec1 << "," << vec2 << "\t" << hour << "\t" << topn << "\t";
    int sz = response.adgroup_ids_size();
    for (int i = 0; i < sz; i++) {
      if (i != 0)
        file << ",";
      file << response.adgroup_ids(i);
    }
    file << "\t";
    for (int i = 0; i < sz; i++) {
      if (i != 0)
        file << ",";
      file << response.prices(i);
    }
    file << "\n";
  }
}