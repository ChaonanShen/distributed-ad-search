#include <bits/stdc++.h>

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
std::vector<uint64_t> randomChoose(std::vector<uint64_t> &v, int num) {
  auto indexes = genIndex(num, 0, v.size());
  std::vector<uint64_t> ret;
  for (auto n : indexes) {
    ret.push_back(v[n]);
  }
  return ret;
}

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
    return;
  }

  // 每个keywords中挑40个，生成一组testcase
  const int num_lines = 10000;
  for (int i = 0; i < num_lines; i++) {
    auto v1 = randomChoose(keyword0, 40);
    auto v2 = randomChoose(keyword1, 40);
    auto v3 = randomChoose(keyword2, 40);

    std::vector<uint64_t> v;
    v.insert(v.end(), v1.begin(), v1.end());
    v.insert(v.end(), v2.begin(), v2.end());
    v.insert(v.end(), v3.begin(), v3.end());

    std::random_shuffle(v.begin(), v.end());

    for (auto e : v) {
      file << e << "\t";
    }

    float vec1 = vectorDist(generator);
    float vec2 = vectorDist(generator);
    float ss = sqrt(vec1 * vec1 + vec2 * vec2);
    vec1 = vec1 / ss;
    vec2 = vec2 / ss;

    uint64_t hour = hourDist(generator);
    uint64_t topn = 100;

    // 生成一个request发送过去，得到结果后写入文件

    file << vec1 << "," << vec2 << "\t" << hour << "\t" << topn << "\n";
    // topn完全设置成100
  }
}