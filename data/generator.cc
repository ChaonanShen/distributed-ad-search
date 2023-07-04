#include <fstream>
#include <iostream>
#include <random>
#include <string>

void generateDataFile(const std::string &filename, uint32_t numLines) {
  std::ofstream file(filename);
  if (!file) {
    std::cerr << "无法打开文件：" << filename << std::endl;
    return;
  }

  // 随机数生成器
  std::random_device rd;
  std::mt19937_64 generator(rd());
  std::uniform_int_distribution<uint64_t> keywordDist(1, 10000000000);
  std::uniform_int_distribution<uint64_t> adgroupIdDist(1, 10000000000000);
  std::uniform_int_distribution<uint64_t> priceDist(1, 100000);
  std::uniform_int_distribution<int8_t> statusDist(0, 1);
  std::uniform_real_distribution<float> vectorDist(0.0, 1.0);
  std::uniform_int_distribution<uint64_t> campaignIdDist(1, 1000000000);
  std::uniform_int_distribution<uint64_t> itemIdDist(1, 10000000000000);

  for (uint32_t i = 0; i < numLines; ++i) {
    uint64_t keyword = keywordDist(generator);
    uint64_t adgroupId = adgroupIdDist(generator);
    uint64_t price = priceDist(generator);
    int8_t status = statusDist(generator);

    std::string timings;
    for (int j = 0; j < 24; ++j) {
      timings += (statusDist(generator) == 1 ? "1" : "0");
      if (j != 23) {
        timings += ",";
      }
    }

    float vec1 = vectorDist(generator);
    float vec2 = vectorDist(generator);

    uint64_t campaignId = campaignIdDist(generator);
    uint64_t itemId = itemIdDist(generator);

    // 写入数据到文件
    file << keyword << "\t" << adgroupId << "\t" << price << "\t"
         << static_cast<int>(status) << "\t" << timings << "\t" << vec1 << ","
         << vec2 << "\t" << campaignId << "\t" << itemId << std::endl;
  }

  file.close();
  std::cout << "数据文件已生成：" << filename << std::endl;
}

int main() {
  std::string filename = "data.csv";
  uint64_t numLines = 6 * 10000 * 10000; // 6亿行数据

  generateDataFile(filename, numLines);

  return 0;
}
