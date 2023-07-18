#include <bits/stdc++.h>

/**
 * 最关键只是关键词(keyword)和广告单元id(adgroup_id)，这两个形成联合主键确定每一行
 * 生成100w个不同的keywords，生成500w个不同的广告单元id
 * 对每个广告单元id，随机挑选120个关键词，生成一条数据，其他信息都随意，总共500w*120正好6亿行
 */

// 小数据量改成10w
const int million = 100 * 10000; // 100w
const int keywordPerAd = 120;
// 一个keyword & adgroup_id的组合
// 500w个adgroup_id * 每个平均120个keyword
std::vector<std::pair<uint64_t, uint64_t>> keyAndAdid;

// 生成0~million之间随机的120个下标
std::vector<uint64_t> genIndex() {
  std::random_device rd;
  std::mt19937_64 generator(rd());

  std::uniform_int_distribution<uint64_t> indexDist(0, million);
  std::unordered_set<uint64_t> indexes;
  while (indexes.size() < keywordPerAd) {
    indexes.insert(indexDist(generator));
  }
  std::vector<uint64_t> ret;
  ret.assign(indexes.begin(), indexes.end());
  return ret;
}

void prepare() {
  std::random_device rd;
  std::mt19937_64 generator(rd());

  std::uniform_int_distribution<uint64_t> keywordDist(1, 10000000000000);
  std::uniform_int_distribution<uint64_t> adgroupIdDist(1, 10000000000000);

  std::unordered_set<uint64_t> keywordSet, adgroupIdSet;
  std::vector<uint64_t> keywordVec, adgroupIdVec;

  keywordSet.reserve(million);
  adgroupIdSet.reserve(5 * million);

  keywordVec.reserve(million);
  adgroupIdVec.reserve(5 * million);

  while (keywordSet.size() < million) {
    keywordSet.insert(keywordDist(generator));
  }
  while (adgroupIdSet.size() < 5 * million) {
    adgroupIdSet.insert(adgroupIdDist(generator));
  }

  keywordVec.assign(keywordSet.begin(), keywordSet.end());
  adgroupIdVec.assign(adgroupIdSet.begin(), adgroupIdSet.end());

  keyAndAdid.reserve(adgroupIdVec.size() * keywordPerAd);

  for (int i = 0; i < adgroupIdVec.size(); i++) {
    // 随机抽取120个keywordVec中的keywords
    auto adgroup_id = adgroupIdVec[i];
    auto indexes = genIndex();
    for (auto index : indexes) {
      keyAndAdid.emplace_back(std::make_pair(keywordVec[index], adgroup_id));
    }
  }

  std::random_shuffle(keyAndAdid.begin(), keyAndAdid.end());
  std::random_shuffle(keyAndAdid.begin(), keyAndAdid.end());

  // 将keywords也都记录下
  std::string keywordFile[3] = {"keyword0.txt", "keyword1.txt", "keyword2.txt"};
  std::ofstream file0(keywordFile[0]), file1(keywordFile[1]),
      file2(keywordFile[2]);
  if (!file0 || !file1 || !file2) {
    std::cerr << "无法打开keywordFile文件" << std::endl;
    return;
  }

  for (auto key : keywordVec) {
    switch (key % 3) {
    case 0:
      file0 << key << "\n";
      break;
    case 1:
      file1 << key << "\n";
      break;
    case 2:
      file2 << key << "\n";
      break;
    }
  }

  file0.close();
  file1.close();
  file2.close();
}

void generateDataFile(const std::string &filename) {
  std::ofstream file(filename);
  if (!file) {
    std::cerr << "无法打开文件：" << filename << std::endl;
    return;
  }

  // 随机数生成器
  std::random_device rd;
  std::mt19937_64 generator(rd());

  std::uniform_int_distribution<uint64_t> priceDist(1, 100000);
  std::uniform_int_distribution<int8_t> statusDist(0, 1);
  std::uniform_real_distribution<float> vectorDist(0.0, 1.0);
  std::uniform_int_distribution<uint64_t> campaignIdDist(1, 1000000000);
  std::uniform_int_distribution<uint64_t> itemIdDist(1, 10000000000000);

  size_t cnt = 0;
  for (auto it : keyAndAdid) {
    uint64_t keyword = it.first;
    uint64_t adgroupId = it.second;
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
    float ss = sqrt(vec1 * vec1 + vec2 * vec2);
    vec1 = vec1 / ss;
    vec2 = vec2 / ss;

    uint64_t campaignId = campaignIdDist(generator);
    uint64_t itemId = itemIdDist(generator);

    // 写入数据到文件
    file << keyword << "\t" << adgroupId << "\t" << price << "\t"
         << static_cast<int>(status) << "\t" << timings << "\t" << vec1 << ","
         << vec2 << "\t" << campaignId << "\t" << itemId << std::endl;

    if ((cnt % million) == 0) {
      std::cout << keyword << " " << adgroupId << std::endl;
    }
    cnt++;
  }

  file.close();
  std::cout << "数据文件已生成：" << filename << std::endl;
}

int main() {
  std::string filename = "real_data.csv";
  prepare();
  std::cout << "prepare done" << std::endl;
  generateDataFile(filename);

  return 0;
}