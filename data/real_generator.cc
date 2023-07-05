#include <bits/stdc++.h>

/**
 * 生成100w个不同的keywords，生成500w个不同的广告单元id
 * 对每个广告单元id，随机挑选100个关键词，生成一条数据，其他信息都随意
 */

const int million = 100 * 10000; // 100w

// 一个keyword & adgroup_id的组合
std::vector<std::pair<uint64_t, uint64_t>> keyAndAdid;

// 生成0~million之间随机的100个下标
std::vector<uint64_t> genIndex() {
  std::random_device rd;
  std::mt19937_64 generator(rd());

  std::uniform_int_distribution<uint64_t> indexDist(0, million);
  std::unordered_set<uint64_t> indexes;
  while (indexes.size() <= 100) {
    indexes.insert(indexDist(generator));
  }
  std::vector<uint64_t> ret;
  ret.assign(indexes.begin(), indexes.end());
  return ret;
}

void prepare() {
  std::random_device rd;
  std::mt19937_64 generator(rd());
  std::uniform_int_distribution<uint64_t> keywordDist(1, 10000000000);
  std::uniform_int_distribution<uint64_t> adgroupIdDist(1, 10000000000000);

  std::unordered_set<uint64_t> keywordSet, adgroupIdSet;
  std::vector<uint64_t> keywordVec, adgroupIdVec;

  while (keywordSet.size() <= million) {
    keywordSet.insert(keywordDist(generator));
  }
  while (adgroupIdSet.size() <= 5 * million) {
    adgroupIdSet.insert(adgroupIdDist(generator));
  }

  keywordVec.reserve(million);
  adgroupIdVec.reserve(5 * million);

  keywordVec.assign(keywordSet.begin(), keywordSet.end());
  adgroupIdVec.assign(adgroupIdSet.begin(), adgroupIdSet.end());

  size_t n = 50000;
  n *= million;
  for (int i = 0; i < adgroupIdVec.size(); i++) {
    // 随机抽取100个keywordVec中的keywords
    auto adgroup_id = adgroupIdVec[i];
    auto indexes = genIndex();
    for (auto keyword : indexes) {
      keyAndAdid.emplace_back(std::make_pair(keyword, adgroup_id));
    }
  }

  std::random_shuffle(keyAndAdid.begin(), keyAndAdid.end());
  std::random_shuffle(keyAndAdid.begin(), keyAndAdid.end());
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

  // TODO(scn):
  // 我这样子生成相当于关键字都排过序了，但是实际上目前数据是未排序过的

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
  std::cout << "prepare done" << std::end;
  generateDataFile(filename);

  return 0;
}
