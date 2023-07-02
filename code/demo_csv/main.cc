#include <bits/stdc++.h>

using namespace std;

#include "read_csv.h"

float GetDataScore(const Data &data, float req_vec1, float req_vec2) {
  // 预估点击率 ECTR(estimated clieck-through rate)
  auto est_ctr = req_vec1 * data.vec1 + req_vec2 * data.vec2;
  // 排序分数 = 预估点击率 x 出价
  return est_ctr * data.keyword_prices;
}

// 数据本身及其分数
struct DataScore {
  struct Data data;
  float score;
};

// 用于比较DataScore的函数对象
// TODO(scn): 能不能把这放进TopN结构中？用bind/function？
struct DataScoreCompare {
  bool operator()(const DataScore &a, const DataScore &b) {
    return a.score > b.score;
  }
};

// 同一keyword 筛选出topN
class TopN {
public:
  TopN(int n) : n(n) {}

  void insert(const DataScore &ds) {
    pq.push(ds);
    if (pq.size() > n) {
      pq.pop();
    }
  }

  std::vector<DataScore> getTopN() {
    std::vector<DataScore> result;
    while (!pq.empty()) {
      // top的是最小的，所以先出去
      result.push_back(pq.top());
      pq.pop();
    }
    return result;
  }

private:
  int n;
  std::priority_queue<DataScore, std::vector<DataScore>, DataScoreCompare> pq;
};

int main() {
  AlimamaCSVReader reader("data.csv", "store");
  reader.readCsvAndSave();

  cout << sizeof(Data) << endl << endl;

  auto result = reader.readFromDisk();
  for (int i = 0; i < result.size(); i++) {
    cout << i + 1 << ":" << endl;
    result[i].print();
  }

  float req_vec[2] = {0.799193, 0.601074}; // 用户传入的向量

  TopN rst(6);
  for (auto d : result) {
    if (d.keyword == 2916200016) {
      rst.insert(DataScore{d, GetDataScore(d, req_vec[0], req_vec[1])});
    }
  }

  // 输出排序结果
  cout << "sort result: " << endl;
  auto sort_rst = rst.getTopN();
  for (auto r : sort_rst) {
    cout << "score " << r.score << endl;
    r.data.print();
  }
}