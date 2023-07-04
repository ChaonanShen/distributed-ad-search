#pragma once

#include <bits/stdc++.h>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <etcd/Client.hpp>
#include <etcd/Response.hpp>

// keyword -> 文件中offset
using IndexType = std::unordered_multimap<uint64_t, uint64_t>;

// mmap文件指针
extern void *fileData;
extern IndexType kw2offset;

struct Data;

int hashKeyword(uint64_t keyword);
float GetCTR(const Data &data, float req_vec1, float req_vec2);
float GetDataScore(const Data &data, float req_vec1, float req_vec2);

void splitStr(std::string str, std::string &s1, std::string &s2);

// 使用GCC/CLANG的__attribute__((packed))可以不进行对齐，但是性能会有影响，我这里还是先对齐吧
// struct __attribute__((packed)) Data {
struct Data {
  uint64_t keyword;
  uint64_t adgroup_id;

  uint64_t campaign_id;
  uint64_t item_id;

  float vec1; // 确保sizeof(float) == 4!
  float vec2;

  char timings_hex[3];
  uint16_t keyword_prices; // price是uint16
  int8_t status;

  void print() {
    std::cout << "keyword " << keyword << std::endl;
    std::cout << "adgroup_id " << adgroup_id << std::endl;
    std::cout << "keyword prices " << keyword_prices << std::endl;
    std::cout << "status " << status << std::endl;
    std::cout << "timings ";
    for (int i = 0; i < 3; ++i) {
      printBinary(timings_hex[i]);
    }
    std::cout << std::endl;
    std::cout << "vectors " << vec1 << " " << vec2 << std::endl;
    std::cout << "campaign_id " << campaign_id << std::endl;
    std::cout << "item_id " << item_id << std::endl;
    std::cout << "========================" << std::endl;
  }

private:
  void printBinary(char c) {
    for (int i = 7; i >= 0; --i) {
      std::cout << ((c >> i) & 1) << " ";
    }
    std::cout << "| ";
  }
};

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

// Data中 筛选出topN
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

// ------ 读取csv数据(满足hash(x)==node_id-1的)，紧凑的保存，建立内存索引 ------
void prepareData(int node_id, IndexType &index, std::string ifilename,
                 std::string ofilename);

// ------ 计算出最好的那一条广告单元 ------
std::vector<DataScore> CalcAdgroupId(uint64_t keyword, uint64_t hour,
                                     uint64_t topn, float context_vec[2]);

// ------ 其他方法 ------
std::string getLocalIP();
int getPort(int argc, char **argv);

// 尝试多种方式，对比
class AlimamaCSVReader {
public:
  using HashFunc = std::function<int(uint64_t)>;

  AlimamaCSVReader(std::string ifilename, std::string ofilename,
                   IndexType &index, HashFunc has, int node_id)
      : ifilename_(ifilename), ofilename_(ofilename), index_(index),
        hash_(hashKeyword), node_id_(node_id) {}

  // TODO(scn): 解析每一行的代码一定要效率高 这个函数要在10min内完成！
  // 这里大量的string生成和析构是否开销很大？能否弄个内存池复用 -
  // 不过只要创建时间在10分钟内也无所谓了
  void readCsvAndSave() {
    // 打开csv文件
    int fileDescriptor = open(this->ifilename_.c_str(), O_RDONLY);
    if (fileDescriptor < 0) {
      std::cerr << "Error opening file: " << this->ifilename_ << std::endl;
      return;
    }

    // 打开最终输出的文件
    std::ofstream outfile(ofilename_, std::ios::binary);
    if (!outfile) {
      std::cerr << "Failed to open the file for writing: " << ofilename_
                << std::endl;
      return;
    }

    struct stat fileInfo;
    if (fstat(fileDescriptor, &fileInfo) < 0) {
      std::cerr << "Error getting file size." << std::endl;
      close(fileDescriptor);
      return;
    }
    off_t fileSize = fileInfo.st_size;

    void *fData =
        mmap(nullptr, fileSize, PROT_READ, MAP_PRIVATE, fileDescriptor, 0);

    if (fData == MAP_FAILED) {
      std::cerr << "Error mapping file to memory." << std::endl;
      close(fileDescriptor);
      return;
    }

    uint64_t linecount = 0;
    char *csvData = static_cast<char *>(fData);
    char *lineStart = csvData;
    char *lineEnd = nullptr;

    uint64_t offset = 0;
    while (lineStart < csvData + fileSize) {
      lineEnd = strchr(lineStart, '\n');
      if (lineEnd == nullptr) {
        lineEnd = csvData + fileSize; // 文件末尾
      }

      size_t len = lineEnd - lineStart;

      Data entry;
      if (readCsvLine(lineStart, len, entry)) { // true才是满足条件的entry
        linecount++;
        // 直接用内存到磁盘数据的映射
        outfile.write((char *)&entry, sizeof(Data));
        // 建立内存索引
        index_.insert(std::make_pair(entry.keyword, offset));
        offset += sizeof(Data);
      }
      lineStart = lineEnd + 1;
    }

    // 解除内存映射
    if (munmap(fData, fileSize) == -1) {
      std::cerr << "Error unmapping file from memory." << std::endl;
    }

    std::cout << "line_count: " << linecount << std::endl;

    close(fileDescriptor);
    outfile.close();
  }

  // keyword是当前节点的
  bool filterKeyword(uint64_t keyword) {
    return hash_(keyword) == (node_id_ - 1);
  }

  // status状态活跃
  bool filterStatus(int8_t status) {
    // TODO(scn): 这个为啥直接status==1判断就有问题？？？
    return (status & 1);
  }

  std::vector<Data> readAllFromDisk() {
    std::vector<Data> data;
    std::ifstream infile(ofilename_, std::ios::binary);
    if (!infile) {
      std::cerr << "Failed to open the file for reading: " << ofilename_
                << std::endl;
      return data;
    }
    while (infile) {
      Data d;
      infile.read((char *)&d, sizeof(Data));
      if (infile) { // 这个好像不能缺，不然数据仿佛会诡异的多出一行
        data.push_back(d);
      }
    }
    infile.close();
    return data;
  }

private:
  bool readCsvLine(const char *lineStart, int len, Data &entry) {
    std::string str(lineStart, len);

    std::istringstream lineStream(str);
    uint64_t keyword, adgroup_id, keyword_prices;
    std::string timings_str;
    float vector_[2];
    int8_t status;
    uint64_t campaign_id, item_id;

    lineStream >> keyword;
    // 直接返回不再解析
    if (!filterKeyword(keyword)) {
      return false;
    }
    lineStream.ignore();
    lineStream >> adgroup_id;
    lineStream.ignore();
    lineStream >> keyword_prices;
    lineStream.ignore();
    lineStream >> status;
    if (!filterStatus(status)) {
      return false;
    }
    lineStream.ignore();
    std::getline(lineStream, timings_str, '\t');
    lineStream >> vector_[0];
    lineStream.ignore();
    lineStream >> vector_[1];
    lineStream >> campaign_id;
    lineStream.ignore();
    lineStream >> item_id;

    // 清理 timings_str 中的逗号
    timings_str.erase(std::remove(timings_str.begin(), timings_str.end(), ','),
                      timings_str.end());

    // 解析 timings_str 为 unsigned int
    std::bitset<24> bits(timings_str);
    unsigned int value = bits.to_ulong();

    // 将 unsigned int 转换为 3 字节的字符数组
    unsigned char bytes[3];
    bytes[0] = (value >> 16) & 0xFF;
    bytes[1] = (value >> 8) & 0xFF;
    bytes[2] = value & 0xFF;

    // 从解析得到的数据构造 Data 结构体
    entry.keyword = keyword;
    entry.adgroup_id = adgroup_id;
    entry.keyword_prices = keyword_prices;
    entry.status = status;
    std::memcpy(entry.timings_hex, bytes, 3);
    entry.vec1 = vector_[0];
    entry.vec2 = vector_[1];
    entry.campaign_id = campaign_id;
    entry.item_id = item_id;

    return true;
  }

  // raw data filename
  std::string ifilename_; // 输入的csv文件名
  std::string ofilename_; // 输出的二进制文件名
  // mutex mtx_;
  int file_count_ = 0;
  IndexType &index_;
  HashFunc hash_;
  const int node_id_;
};

std::string EtcdGetKVWait(etcd::Client &client, std::string key);
void EtcdSetKV(etcd::Client &client, std::string key, std::string value);