#pragma once

#include <bits/stdc++.h>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <etcd/Client.hpp>
#include <etcd/Response.hpp>

#define RUN_REMOTE 1

struct Data;

// keyword -> 文件中offset
using IndexType = std::unordered_multimap<uint64_t, Data>;
extern IndexType kw2data;

int hashKeyword(uint64_t keyword);
float GetCTR(const Data &data, float req_vec1, float req_vec2);
float GetDataScore(const Data &data, float req_vec1, float req_vec2);

// 使用GCC/CLANG的__attribute__((packed))可以不进行对齐，但是性能会有影响，我这里还是先对齐吧
#pragma pack(push, 1)
struct Data {
  // keyword直接保存在索引里
  // uint64_t keyword;
  uint64_t adgroup_id;

  float vec1; // 确保sizeof(float) == 4!
  float vec2;

  char timings_hex[3];
  uint16_t keyword_prices; // price是uint16

  void print() {
    // std::cout << "keyword " << keyword << std::endl;
    std::cout << "adgroup_id " << adgroup_id << std::endl;
    std::cout << "keyword prices " << keyword_prices << std::endl;
    std::cout << "timings ";
    for (int i = 0; i < 3; ++i) {
      printBinary(timings_hex[i]);
    }
    std::cout << std::endl;
    std::cout << "vectors " << vec1 << " " << vec2 << std::endl;
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
#pragma pack(pop)

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
      result.emplace_back(pq.top());
      pq.pop();
    }
    return result;
  }

private:
  int n;
  std::priority_queue<DataScore, std::vector<DataScore>, DataScoreCompare> pq;
};

// ------ 读取csv数据(满足hash(x)==node_id-1的)，紧凑的保存，建立内存索引 ------
void prepareData(int node_id, IndexType &index, std::string ifilename);

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

  AlimamaCSVReader(std::string ifilename, IndexType &index, HashFunc has,
                   int node_id)
      : ifilename_(ifilename), index_(index), hash_(hashKeyword),
        node_id_(node_id) {}

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
      uint64_t keyword;
      if (readCsvLine(lineStart, len, entry, keyword)) {
        // true才是满足条件的entry
        linecount++;
        // 建立内存索引
        index_.insert(std::make_pair(keyword, entry));
      }
      lineStart = lineEnd + 1;
    }

    // 解除内存映射
    if (munmap(fData, fileSize) == -1) {
      std::cerr << "Error unmapping file from memory." << std::endl;
    }

    std::cout << "line_count: " << linecount << std::endl;

    close(fileDescriptor);
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

private:
  bool readCsvLine(const char *lineStart, int len, Data &entry,
                   uint64_t &keyword) {
    std::string str(lineStart, len);

    std::istringstream lineStream(str);
    // uint64_t keyword;
    uint64_t adgroup_id, keyword_prices;

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
    // entry.keyword = keyword;
    entry.adgroup_id = adgroup_id;
    entry.keyword_prices = keyword_prices;
    std::memcpy(entry.timings_hex, bytes, 3);
    entry.vec1 = vector_[0];
    entry.vec2 = vector_[1];

    return true;
  }

  // raw data filename
  std::string ifilename_; // 输入的csv文件名
  // mutex mtx_;
  int file_count_ = 0;
  IndexType &index_;
  HashFunc hash_;
  const int node_id_;
};

std::string EtcdGetKVWait(etcd::Client &client, std::string key);
void EtcdSetKV(etcd::Client &client, std::string key, std::string value);

class ThreadPool {
private:
  std::vector<std::thread> workers;
  std::queue<std::function<void()>> tasks;
  std::mutex queue_mutex;
  std::condition_variable condition;
  bool stop;

public:
  ThreadPool(size_t threads) : stop(false) {
    for (size_t i = 0; i < threads; ++i) {
      workers.emplace_back([this] {
        while (true) {
          std::function<void()> task;
          {
            std::unique_lock<std::mutex> lock(this->queue_mutex);
            this->condition.wait(
                lock, [this] { return this->stop || !this->tasks.empty(); });
            if (this->stop && this->tasks.empty())
              return;
            task = std::move(this->tasks.front());
            this->tasks.pop();
          }
          task();
        }
      });
    }
  }

  template <class F, class... Args>
  auto enqueue(F &&f, Args &&...args)
      -> std::future<typename std::result_of<F(Args...)>::type> {
    using return_type = typename std::result_of<F(Args...)>::type;

    auto task = std::make_shared<std::packaged_task<return_type()>>(
        std::bind(std::forward<F>(f), std::forward<Args>(args)...));

    std::future<return_type> res = task->get_future();
    {
      std::unique_lock<std::mutex> lock(queue_mutex);
      if (stop)
        throw std::runtime_error("enqueue on stopped ThreadPool");
      tasks.emplace([task]() { (*task)(); });
    }
    condition.notify_one();
    return res;
  }

  ~ThreadPool() {
    {
      std::unique_lock<std::mutex> lock(queue_mutex);
      stop = true;
    }
    condition.notify_all();
    for (std::thread &worker : workers)
      worker.join();
  }
};