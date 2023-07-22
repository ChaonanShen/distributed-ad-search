#pragma once

#include <bits/stdc++.h>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <etcd/Client.hpp>
#include <etcd/Response.hpp>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "alimama.grpc.pb.h"

#include "flat_hash_map.h"
#include "flat_hash_set.h"

using alimama::proto::BatchRequest;
using alimama::proto::BatchResponseLocal;
using alimama::proto::Request;
using alimama::proto::Response;
using alimama::proto::ResponseLocal;
using alimama::proto::SearchService;

void doSearchLocal(const BatchRequest *request, BatchResponseLocal *response);
void doSearchMerge(const Request *request, Response *response,
                   BatchResponseLocal resp_local[3], int batch_idx);

#define RUN_REMOTE 1

// 这样cq.Next等待队列的并发应该小一些就行，因为最重要的任务都线程池去做了
const int search_cq_num = 16;
const int searchlocal_cq_num = 16;

// 64-14 7-9 / 80-20 7-9 /
const int tp_io_num = 64;
const int tp_cpu_num = 14;

struct Data;

// keyword -> 文件中offset
using IndexType = absl::flat_hash_map<uint64_t, uint32_t>;
extern IndexType kw2index;
extern std::vector<Data> datas;

extern int port;
extern std::string serverAddr[3];
std::string getCurrentServerAddr();
std::string getSearchServerAddr(int index);
extern std::unique_ptr<SearchService::Stub> stubs[3];
void MakeupStubs();

int hashKeyword(uint64_t keyword);
float GetCTR(const Data &data, float req_vec1, float req_vec2);
float GetDataScore(const Data &data, float req_vec1, float req_vec2);

// 使用GCC/CLANG的__attribute__((packed))可以不进行对齐，但是性能会有影响，我这里还是先对齐吧
#pragma pack(push, 1)
struct Data {
  // keyword直接保存在索引里
  uint64_t keyword;
  uint64_t adgroup_id;

  float vec1; // 确保sizeof(float) == 4!
  float vec2;

  char timings_hex[3];
  uint16_t keyword_prices; // price是uint16

  void print() {
    std::cout << "keyword " << keyword << std::endl;
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

  void getTopN(std::vector<DataScore> &result) {
    result.reserve(pq.size());
    while (!pq.empty()) {
      // top的是最小的，所以先出去
      result.emplace_back(pq.top());
      pq.pop();
    }
  }

private:
  int n;
  std::priority_queue<DataScore, std::vector<DataScore>, DataScoreCompare> pq;
};

// ------ 读取csv数据(满足hash(x)==node_id-1的)，紧凑的保存，建立内存索引 ------
void prepareData(int node_id, std::string ifilename, std::vector<Data> &datas);

// ------ 计算出最好的那一条广告单元 ------
void CalcAdgroupId(uint64_t keyword, uint64_t hour, uint64_t topn,
                   float context_vec[2], std::vector<DataScore> &result);

// ------ 其他方法 ------
std::string getLocalIP();
int getPort(int argc, char **argv);

// 尝试多种方式，对比
class AlimamaCSVReader {
public:
  using HashFunc = std::function<int(uint64_t)>;

  AlimamaCSVReader(std::string ifilename, HashFunc has, int node_id,
                   std::vector<Data> &datas)
      : ifilename_(ifilename), hash_(hashKeyword), node_id_(node_id),
        datas_(datas) {}

  // 保证size内包含的是完整的各行
  void readFromStr(char *start, uint64_t size) {
    char *lineStart = start;
    char *lineEnd = nullptr;

    while (lineStart < start + size) {
      lineEnd = strchr(lineStart, '\n');
      if (lineEnd == nullptr) {
        lineEnd = start + size; // 文件末尾
      }

      size_t len = lineEnd - lineStart;

      Data entry;
      uint64_t keyword;
      if (readCsvLine(lineStart, len, entry, keyword)) {
        linecount_++;
        {
          std::scoped_lock lock{mtx_};
          datas.emplace_back(entry);
        }
      }
      lineStart = lineEnd + 1;
    }
  }

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

    std::cout << "fileSize=" << fileSize << std::endl;

    void *fData =
        mmap(nullptr, fileSize, PROT_READ, MAP_PRIVATE, fileDescriptor, 0);

    if (fData == MAP_FAILED) {
      std::cerr << "Error mapping file to memory." << std::endl;
      close(fileDescriptor);
      return;
    }

    char *csvData = static_cast<char *>(fData);

    const int PARALLEL_NUM = 16;
    char *lineStart[PARALLEL_NUM];
    uint64_t fSize[PARALLEL_NUM];

    lineStart[0] = csvData;
    for (int i = 1; i < PARALLEL_NUM; i++) {
      lineStart[i] = csvData + i * fileSize / PARALLEL_NUM;
      lineStart[i] = strchr(lineStart[i], '\n');
      assert(*lineStart[i] == '\n');
      lineStart[i] = lineStart[i] + 1; // skip '\n'
    }

    uint64_t sz = 0;
    for (int i = 0; i < PARALLEL_NUM - 1; i++) {
      fSize[i] = lineStart[i + 1] - lineStart[i];
      sz += fSize[i];
    }
    fSize[PARALLEL_NUM - 1] = fileSize - sz;

    for (int i = 0; i < PARALLEL_NUM; i++) {
      std::cout << "fSize " << i << " " << fSize[i] << std::endl;
    }

    std::vector<std::thread> threads;

    std::function<void(char *, uint64_t)> func =
        std::bind(&AlimamaCSVReader::readFromStr, this, std::placeholders::_1,
                  std::placeholders::_2);
    for (int i = 0; i < PARALLEL_NUM; i++) {
      threads.push_back(std::thread(func, lineStart[i], fSize[i]));
    }

    for (auto &th : threads) {
      th.join();
    }

    // 解除内存映射
    if (munmap(fData, fileSize) == -1) {
      std::cerr << "Error unmapping file from memory." << std::endl;
    }

    std::cout << "line_count: " << linecount_ << std::endl;

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
    entry.keyword = keyword;
    entry.adgroup_id = adgroup_id;
    entry.keyword_prices = keyword_prices;
    std::memcpy(entry.timings_hex, bytes, 3);
    entry.vec1 = vector_[0];
    entry.vec2 = vector_[1];

    return true;
  }

  // raw data filename
  std::string ifilename_; // 输入的csv文件名
  std::mutex mtx_;        // 保护datas_的更新
  std::vector<Data> &datas_;
  HashFunc hash_;
  const int node_id_;
  std::atomic_int linecount_ = 0;
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

// class ThreadPool {
// public:
//   ThreadPool(size_t threads = std::thread::hardware_concurrency())
//       : stop_flag(false) {
//     for (size_t i = 0; i < threads; ++i) {
//       workers.emplace_back([this] {
//         while (!stop_flag) {
//           std::function<void()> task;
//           if (queue.try_dequeue(task)) {
//             task();
//           } else {
//             std::this_thread::yield();
//           }
//         }
//       });
//     }
//   }

//   ~ThreadPool() {
//     stop_flag = true;
//     for (auto &worker : workers) {
//       worker.join();
//     }
//   }

//   template <class F> void enqueue(F &&f) { queue.enqueue(std::forward<F>(f));
//   }

// private:
//   std::vector<std::thread> workers;
//   moodycamel::ConcurrentQueue<std::function<void()>> queue;
//   std::atomic<bool> stop_flag;
// };

// class ThreadPool {
// public:
//   ThreadPool(size_t numThreads) {
//     for (size_t i = 0; i < numThreads; ++i) {
//       threads_.emplace_back([this] {
//         while (true) {
//           std::function<void()> task;
//           if (queue_.try_dequeue(task)) {
//             task();
//           } else {
//             std::this_thread::yield();
//           }
//         }
//       });
//     }
//   }

//   ~ThreadPool() {
//     for (auto &thread : threads_) {
//       thread.detach();
//     }
//   }

//   void enqueue(const std::function<void()> &task) { queue_.enqueue(task); }

// private:
//   moodycamel::ConcurrentQueue<std::function<void()>> queue_;
//   std::vector<std::thread> threads_;
// };