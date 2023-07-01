#include "csv.h"
#include <bits/stdc++.h>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

using namespace std;

using CSVDef =
    csv::CSVReader<8, csv::trim_chars<' '>, csv::no_quote_escape<'\t'>>;

long getRowsCount(std::string filename) {
  // std::string filename = "/work/alimama/raw_data.csv";  // 要统计行数的文件名
  std::ifstream file(filename);

  if (!file) {
    std::cout << "无法打开文件：" << filename << std::endl;
    return 1;
  }

  long lineCount = 0;
  std::string line;
  while (std::getline(file, line)) {
    lineCount++;
  }

  file.close();
  return lineCount;
}

struct Data {
  uint64_t keyword;
  uint64_t adgroup_id;
  uint64_t keyword_prices;
  int8_t status;
  char timings_hex[3];
  float vec1; // 确保sizeof(float) == 4!
  float vec2;
  uint64_t campaign_id;
  uint64_t item_id;

  void print() {
    cout << "keyword " << keyword << endl;
    cout << "adgroup_id " << adgroup_id << endl;
    cout << "keyword prices " << keyword_prices << endl;
    cout << "status " << status << endl;
    cout << "timings ";
    for (int i = 0; i < 3; ++i) {
      printBinary(timings_hex[i]);
    }
    cout << endl;
    cout << "vectors " << vec1 << " " << vec2 << endl;
    cout << "campaign_id " << campaign_id << endl;
    cout << "item_id " << item_id << endl;
    cout << "========================" << endl;
  }

private:
  void printBinary(char c) {
    for (int i = 7; i >= 0; --i) {
      std::cout << ((c >> i) & 1) << " ";
    }
    cout << "| ";
  }
};

class ThreadPool {
public:
  ThreadPool(int numThreads) : stop(false) {
    for (int i = 0; i < numThreads; ++i) {
      threads.emplace_back(std::thread(&ThreadPool::workerThread, this));
    }
  }

  ~ThreadPool() {
    {
      std::unique_lock<std::mutex> lock(queueMutex);
      stop = true;
    }
    condition.notify_all();

    for (std::thread &thread : threads) {
      thread.join();
    }
  }

  template <typename F, typename... Args>
  void enqueue(F &&func, Args &&...args) {
    {
      std::unique_lock<std::mutex> lock(queueMutex);
      taskQueue.emplace(
          std::bind(std::forward<F>(func), std::forward<Args>(args)...));
    }
    condition.notify_one();
  }

private:
  void workerThread() {
    while (true) {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lock(queueMutex);
        condition.wait(lock, [this]() { return stop || !taskQueue.empty(); });

        if (stop && taskQueue.empty()) {
          return;
        }

        task = std::move(taskQueue.front());
        taskQueue.pop();
      }

      task();
    }
  }

  std::vector<std::thread> threads;
  std::queue<std::function<void()>> taskQueue;
  std::mutex queueMutex;
  std::condition_variable condition;
  bool stop;
};

// 尝试多种方式，对比
class AlimamaReader {
public:
  AlimamaReader() {}

  AlimamaReader(string filename) { this->filename_ = filename; }

  // TODO(scn): 解析每一行的代码一定要效率高
  // 这里大量的string生成和析构是否开销很大？能否弄个内存池复用 -
  // 不过只要创建时间在10分钟内也无所谓了
  void readAllData() {
    // 打开文件
    int fileDescriptor = open(this->filename_.c_str(), O_RDONLY);
    if (fileDescriptor < 0) {
      std::cerr << "Error opening file: " << this->filename_ << std::endl;
      return;
    }

    struct stat fileInfo;
    if (fstat(fileDescriptor, &fileInfo) < 0) {
      std::cerr << "Error getting file size." << std::endl;
      close(fileDescriptor);
      return;
    }
    off_t fileSize = fileInfo.st_size;

    void *fileData = mmap(nullptr, fileSize, PROT_READ | PROT_WRITE,
                          MAP_PRIVATE, fileDescriptor, 0);

    if (fileData == MAP_FAILED) {
      std::cerr << "Error mapping file to memory." << std::endl;
      close(fileDescriptor);
      return;
    }

    uint64_t linecount = 0;
    char *csvData = static_cast<char *>(fileData);
    char *lineStart = csvData;
    char *lineEnd = nullptr;

    while (lineStart < csvData + fileSize) {
      lineEnd = strchr(lineStart, '\n');
      if (lineEnd == nullptr) {
        lineEnd = csvData + fileSize; // 文件末尾
      }

      size_t len = lineEnd - lineStart;

      Data entry;
      readOneLine(lineStart, len, entry);

      lineStart = lineEnd + 1;
      linecount++;

      entry.print();
    }

    // 解除内存映射
    if (munmap(fileData, fileSize) == -1) {
      std::cerr << "Error unmapping file from memory." << std::endl;
    }

    close(fileDescriptor);
  }

  void readOneLine(const char *lineStart, int len, Data &entry) {
    string str(lineStart, len);

    std::istringstream lineStream(str);
    uint64_t keyword, adgroup_id, keyword_prices;
    std::string timings_str;
    float vector_[2];
    int8_t status;
    uint64_t campaign_id, item_id;

    lineStream >> keyword;
    lineStream.ignore();
    lineStream >> adgroup_id;
    lineStream.ignore();
    lineStream >> keyword_prices;
    lineStream.ignore();
    lineStream >> status;
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
  }

private:
  // raw data filename
  string filename_;
  mutex mtx_;
  int file_count_ = 0;
};