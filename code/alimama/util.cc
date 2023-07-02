#include <arpa/inet.h>
#include <cstdlib>
#include <ifaddrs.h>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "util.h"

// 全局变量定义在util中，其他地方extern引用
void *fileData;
IndexType kw2offset;

int hash(uint64_t keyword) { return keyword % 3; }

// 读取csv数据(满足hash(x)==node_id-1的)，紧凑的保存，建立内存索引
void prepareData(int node_id, IndexType &index, std::string ifilename,
                 std::string ofilename) {
  AlimamaCSVReader reader(ifilename, ofilename, index, hash, node_id);
  reader.readCsvAndSave();
}

// 预估点击率
float GetCTR(const Data &data, float req_vec1, float req_vec2) {
  // 预估点击率 ECTR(estimated clieck-through rate)
  auto est_ctr = req_vec1 * data.vec1 + req_vec2 * data.vec2;
  est_ctr += 0.000001f; // 点击率预估分计算原始结果加上0.000001f，确保非0
  return est_ctr;
}

float GetDataScore(const Data &data, float req_vec1, float req_vec2) {
  auto est_ctr = GetCTR(data, req_vec1, req_vec2);
  // 排序分数 = 预估点击率 x 出价
  return est_ctr * data.keyword_prices;
}

// hour是[0, 23]
bool filterHour(Data &entry, uint64_t hour) {
  int n = hour / 8;
  int m = hour % 8;
  return entry.timings_hex[n] & (1 << (7 - m));
}

std::vector<DataScore> CalcAdgroupId(uint64_t keyword, uint64_t hour,
                                     uint64_t topn, float context_vec[2]) {
  // 找到所有keyword的offset
  auto range = kw2offset.equal_range(keyword);
  // 从文件中读取，找到top(n+1) -> 因为第topn个要根据top(n+1)个来确定分数

  TopN topN(topn + 1);
  for (auto it = range.first; it != range.second; it++) {
    auto offset = it->second;
    // 读出数据
    Data entry = *(Data *)((char *)fileData + offset);
    // 确保能读出正确的数据
    // std::cout << "keyword: " << it->first << " offset: " << offset <<
    // std::endl; entry.print();
    if (filterHour(entry, hour)) { // 要时段匹配的
      topN.insert({entry, GetDataScore(entry, context_vec[0], context_vec[1])});
    }
  }

  return topN.getTopN();
}

std::string getLocalIP() {
  struct ifaddrs *ifAddrStruct = NULL;
  void *tmpAddrPtr = NULL;
  std::string localIP;
  getifaddrs(&ifAddrStruct);
  while (ifAddrStruct != NULL) {
    if (ifAddrStruct->ifa_addr->sa_family == AF_INET) {
      tmpAddrPtr = &((struct sockaddr_in *)ifAddrStruct->ifa_addr)->sin_addr;
      char addressBuffer[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
      std::string interfaceName(ifAddrStruct->ifa_name);
      if (interfaceName == "en0" || interfaceName == "eth0") {
        return addressBuffer;
      }
    }
    ifAddrStruct = ifAddrStruct->ifa_next;
  }
  return "";
}

// 接收-p 50051这样的指定端口的参数解析
int getPort(int argc, char **argv) {
  int opt, port;
  while ((opt = getopt(argc, argv, "p:")) != -1) {
    switch (opt) {
    case 'p':
      port = std::atoi(optarg);
      break;
    default: /* '?' */
      std::cerr << "Usage: " << argv[0] << " [-p port]" << std::endl;
      exit(EXIT_FAILURE);
    }
  }
  return port;
}

// 从etcd中获取key对应value，如果key还未注册，就一直死等
std::string EtcdGetKVWait(etcd::Client &client, std::string key) {
  std::string value;
  while (1) {
    etcd::Response resp = client.get(key.c_str()).get();
    if (resp.is_ok()) {
      value = resp.value().as_string();
      std::cout << "found in etcd " << key << " -> " << value << std::endl;
    } else if (resp.error_code() == etcd::ERROR_KEY_NOT_FOUND) {
      std::cout << "etcd key " << key << " not ready, sleep 1s" << std::endl;
      sleep(1);
      continue;
    } else {
      std::cout << "Error: " << resp.error_message() << std::endl;
    }
    break;
  }
  return value;
}

// 向etcd中插入key-value
void EtcdSetKV(etcd::Client &client, std::string key, std::string value) {
  client.set(key, value);
}