#include <bits/stdc++.h>

#include "async_balancer.h"

// server1 ip1:50051 server2 ip2:50052 server3 ip3:50053
std::string searchServerAddrs[3];

// 一个stub&channel背后其实有很多物理连接
std::vector<std::unique_ptr<SearchService::Stub>> lb_stubs;

#if RUN_REMOTE
// 创建一个etcd客户端
etcd::Client etcd_client("http://etcd:2379");
#endif

int main() {
#if RUN_REMOTE
  // 等待三个server将数据都准备好
  searchServerAddrs[0] = EtcdGetKVWait(etcd_client, "/node1");
  searchServerAddrs[1] = EtcdGetKVWait(etcd_client, "/node2");
  searchServerAddrs[2] = EtcdGetKVWait(etcd_client, "/node3");
#endif

  for (int i = 0; i < 3; i++) {
    std::cout << "balancer found server" << i << " " << searchServerAddrs[i]
              << std::endl;
  }

  for (int i = 0; i < 3; i++) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
        searchServerAddrs[i], grpc::InsecureChannelCredentials());
    lb_stubs.emplace_back(SearchService::NewStub(channel));
  }

  AsyncBalancerImpl server;
  server.Run();
}