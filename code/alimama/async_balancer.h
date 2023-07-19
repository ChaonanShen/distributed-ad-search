#include <bits/stdc++.h>

#include <etcd/Client.hpp>
#include <grpcpp/grpcpp.h>

#include "alimama.grpc.pb.h"

#include "util.h"

using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;

using alimama::proto::Request;
using alimama::proto::Response;
using alimama::proto::SearchService;

#if RUN_REMOTE
// 创建一个etcd客户端
extern etcd::Client etcd_client;
#endif

// server1 ip1:50051 server2 ip2:50052 server3 ip3:50053
extern std::string searchServerAddrs[3];

// 每次new CallSearch时候将stub_idx++，然后选择lb_stubs[stub_idx]
std::atomic_uint stub_idx = 0;
// 一个stub&channel背后其实有很多物理连接
extern std::vector<std::unique_ptr<SearchService::Stub>> lb_stubs;

class CallBase {
public:
  virtual void Proceed() = 0;
};

class CallSearch;

class AsyncBalancerClient : CallBase {
  SearchService::Stub *stub_;
  ServerCompletionQueue *cq_;
  ClientContext context_;
  // 其实不需要这个ClientStatus 正常情况只有PROCESS这个状态
  enum class ClientStatus { PROCESS = 1, FINISH = 2 };
  ClientStatus status_; // 调用AsyncSearch和
  std::unique_ptr<ClientAsyncResponseReader<Response>> stream_;

  Request *request_;
  Response *response_;

  grpc::Status finish_status_ = grpc::Status::OK;
  CallSearch *parent_call_base_ = nullptr;

public:
  AsyncBalancerClient(SearchService::Stub *stub, ServerCompletionQueue *cq,
                      CallSearch *parent_call_base, Request *request,
                      Response *response)
      : stub_(stub), cq_(cq), request_(request), response_(response),
        parent_call_base_(parent_call_base) {}
  void AsyncSearch();
  void Proceed() override;
};

class CallSearch : CallBase {
  SearchService::AsyncService *service_;
  ServerCompletionQueue *cq_;
  ServerContext ctx_;
  Request request_;
  Response reply_;
  // 整个Response形成后写回给客户端
  ServerAsyncResponseWriter<Response> responder_;
  enum CallStatus { CREATE, PROCESS, FINISH };
  CallStatus status_;

  AsyncBalancerClient *client_;

public:
  CallSearch(SearchService::AsyncService *service, ServerCompletionQueue *cq)
      : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {

    service_->RequestSearch(&ctx_, &request_, &responder_, cq_, cq_, this);
    status_ = PROCESS;

    // 挑出stub
    uint idx = stub_idx.fetch_add(1);
    client_ = new AsyncBalancerClient(lb_stubs[idx % 3].get(), cq_, this,
                                      &request_, &reply_);
  }

  void NotifyDone() {
    // client的AsyncSearch调用得到结果后回调NotifyDone，reply_已经写入
    status_ = FINISH;
    responder_.Finish(reply_, grpc::Status::OK, this);
  }

  void Proceed() override {
    if (status_ == CREATE) {
      status_ = PROCESS;
      service_->RequestSearch(&ctx_, &request_, &responder_, cq_, cq_, this);
    } else if (status_ == PROCESS) {
      new CallSearch(service_, cq_);
      // 这个client异步调用结束后
      client_->AsyncSearch();
    } else {
      GPR_ASSERT(status_ == FINISH);
      delete this;
    }
  }
};

void AsyncBalancerClient::AsyncSearch() {
  stream_ = stub_->PrepareAsyncSearch(&context_, *request_, cq_);
  stream_->StartCall();
  stream_->Finish(response_, &finish_status_, this);
  status_ = ClientStatus::PROCESS;
}

void AsyncBalancerClient::Proceed() {
  if (status_ == ClientStatus::PROCESS) {
    parent_call_base_->NotifyDone();
    delete this;
  } else {
    std::cout << "Unexpected status" << std::endl;
    assert(false);
  }
}

class AsyncBalancerImpl final {
  std::unique_ptr<ServerCompletionQueue> cq_;
  SearchService::AsyncService service_;
  std::unique_ptr<Server> server_;

public:
  ~AsyncBalancerImpl() {
    server_->Shutdown();
    cq_->Shutdown();
  }

  void Run() {
    std::string server_address = ("0.0.0.0:56789");

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service_);
    cq_ = builder.AddCompletionQueue();

    server_ = builder.BuildAndStart();
    std::cout << "Balancer listening on " << server_address << std::endl;

    // 对外注册
#if RUN_REMOTE
    // 将服务地址注册到etcd中
    // 相当于 etcdctl put /services/searchservice ip:port
    std::string external_address = getLocalIP() + std::string(":56789");
    std::string key = std::string("/services/searchservice");
    auto response = etcd_client.set(key, external_address).get();
    if (response.is_ok()) {
      std::cout << "Service registration successful.\n";
    } else {
      std::cerr << "Service registration failed: " << response.error_message()
                << "\n";
    }
#endif

    new CallSearch(&service_, cq_.get());
    std::thread th(&AsyncBalancerImpl::HandleRpcs, this);

    th.join();
  }

private:
  void HandleRpcs() {
    void *tag;
    bool ok;
    while (true) {
      GPR_ASSERT(cq_->Next(&tag, &ok));
      CallBase *base = (CallBase *)tag;
      base->Proceed();
    }
  }
};