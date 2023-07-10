#include <bits/stdc++.h>

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>

#include "alimama.grpc.pb.h"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;

using alimama::proto::AdgroupResp;
using alimama::proto::Request;
using alimama::proto::Response;
using alimama::proto::ResponseLocal;
using alimama::proto::SearchService;

void doSearch(const Request *request, Response *response);
void doSearchLocal(const Request *request, ResponseLocal *response);

class AsyncServerImpl final {
public:
  void Run(int port) {
    std::string server_addr = std::string("0.0.0.0:") + std::to_string(port);
    ServerBuilder builder;
    builder.AddListeningPort(server_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service_);

    // 因为Search rpc会调用SearchLocal rpc,共用一个cq可能会死锁!
    cq1_ = builder.AddCompletionQueue();
    cq2_ = builder.AddCompletionQueue();

    server_ = builder.BuildAndStart();

    std::cout << "Server listening on " << server_addr << std::endl;

    // 可以只用一个HandleRpcs(),也可以用多个,反正CompletionQueue是并发安全的
    // 处理从cq中获得的事件

    // HandleCallSearch和HandleCallSearchLocal比例应该是1:1
    // 因为一个Request在某个节点上Search和SearchLocal调用数量是一样的！
    std::vector<std::thread> threads;
    // 把这个cq的数量弄大，qps倍增
    const int num_search_threads = 16;
    for (int i = 0; i < num_search_threads; i++) {
      threads.push_back(
          std::thread(std::bind(&AsyncServerImpl::HandleCallSearch, this)));
    }
    for (int i = 0; i < num_search_threads; i++) {
      threads.push_back(std::thread(
          std::bind(&AsyncServerImpl::HandleCallSearchLocal, this)));
    }
    for (auto &th : threads) {
      th.join();
    }
  }

  ~AsyncServerImpl() {
    server_->Shutdown();
    cq1_->Shutdown();
    cq2_->Shutdown();
  }

private:
  class CallBase {
  public:
    virtual void Proceed() = 0;
  };

  class CallSearch : public CallBase {
    SearchService::AsyncService *service_;
    ServerCompletionQueue *cq_;
    ServerContext ctx_;
    Request request_;
    Response reply_;
    ServerAsyncResponseWriter<Response> responder_;
    enum CallStatus { CREATE, PROCESS, FINISH };
    CallStatus status_;

  public:
    CallSearch(SearchService::AsyncService *service, ServerCompletionQueue *cq)
        : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
      Proceed();
    }

    void Proceed() override {
      if (status_ == CREATE) {
        status_ = PROCESS;
        service_->RequestSearch(&ctx_, &request_, &responder_, cq_, cq_, this);
      } else if (status_ == PROCESS) {
        // 准备处理当前事件，新生成一个对象，接收新的请求
        new CallSearch(service_, cq_);
        // 正式操作，生成Response
        doSearch(&request_, &reply_);
        status_ = FINISH;
        responder_.Finish(reply_, Status::OK, this);
      } else {
        GPR_ASSERT(status_ == FINISH);
        delete this;
      }
    }
  };

  class CallSearchLocal : public CallBase {
    SearchService::AsyncService *service_;
    ServerCompletionQueue *cq_;
    ServerContext ctx_;
    Request request_;
    ResponseLocal reply_;
    ServerAsyncResponseWriter<ResponseLocal> responder_;
    enum CallStatus { CREATE, PROCESS, FINISH };
    CallStatus status_;

  public:
    CallSearchLocal(SearchService::AsyncService *service,
                    ServerCompletionQueue *cq)
        : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
      Proceed();
    }

    void Proceed() override {
      if (status_ == CREATE) {
        status_ = PROCESS;
        service_->RequestSearchLocal(&ctx_, &request_, &responder_, cq_, cq_,
                                     this);
      } else if (status_ == PROCESS) {
        // 准备处理当前事件，新生成一个对象，接收新的请求
        new CallSearchLocal(service_, cq_);
        // 正式操作，生成Response
        doSearchLocal(&request_, &reply_);
        status_ = FINISH;
        responder_.Finish(reply_, Status::OK, this);
      } else {
        GPR_ASSERT(status_ == FINISH);
        delete this;
      }
    }
  };

  void HandleCallSearch() {
    new CallSearch(&service_, cq1_.get());
    std::thread t([this]() {
      void *tag;
      bool ok;
      while (true) {
        GPR_ASSERT(cq1_->Next(&tag, &ok));
        GPR_ASSERT(ok);
        static_cast<CallSearch *>(tag)->Proceed();
      }
    });
    t.join();
  }

  void HandleCallSearchLocal() {
    new CallSearchLocal(&service_, cq2_.get());
    std::thread t([this]() {
      void *tag;
      bool ok;
      while (true) {
        GPR_ASSERT(cq2_->Next(&tag, &ok));
        GPR_ASSERT(ok);
        static_cast<CallSearchLocal *>(tag)->Proceed();
      }
    });
    t.join();
  }

  std::unique_ptr<ServerCompletionQueue> cq1_, cq2_;
  SearchService::AsyncService service_;
  std::unique_ptr<Server> server_;
};