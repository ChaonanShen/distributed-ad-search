#include <bits/stdc++.h>

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>

#include "alimama.grpc.pb.h"

#include "util.h"

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

ThreadPool tp(64);

class AsyncServerImpl final {
public:
  void Run(int port) {
    std::string server_addr = std::string("0.0.0.0:") + std::to_string(port);
    ServerBuilder builder;
    builder.AddListeningPort(server_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service_);

    // 因为Search rpc会调用SearchLocal rpc,共用一个cq可能会死锁!
    for (int i = 0; i < server_cq_num; i++) {
      cqs1_.emplace_back(builder.AddCompletionQueue());
      cqs2_.emplace_back(builder.AddCompletionQueue());
    }

    server_ = builder.BuildAndStart();

    std::cout << "Server listening on " << server_addr << std::endl;

    // 可以只用一个HandleRpcs(),也可以用多个,反正CompletionQueue是并发安全的
    // 处理从cq中获得的事件

    // HandleCallSearch和HandleCallSearchLocal比例应该是1:1
    // 因为一个Request在某个节点上Search和SearchLocal调用数量是一样的！
    std::vector<std::thread> threads;
    // 把这个cq的数量弄大，qps倍增
    for (int i = 0; i < server_cq_num; i++) {
      threads.push_back(
          std::thread(&AsyncServerImpl::HandleCallSearch, this, i));
    }
    for (int i = 0; i < server_cq_num; i++) {
      threads.push_back(
          std::thread(&AsyncServerImpl::HandleCallSearchLocal, this, i));
    }
    for (auto &th : threads) {
      th.join();
    }
  }

  ~AsyncServerImpl() {
    server_->Shutdown();
    for (auto &cq : cqs1_) {
      cq->Shutdown();
    }
    for (auto &cq : cqs2_) {
      cq->Shutdown();
    }
  }

private:
  class CallBase {
  public:
    virtual void Proceed(bool ok = true) = 0;
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

    // 搞清楚哪些是grpc给出的机制，哪些是自己把控的
    void Proceed(bool ok = true) override {
      if (!ok) {
        // 如果出问题，直接退出
        // TODO(scn): status_不能返回ok
        responder_.Finish(reply_, Status::OK, this);
        status_ = FINISH;
      }

      if (status_ == CREATE) {
        status_ = PROCESS;
        // RequestSearch调用后，遇到一个客户端连接就会告知
        service_->RequestSearch(&ctx_, &request_, &responder_, cq_, cq_, this);
      } else if (status_ == PROCESS) {
        // 准备处理当前事件，新生成一个对象，接收新的请求
        new CallSearch(service_, cq_);
        // 正式操作，生成Response
        // TODO(scn):
        // 这个能否在另一个线程中异步进行，然后好了之后再通过cq提醒(反正Finish之后还会继续cq提醒)
        tp.enqueue([&]() {
          doSearch(&request_, &reply_);
          status_ = FINISH;
          responder_.Finish(reply_, Status::OK, this);
        });
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

    void Proceed(bool ok = true) override {
      if (!ok) {
        responder_.Finish(reply_, Status::OK, this);
        status_ = FINISH;
      }
      if (status_ == CREATE) {
        status_ = PROCESS;
        service_->RequestSearchLocal(&ctx_, &request_, &responder_, cq_, cq_,
                                     this);
      } else if (status_ == PROCESS) {
        // 准备处理当前事件，新生成一个对象，接收新的请求
        new CallSearchLocal(service_, cq_);
        // 正式操作，生成Response
        // 如果doSearchLocal很快，就不用搞个线程了
        doSearchLocal(&request_, &reply_);
        status_ = FINISH;
        responder_.Finish(reply_, Status::OK, this);
      } else {
        GPR_ASSERT(status_ == FINISH);
        delete this;
      }
    }
  };

  // 我突然想到，其实CallSearch和CallSearchLocal其实完全可以共享CompletionQueue
  // 不能共享！Search里会调用SearchLocal，共享cq可能死锁！

  void HandleCallSearch(int cq_idx) {
    new CallSearch(&service_, cqs1_[cq_idx].get());
    void *tag;
    bool ok;
    while (true) {
      GPR_ASSERT(cqs1_[cq_idx]->Next(&tag, &ok));
      // TODO(scn): doSearch改成异步后，这里好像容易出问题(好像又不止)
      // TODO(scn): 现在ok居然可能为false，这点让我很担心，之前程序都不会
      // GPR_ASSERT(ok);
      CallSearch *call = static_cast<CallSearch *>(tag);
      // TODO(scn): 不ok就直接FINISH阶段
      call->Proceed(ok);
    }
  }

  void HandleCallSearchLocal(int cq_idx) {
    new CallSearchLocal(&service_, cqs2_[cq_idx].get());
    void *tag;
    bool ok;
    while (true) {
      GPR_ASSERT(cqs2_[cq_idx]->Next(&tag, &ok));
      CallSearchLocal *call = static_cast<CallSearchLocal *>(tag);
      call->Proceed(ok);
    }
  }

  std::vector<std::unique_ptr<ServerCompletionQueue>> cqs1_, cqs2_;
  SearchService::AsyncService service_;
  std::unique_ptr<Server> server_;
  std::mutex mtx_;
};