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

void doSearchLocal(const Request *request, ResponseLocal *response);
void doSearchMerge(const Request *request, Response *response,
                   ResponseLocal resp_local[3]);

extern ThreadPool tp_io;
extern ThreadPool tp_cpu;

class AsyncServerImpl final {
public:
  void Run(int port) {
    std::string server_addr = std::string("0.0.0.0:") + std::to_string(port);
    ServerBuilder builder;
    builder.AddListeningPort(server_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service_);

    // 因为Search rpc会调用SearchLocal rpc,共用一个cq可能会死锁!
    for (int i = 0; i < search_cq_num; i++) {
      cqs1_.emplace_back(builder.AddCompletionQueue());
    }
    for (int i = 0; i < searchlocal_cq_num; i++) {
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
    for (int i = 0; i < search_cq_num; i++) {
      threads.push_back(
          std::thread(&AsyncServerImpl::HandleCallSearch, this, i));
    }
    for (int i = 0; i < searchlocal_cq_num; i++) {
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
    enum CallStatus { CREATE, PROCESS, PROCESS2, FINISH };
    CallStatus status_;

    std::atomic_int resp_counter_ = 0;
    grpc::ClientContext context_[3];
    ResponseLocal resp_local_[3];
    std::unique_ptr<grpc::ClientAsyncResponseReader<ResponseLocal>>
        response_reader_[3];
    grpc::Status finish_status_[3];

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
        status_ = PROCESS2;

        // 异步发出三个请求
        for (int i = 0; i < 3; i++) {
          response_reader_[i] =
              stubs[i]->PrepareAsyncSearchLocal(&context_[i], request_, cq_);
          response_reader_[i]->StartCall();
          response_reader_[i]->Finish(&resp_local_[i], &finish_status_[i],
                                      this);
        }
      } else if (status_ == PROCESS2) {
        auto cnt = resp_counter_.fetch_add(1);
        if (cnt == 2) {
          tp_io.enqueue([&]() {
            status_ = FINISH;
            doSearchMerge(&request_, &reply_, resp_local_);
            responder_.Finish(reply_, Status::OK, this);
          });
        }
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
        tp_cpu.enqueue([&]() {
          new CallSearchLocal(service_, cq_);
          // 正式操作，生成Response
          doSearchLocal(&request_, &reply_);
          status_ = FINISH;
          responder_.Finish(reply_, Status::OK, this);
        });
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
      GPR_ASSERT(ok);
      CallSearch *call = static_cast<CallSearch *>(tag);
      // 这个Proceed没有太多耗时操作，耗时的网络io已经放到线程池中处理
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
      // 最多16个节点同时进行Proceed，不过也够了，毕竟最多16核
      call->Proceed(ok);
    }
  }

  std::vector<std::unique_ptr<ServerCompletionQueue>> cqs1_, cqs2_;
  SearchService::AsyncService service_;
  std::unique_ptr<Server> server_;
  std::mutex mtx_;
};