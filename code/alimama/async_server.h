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
    cq_ = builder.AddCompletionQueue();
    server_ = builder.BuildAndStart();

    std::cout << "Server listening on " << server_addr << std::endl;

    // 处理从cq中获得的事件
    HandleRpcs();
  }

  ~AsyncServerImpl() {
    server_->Shutdown();
    cq_->Shutdown();
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
        std::cout << "CallSearch - Proceed CREATE" << std::endl;

        status_ = PROCESS;
        service_->RequestSearch(&ctx_, &request_, &responder_, cq_, cq_, this);
      } else if (status_ == PROCESS) {
        // 准备处理当前事件，新生成一个对象，接收新的请求
        new CallSearch(service_, cq_);

        std::cout << "CallSearch - Proceed PROCESS" << std::endl;
        // 正式操作，生成Response
        doSearch(&request_, &reply_);
        status_ = FINISH;
        responder_.Finish(reply_, Status::OK, this);
      } else {
        std::cout << "CallSearch - Proceed FINISH" << std::endl;

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
        std::cout << "CallSearchLocal - Proceed CREATE" << std::endl;

        status_ = PROCESS;
        service_->RequestSearchLocal(&ctx_, &request_, &responder_, cq_, cq_,
                                     this);
      } else if (status_ == PROCESS) {
        // 准备处理当前事件，新生成一个对象，接收新的请求
        new CallSearchLocal(service_, cq_);

        std::cout << "CallSearchLocal - Proceed PROCESS" << std::endl;
        // 正式操作，生成Response
        doSearchLocal(&request_, &reply_);
        status_ = FINISH;
        responder_.Finish(reply_, Status::OK, this);
      } else {
        std::cout << "CallSearchLocal - Proceed FINISH" << std::endl;

        GPR_ASSERT(status_ == FINISH);
        delete this;
      }
    }
  };

  void HandleRpcs() {
    new CallSearch(&service_, cq_.get());
    new CallSearchLocal(&service_, cq_.get());
    void *tag;
    bool ok;
    while (true) {
      GPR_ASSERT(cq_->Next(&tag, &ok));
      GPR_ASSERT(ok);
      static_cast<CallBase *>(tag)->Proceed();
      std::cout << "HandleRpcs process one request" << std::endl;
    }
  }

  std::unique_ptr<ServerCompletionQueue> cq_;
  SearchService::AsyncService service_;
  std::unique_ptr<Server> server_;
};