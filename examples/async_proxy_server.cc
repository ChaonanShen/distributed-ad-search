#include <grpc++/grpc++.h>
#include <grpc/support/log.h>

#include "helloworld.grpc.pb.h"

using grpc::Channel;
using grpc::ClientAsyncReaderWriter;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;

using grpc::Server;
using grpc::ServerAsyncReaderWriter;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;

using helloworld::Greeter;
using helloworld::HelloReply;
using helloworld::HelloRequest;

int g_thread_num = 1;
int g_cq_num = 1;
int g_ins_pool = 1;
int g_channel_pool = 1;

std::atomic<void *> **g_instance_pool = nullptr;

typedef std::shared_ptr<::grpc::Channel> ShpChannel;
typedef std::vector<ShpChannel> VecChannel;
typedef std::unique_ptr<VecChannel> UptrVecChannel;

std::vector<std::string> g_vec_backends{"localhost:50052", "localhost:50053"};

struct ChannelPool {

  ChannelPool() {

    for (const auto &_svr : g_vec_backends) {
      this->m_backend_pool.insert(std::pair<std::string, UptrVecChannel>(
          _svr, UptrVecChannel(new VecChannel())));
      for (int i = 0; i < g_channel_pool; ++i)
        this->m_backend_pool[_svr]->emplace_back(
            grpc::CreateChannel(_svr, grpc::InsecureChannelCredentials()));
    }
  }

  ShpChannel PickupOneChannel(const std::string &svr) {
    if (m_backend_pool.find(svr) == m_backend_pool.cend())
      return ShpChannel();

    auto &_uptr_vec = m_backend_pool[svr];
    return _uptr_vec->operator[](this->GenerateRandom(0, g_channel_pool - 1));
  }

  uint32_t GenerateRandom(uint32_t from, uint32_t to) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<unsigned long> dis(from, to);
    return dis(gen);
  }

  // Read only after initialized.
  std::map<std::string, UptrVecChannel> m_backend_pool;
} *g_channel;

class CallDataBase {

public:
  CallDataBase() {}
  virtual void Proceed(bool ok) = 0;
};

class CallDataServerBase : public CallDataBase {
public:
  CallDataServerBase(Greeter::AsyncService *service, ServerCompletionQueue *cq)
      : service_(service), cq_(cq) {}

protected:
  // The means of communication with the gRPC runtime for an asynchronous
  // server.
  Greeter::AsyncService *service_;
  // The producer-consumer queue where for asynchronous server notifications.
  ServerCompletionQueue *cq_;

  // Context for the rpc, allowing to tweak aspects of it such as the use
  // of compression, authentication, as well as to send metadata back to the
  // client.
  ServerContext ctx_;

  // What we get from the client.
  HelloRequest request_;
  // What we send back to the client.
  HelloReply reply_;
};

class CallDataUnary;

class AsyncUnaryGreeterClient : public CallDataBase {
  enum class ClientStatus { PROCESS = 1, FINISH = 2 };

public:
  explicit AsyncUnaryGreeterClient(std::shared_ptr<Channel> channel,
                                   ServerCompletionQueue *cq,
                                   CallDataUnary *pcd);

  ~AsyncUnaryGreeterClient();

  // Similar to the async hello example in greeter_async_client but does not
  // wait for the response. Instead queues up a tag in the completion queue
  // that is notified when the server responds back (or when the stream is
  // closed). Returns false when the stream is requested to be closed.
  void AsyncSayHello(const std::string &user);

  // Runs a gRPC completion-queue processing thread. Checks for 'Next' tag
  // and processes them until there are no more (or when the completion queue
  // is shutdown).
  void Proceed(bool ok);

private:
  // Context for the client. It could be used to convey extra information to
  // the server and/or tweak certain RPC behaviors.
  ClientContext context_;

  ClientStatus status_;

  // Out of the passed in Channel comes the stub, stored here, our view of the
  // server's exposed services.
  std::unique_ptr<Greeter::Stub> stub_;

  // The bidirectional, asynchronous stream for sending/receiving messages.
  std::unique_ptr<ClientAsyncResponseReader<HelloReply>> stream_;

  ServerCompletionQueue *cq_;

  // Allocated protobuf that holds the response. In real clients and servers,
  // the memory management would a bit more complex as the thread that fills
  // in the response should take care of concurrency as well as memory
  // management.
  HelloRequest request_;
  HelloReply response_;

  // Finish status when the client is done with the stream.
  grpc::Status finish_status_ = grpc::Status::OK;

  CallDataUnary *m_parent_call_data = nullptr;
};

class CallDataUnary : CallDataServerBase {
public:
  // Take in the "service" instance (in this case representing an asynchronous
  // server) and the completion queue "cq" used for asynchronous communication
  // with the gRPC runtime.
  CallDataUnary(Greeter::AsyncService *service, ServerCompletionQueue *cq)
      : CallDataServerBase(service, cq), responder_(&ctx_), status_(CREATE) {
    // Invoke the serving logic right away.

    // As part of the initial CREATE state, we *request* that the system
    // start processing SayHello requests. In this request, "this" acts are
    // the tag uniquely identifying the request (so that different CallDataUnary
    // instances can serve different requests concurrently), in this case
    // the memory address of this CallDataUnary instance.

    // 等待客户端请求发来，然后就能立马进入Proceed的PROCESS状态
    service_->RequestSayHello(&ctx_, &request_, &responder_, cq_, cq_, this);
    status_ = PROCESS;

    for (const auto &_svr : g_vec_backends) {
      auto _shp_channel = g_channel->PickupOneChannel(_svr);
      m_backends.emplace_back(
          new AsyncUnaryGreeterClient(_shp_channel, cq, this));
    }
  }

  void NotifyOneDone() {
    m_done_counter++;
    if (m_done_counter == g_vec_backends.size()) {
      reply_.set_message("arthur");
      status_ = FINISH;
      responder_.Finish(reply_, Status::OK, this);
      m_done_counter = 0;
    }
  }

  void Proceed(bool ok) {

    if (status_ == PROCESS) {
      // Spawn a new CallDataUnary instance to serve new clients while we
      // process the one for this CallDataUnary. The instance will deallocate
      // itself as part of its FINISH state.

      new CallDataUnary(service_, cq_);

      // broadcasting
      for (auto *_p_svr : m_backends)
        _p_svr->AsyncSayHello(request_.name());

    } else {
      GPR_ASSERT(status_ == FINISH);
      // Once in the FINISH state, deallocate ourselves (CallDataUnary).
      delete this;
    }
  }

private:
  std::list<AsyncUnaryGreeterClient *> m_backends;
  
  // 是不是得改成atomic?
  int m_done_counter = 0;

  // The means to get back to the client.
  ServerAsyncResponseWriter<HelloReply> responder_;

  // Let's implement a tiny state machine with the following states.
  enum CallStatus { CREATE, PROCESS, FINISH };
  CallStatus status_; // The current serving state.
};

AsyncUnaryGreeterClient::AsyncUnaryGreeterClient(
    std::shared_ptr<Channel> channel, ServerCompletionQueue *cq,
    CallDataUnary *pcd) {
  stub_ = Greeter::NewStub(channel);
  m_parent_call_data = pcd;
  cq_ = cq;
}

AsyncUnaryGreeterClient::~AsyncUnaryGreeterClient() {
  std::cout << "destructing client" << std::endl;
}

void AsyncUnaryGreeterClient::AsyncSayHello(const std::string &user) {

  request_.set_name(user);

  std::cout << "entrust unary req to peer:" << context_.peer() << std::endl;

  stream_ = stub_->PrepareAsyncSayHello(&context_, request_, cq_);
  stream_->StartCall();
  stream_->Finish(&response_, &finish_status_, this);

  status_ = ClientStatus::PROCESS;

  return;
}

// Runs a gRPC completion-queue processing thread. Checks for 'Next' tag
// and processes them until there are no more (or when the completion queue
// is shutdown).
void AsyncUnaryGreeterClient::Proceed(bool ok) {

  // For all cases that the CQ didn't return a true result, just close the
  // stream.
  if (!ok) {
    std::cout << "Unary client get non-ok result from peer:" << context_.peer()
              << std::endl;
    return;
  }

  switch (status_) {
  case ClientStatus::PROCESS:
    std::cout << "Read a new message:" << response_.message()
              << " from peer:" << context_.peer() << std::endl;
    m_parent_call_data->NotifyOneDone();
    delete this;
    break;
  default:
    std::cerr << "Unexpected status" << std::endl;
    assert(false);
  }
}

class CallDataBidi;

class AsyncBidiGreeterClient : public CallDataBase {
  enum class ClientStatus {
    READ = 1,
    WRITE = 2,
    CONNECT = 3,
    WRITES_DONE = 4,
    FINISH = 5
  };

public:
  explicit AsyncBidiGreeterClient(std::shared_ptr<Channel> channel,
                                  ServerCompletionQueue *cq, CallDataBidi *pcd);

  ~AsyncBidiGreeterClient();

  // Similar to the async hello example in greeter_async_client but does not
  // wait for the response. Instead queues up a tag in the completion queue
  // that is notified when the server responds back (or when the stream is
  // closed). Returns false when the stream is requested to be closed.
  void AsyncSayHello(const std::string &user);

  // Runs a gRPC completion-queue processing thread. Checks for 'Next' tag
  // and processes them until there are no more (or when the completion queue
  // is shutdown).
  void Proceed(bool ok);

private:
  // Context for the client. It could be used to convey extra information to
  // the server and/or tweak certain RPC behaviors.
  ClientContext context_;

  ClientStatus status_;

  // Out of the passed in Channel comes the stub, stored here, our view of the
  // server's exposed services.
  std::unique_ptr<Greeter::Stub> stub_;

  // The bidirectional, asynchronous stream for sending/receiving messages.
  std::unique_ptr<ClientAsyncReaderWriter<HelloRequest, HelloReply>> stream_;

  // Allocated protobuf that holds the response. In real clients and servers,
  // the memory management would a bit more complex as the thread that fills
  // in the response should take care of concurrency as well as memory
  // management.
  HelloRequest request_;
  HelloReply response_;

  // Finish status when the client is done with the stream.
  grpc::Status finish_status_ = grpc::Status::OK;

  CallDataBidi *m_parent_call_data = nullptr;
};

class CallDataBidi : CallDataServerBase {

public:
  // Take in the "service" instance (in this case representing an asynchronous
  // server) and the completion queue "cq" used for asynchronous communication
  // with the gRPC runtime.
  CallDataBidi(Greeter::AsyncService *service, ServerCompletionQueue *cq)
      : CallDataServerBase(service, cq), rw_(&ctx_) {
    // Invoke the serving logic right away.

    service_->RequestSayHelloEx(&ctx_, &rw_, cq_, cq_, this);
    status_ = BidiStatus::CONNECT;

    ctx_.AsyncNotifyWhenDone(this);

    for (const auto &_svr : g_vec_backends) {
      auto _shp_channel = g_channel->PickupOneChannel(_svr);
      m_backends.emplace_back(
          new AsyncBidiGreeterClient(_shp_channel, cq, this));
    }
  }

  void NotifyOneDone() {
    m_done_counter++;
    if (m_done_counter == g_vec_backends.size()) {
      reply_.set_message("arthur");
      rw_.Write(reply_, this);
      status_ = BidiStatus::WRITE;
      m_done_counter = 0;
    }
  }

  void Proceed(bool ok) {

    switch (status_) {
    case BidiStatus::READ:

      // Meaning client said it wants to end the stream either by a 'writedone'
      // or 'finish' call.
      if (!ok) {
        Status _st(StatusCode::OUT_OF_RANGE, "test error msg");

        // broadcasting quit msg.
        for (auto *_p_svr : m_backends)
          _p_svr->AsyncSayHello("quit");

        rw_.Finish(_st, this);
        status_ = BidiStatus::DONE;
        break;
      }

      // broadcasting normal msg.
      for (auto *_p_svr : m_backends)
        _p_svr->AsyncSayHello(request_.name());

      std::cout << "tag:" << this << " Read a new message:" << request_.name()
                << std::endl;
      break;

    case BidiStatus::WRITE:
      std::cout << "tag:" << this << " Written a message:" << reply_.message()
                << std::endl;
      rw_.Read(&request_, this);
      status_ = BidiStatus::READ;
      break;

    case BidiStatus::CONNECT:
      std::cout << "tag:" << this << " connected:" << std::endl;
      new CallDataBidi(service_, cq_);
      rw_.Read(&request_, this);
      status_ = BidiStatus::READ;
      break;

    case BidiStatus::DONE:
      std::cout << "tag:" << this << " Server done." << std::endl;
      status_ = BidiStatus::FINISH;
      break;

    case BidiStatus::FINISH:
      std::cout << "tag:" << this << " Server finish." << std::endl;
      delete this;
      break;

    default:
      std::cerr << "Unexpected tag " << int(status_) << std::endl;
      assert(false);
    }
  }

private:
  std::list<AsyncBidiGreeterClient *> m_backends;

  int m_done_counter = 0;

  // The means to get back to the client.
  ServerAsyncReaderWriter<HelloReply, HelloRequest> rw_;

  // Let's implement a tiny state machine with the following states.
  enum class BidiStatus {
    READ = 1,
    WRITE = 2,
    CONNECT = 3,
    DONE = 4,
    FINISH = 5
  };
  BidiStatus status_;
};

AsyncBidiGreeterClient::AsyncBidiGreeterClient(std::shared_ptr<Channel> channel,
                                               ServerCompletionQueue *cq,
                                               CallDataBidi *pcd) {
  stub_ = Greeter::NewStub(channel);
  m_parent_call_data = pcd;

  stream_ = stub_->AsyncSayHelloEx(&context_, cq, this);
  status_ = ClientStatus::CONNECT;
}

AsyncBidiGreeterClient::~AsyncBidiGreeterClient() {
  std::cout << "destructing client" << std::endl;
}

void AsyncBidiGreeterClient::AsyncSayHello(const std::string &user) {
  if (user == "quit") {
    stream_->WritesDone(this);
    status_ = ClientStatus::WRITES_DONE;
    return;
  }

  // Data we are sending to the server.
  request_.set_name(user);

  // This is important: You can have at most one write or at most one read
  // at any given time. The throttling is performed by gRPC completion
  // queue. If you queue more than one write/read, the stream will crash.
  // Because this stream is bidirectional, you *can* have a single read
  // and a single write request queued for the same stream. Writes and reads
  // are independent of each other in terms of ordering/delivery.
  // std::cout << " ** Sending request: " << user << std::endl;
  stream_->Write(request_, this);
  status_ = ClientStatus::WRITE;
  return;
}

// Runs a gRPC completion-queue processing thread. Checks for 'Next' tag
// and processes them until there are no more (or when the completion queue
// is shutdown).
void AsyncBidiGreeterClient::Proceed(bool ok) {

  // For all cases that the CQ didn't return a true result, just close the
  // stream.
  if (!ok) {
    stream_->Finish(&finish_status_, this);
    status_ = ClientStatus::FINISH;
    return;
  }

  switch (status_) {
  case ClientStatus::READ:
    std::cout << "Read a new message:" << response_.message()
              << " from peer:" << context_.peer() << std::endl;
    m_parent_call_data->NotifyOneDone();
    break;
  case ClientStatus::WRITE:
    std::cout << "Sent message :" << request_.name()
              << " to peer:" << context_.peer() << std::endl;
    stream_->Read(&response_, this);
    status_ = ClientStatus::READ;
    break;
  case ClientStatus::CONNECT:
    std::cout << "connect to " << context_.peer() << " succeed." << std::endl;
    break;
  case ClientStatus::WRITES_DONE:
    std::cout << "writesdone sent" << std::endl;
    stream_->Finish(&finish_status_, this);
    status_ = ClientStatus::FINISH;
    break;
  case ClientStatus::FINISH:
    std::cout << "Client finish status:" << finish_status_.error_code()
              << ", msg:" << finish_status_.error_message() << std::endl;
    delete this;
    break;
  default:
    std::cerr << "Unexpected status" << std::endl;
    assert(false);
  }
}

class ServerImpl final {
public:
  ~ServerImpl() {
    server_->Shutdown();
    // Always shutdown the completion queue after the server.
    for (const auto &_cq : m_cq)
      _cq->Shutdown();
  }

  // There is no shutdown handling in this code.
  void Run() {
    std::string server_address("0.0.0.0:50051");

    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service_" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *asynchronous* service.
    builder.RegisterService(&service_);
    // Get hold of the completion queue used for the asynchronous communication
    // with the gRPC runtime.

    for (int i = 0; i < g_cq_num; ++i) {
      // cq_ = builder.AddCompletionQueue();
      m_cq.emplace_back(builder.AddCompletionQueue());
    }

    // Finally assemble the server.
    server_ = builder.BuildAndStart();
    std::cout << "Server listening on " << server_address << std::endl;

    // Proceed to the server's main loop.
    std::vector<std::thread *> _vec_threads;

    for (int i = 0; i < g_thread_num; ++i) {
      int _cq_idx = i % g_cq_num;
      for (int j = 0; j < g_ins_pool; ++j) {
        new CallDataUnary(&service_, m_cq[_cq_idx].get());
        new CallDataBidi(&service_, m_cq[_cq_idx].get());
      }

      _vec_threads.emplace_back(
          new std::thread(&ServerImpl::HandleRpcs, this, _cq_idx));
    }

    std::cout << g_thread_num << " working aysnc threads spawned" << std::endl;

    for (const auto &_t : _vec_threads)
      _t->join();
  }

private:
  // Class encompasing the state and logic needed to serve a request.

  // This can be run in multiple threads if needed.
  void HandleRpcs(int cq_idx) {
    uint32_t _counter = 0;
    // Spawn a new CallDataUnary instance to serve new clients.
    void *tag; // uniquely identifies a request.
    bool ok;
    while (true) {
      // Block waiting to read the next event from the completion queue. The
      // event is uniquely identified by its tag, which in this case is the
      // memory address of a CallDataUnary instance.
      // The return value of Next should always be checked. This return value
      // tells us whether there is any kind of event or cq_ is shutting down.
      // GPR_ASSERT(cq_->Next(&tag, &ok));
      GPR_ASSERT(m_cq[cq_idx]->Next(&tag, &ok));

      CallDataBase *_p_ins = (CallDataBase *)tag;
      _p_ins->Proceed(ok);
    }
  }

  std::vector<std::unique_ptr<ServerCompletionQueue>> m_cq;

  Greeter::AsyncService service_;
  std::unique_ptr<Server> server_;
};

const char *ParseCmdPara(char *argv, const char *para) {
  auto p_target = std::strstr(argv, para);
  if (p_target == nullptr) {
    printf("para error argv[%s] should be %s \n", argv, para);
    return nullptr;
  }
  p_target += std::strlen(para);
  return p_target;
}

int main(int argc, char **argv) {
  if (argc != 5) {
    std::cout
        << "Usage:./program --thread=xx --cq=xx --pool=xx --channel_pool=xx";
    return 0;
  }

  g_thread_num = std::atoi(ParseCmdPara(argv[1], "--thread="));
  g_cq_num = std::atoi(ParseCmdPara(argv[2], "--cq="));
  g_ins_pool = std::atoi(ParseCmdPara(argv[3], "--pool="));
  g_channel_pool = std::atoi(ParseCmdPara(argv[4], "--channel_pool="));

  g_channel = new ChannelPool();

  ServerImpl server;
  server.Run();

  return 0;
}