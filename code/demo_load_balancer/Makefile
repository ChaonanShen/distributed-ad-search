IMAGE = public-images-registry.cn-hangzhou.cr.aliyuncs.com/public/alimama-2023:B-v1

HOST_SYSTEM = $(shell uname | cut -f 1 -d_)
SYSTEM ?= $(HOST_SYSTEM)
CXX = g++
CPPFLAGS += `pkg-config --cflags grpc protobuf`
CXXFLAGS += -std=c++17


LDFLAGS += -L/usr/local/lib \
		   -letcd-cpp-api\
		   -lcpprest\
		   -lboost_log\
		   -lssl\
		   -lcrypto\
		   -O2 -g\
       -pthread\
		   `pkg-config --libs --static protobuf grpc++`\
       -ldl

PROTOC = protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

PROTOS_PATH = ./

vpath %.proto $(PROTOS_PATH)

all: server client load_balancer

server: service.pb.o service.grpc.pb.o server.o
	$(CXX) $^ $(LDFLAGS) -o $@

client: service.pb.o service.grpc.pb.o client.o
	$(CXX) $^ $(LDFLAGS) -o $@

load_balancer: service.pb.o service.grpc.pb.o load_balancer.o
	$(CXX) $^ $(LDFLAGS) -o $@

.PRECIOUS: %.grpc.pb.cc
%.grpc.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

.PRECIOUS: %.pb.cc
%.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=. $<

clean:
	rm -f *.o *.pb.cc *.pb.h server client load_balancer

docker:
	docker run --rm -it  -v ./:/work  -w /work ${IMAGE} bash