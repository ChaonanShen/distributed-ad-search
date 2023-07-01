#include <arpa/inet.h>
#include <cstdlib>
#include <ifaddrs.h>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "util.h"

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