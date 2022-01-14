#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cassert>
#include <cstring>
#include <chrono>

#include "replicant.h"

Replicant::Replicant()
    : consensus_(new Paxos(new MemStore())),
      client_fd_(-1),
      done_(false) {

  client_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  assert(client_fd_ != -1);

  int optval = 1;
  setsockopt(client_fd_, SOL_SOCKET, SO_REUSEADDR,
	     (const void *)&optval , sizeof(int));

  struct sockaddr_in addr;
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(CLIENT_PORT);

  int rc = bind(client_fd_, (struct sockaddr *) &addr, sizeof(addr));
  assert(rc == 0);

  rc = listen(client_fd_, 5);
  assert(rc == 0);
}

Replicant::~Replicant() {
  done_ = false;
}

void Replicant::Run() {
  uint32_t n;
  struct sockaddr_in addr;

  while (!done_) {
    int fd = accept(client_fd_, (struct sockaddr *) &addr, &n);
    assert(fd != -1);
    tp_.Submit([this, fd]() { HandleClient(fd); });
  }
}

void Replicant::HandleClient(int fd) {
  char buf[1024];
  int nread, nwrite;
  for (;;) {
    memset(buf, 0, sizeof(buf));
    nread = read(fd, buf, 1024);
    assert(nread >= 0);
    if (nread == 0)
      break;
    nwrite = write(fd, buf, strlen(buf));
    assert(nwrite == nread);
  }
  int rc = close(fd);
  assert(rc == 0);
}
