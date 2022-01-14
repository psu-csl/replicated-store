#include <iostream>
#include <optional>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cassert>
#include <cstring>

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
  for (;;) {
    auto cmd = ReadCommand(fd);
    if (cmd)
      tp_.Submit([this, fd, cmd = std::move(*cmd)]() { HandleCommand(fd, cmd); });
    else
      break;
  }
  int rc = close(fd);
  assert(rc == 0);
}

std::optional<Command> Replicant::ReadCommand(int fd) {
  std::string line = ReadLine(fd);
  if (line.empty())
    return std::nullopt;
  if (strncmp(line.c_str(), "get", 3) == 0)
    return Command {CommandType::kGet, line.substr(4), ""};
  if (strncmp(line.c_str(), "del", 3) == 0)
    return Command {CommandType::kDel, line.substr(4), ""};

  assert(strncmp(line.c_str(), "put", 3) == 0);
  size_t p = line.find(":", 4);
  return Command {CommandType::kPut, line.substr(4, p-4), line.substr(p+1)};
}

std::string ReadLine(int fd) {
  std::string line;
  for (;;) {
    char c;
    ssize_t n = read(fd, &c, 1);
    if (n == 1) {
      line.push_back(c);
      if (c == '\n')
        break;
    } else if (n == 0 || (n == -1 && errno != EINTR)) {
      break;
    }
  }
  return line;
}

void Replicant::HandleCommand(int fd, Command cmd) {
  bool is_get = cmd.type == CommandType::kGet;
  auto r = consensus_->AgreeAndExecute(std::move(cmd));
  if (r.ok) {
    write(fd, "ok", sizeof("ok"));
    if (is_get)
      write(fd, r.value.c_str(), r.value.size());
  } else {
    write(fd, "not-ok", sizeof("not-ok"));
  }
}
