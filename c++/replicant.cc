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
      tp_(4),
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
    asio::post(tp_, [this, fd]() { HandleClient(fd); });
  }
}

void Replicant::HandleClient(int fd) {
  for (;;) {
    auto cmd = ReadCommand(fd);
    if (cmd)
      asio::post(tp_, [this, fd, cmd = std::move(*cmd)]() { HandleCommand(fd, cmd); });
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

std::string Replicant::ReadLine(int fd) {
  std::string line;
  for (;;) {
    char c;
    ssize_t n = read(fd, &c, 1);
    if (n == 1) {
      if (c == '\n')
        break;
      line.push_back(c);
    } else if (n == 0 || (n == -1 && errno != EINTR)) {
      break;
    }
  }
  return line;
}

void Replicant::HandleCommand(int fd, Command cmd) {
  bool is_get = cmd.type == CommandType::kGet;
  auto r = consensus_->AgreeAndExecute(std::move(cmd));

  static const std::string success = "success\n";
  static const std::string failure = "failure\n";

  ssize_t n;
  if (r.ok) {
    n = write(fd, success.c_str(), success.size());
    assert(n == static_cast<ssize_t>(success.size()));
    if (is_get) {
      r.value.push_back('\n');
      n = write(fd, r.value.c_str(), r.value.size());
      assert(n == static_cast<ssize_t>(r.value.size()));
    }
  } else {
    n = write(fd, failure.c_str(), failure.size());
    assert(n == static_cast<ssize_t>(failure.size()));
  }
}
