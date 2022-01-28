#ifndef PAXOSRPC_H_
#define PAXOSRPC_H_

#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <memory>
#include "paxosrpc.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ServerContext;
using grpc::Status;

using paxosrpc::HeartbeatReply;
using paxosrpc::HeartbeatRequest;
using paxosrpc::PaxosRPC;

class Paxos;

class PaxosRPCServiceImpl : public PaxosRPC::Service {
 public:
  explicit PaxosRPCServiceImpl(Paxos* paxos) : paxos_(paxos) {}

 private:
  Status Heartbeat(ServerContext*,
                   const HeartbeatRequest*,
                   HeartbeatReply*) override;
  Paxos* paxos_;
};

class PaxosRPCClient {
 public:
  PaxosRPCClient(Paxos* paxos, std::shared_ptr<Channel> channel)
      : paxos_(paxos), stub_(PaxosRPC::NewStub(channel)) {}

  int Heartbeat(int min_last_executed);

 private:
  Paxos* paxos_;
  std::unique_ptr<PaxosRPC::Stub> stub_;
};

#endif
