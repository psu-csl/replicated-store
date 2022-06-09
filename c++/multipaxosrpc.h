#ifndef MULTI_PAXOS_RPC_H_
#define MULTI_PAXOS_RPC_H_

#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <memory>
#include "multipaxosrpc.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ServerContext;
using grpc::Status;

using multipaxosrpc::HeartbeatRequest;
using multipaxosrpc::HeartbeatResponse;
using multipaxosrpc::MultiPaxosRPC;

class MultiPaxos;

class MultiPaxosRPCServiceImpl : public MultiPaxosRPC::Service {
 public:
  explicit MultiPaxosRPCServiceImpl(MultiPaxos* mp) : mp_(mp) {}

 private:
  Status Heartbeat(ServerContext*,
                   const HeartbeatRequest*,
                   HeartbeatResponse*) override;
  MultiPaxos* mp_;
};

class MultiPaxosRPCClient {
 public:
  MultiPaxosRPCClient(MultiPaxos* mp, std::shared_ptr<Channel> channel)
      : mp_(mp), stub_(MultiPaxosRPC::NewStub(channel)) {}

  int Heartbeat(int64_t global_last_executed);

 private:
  MultiPaxos* mp_;
  std::unique_ptr<MultiPaxosRPC::Stub> stub_;
};

#endif
