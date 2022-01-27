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
  PaxosRPCClient(std::shared_ptr<Channel> channel)
      : stub_(PaxosRPC::NewStub(channel)) {}

  int Heartbeat(int min_last_executed) {
    HeartbeatRequest request;
    request.set_min_last_executed(min_last_executed);
    HeartbeatReply reply;
    ClientContext context;

    Status status = stub_->Heartbeat(&context, request, &reply);
    if (status.ok()) {
      return reply.last_executed();
    }
    LOG(ERROR) << "Heartbeat RPC to " << context.peer() << " failed";
    return -1;
  };

 private:
  std::unique_ptr<PaxosRPC::Stub> stub_;
};

#endif
