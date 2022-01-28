#include "paxosrpc.h"
#include <glog/logging.h>
#include "paxos.h"

Status PaxosRPCServiceImpl::Heartbeat(ServerContext* context,
                                      const HeartbeatRequest* request,
                                      HeartbeatReply* reply) {
  DLOG(INFO) << paxos_->id() << " received heartbeat rpc from "
             << context->peer();
  paxos_->set_min_last_executed(request->min_last_executed());
  reply->set_last_executed(paxos_->last_executed());
  return Status::OK;
}

int PaxosRPCClient::Heartbeat(int min_last_executed) {
  HeartbeatRequest request;
  request.set_min_last_executed(min_last_executed);
  HeartbeatReply reply;
  ClientContext context;

  DLOG(INFO) << paxos_->id() << " sent heartbeat rpc to " << context.peer();
  Status status = stub_->Heartbeat(&context, request, &reply);
  if (status.ok()) {
    DLOG(INFO) << "heartbeat rpc to " << context.peer() << " succeeded";
    return reply.last_executed();
  }
  DLOG(ERROR) << "heartbeat rpc to " << context.peer() << " failed";
  return -1;
};
