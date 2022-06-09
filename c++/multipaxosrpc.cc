#include <glog/logging.h>

#include "multipaxos.h"
#include "multipaxosrpc.h"

Status MultiPaxosRPCServiceImpl::Heartbeat(ServerContext* context,
                                           const HeartbeatRequest*,
                                           HeartbeatResponse*) {
  DLOG(INFO) << mp_->id() << " received heartbeat rpc from " << context->peer();
  return Status::OK;
}

int MultiPaxosRPCClient::Heartbeat(int64_t) {
  ClientContext context;
  DLOG(INFO) << mp_->id() << " sent heartbeat rpc to " << context.peer();
  return 0;
};
