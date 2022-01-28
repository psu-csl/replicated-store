#include "paxosrpc.h"
#include <glog/logging.h>
#include "paxos.h"

Status PaxosRPCServiceImpl::Heartbeat(ServerContext* context,
                                      const HeartbeatRequest* request,
                                      HeartbeatReply* reply) {
  LOG(INFO) << "received heartbeat RPC from " << context->peer();
  paxos_->set_min_last_executed(request->min_last_executed());
  reply->set_last_executed(paxos_->last_executed());
  return Status::OK;
}
