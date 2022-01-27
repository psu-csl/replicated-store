#include "paxosrpc.h"
#include "paxos.h"

Status PaxosRPCServiceImpl::Heartbeat(ServerContext*,
                                      const HeartbeatRequest* request,
                                      HeartbeatReply* reply) {
  paxos_->set_min_last_executed(request->min_last_executed());
  reply->set_last_executed(paxos_->last_executed());
  return Status::OK;
}
