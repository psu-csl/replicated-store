#include "paxosrpc.h"

Status PaxosRPCServiceImpl::Heartbeat(ServerContext*,
                                      const HeartbeatRequest*,
                                      HeartbeatReply*) {
  return Status::OK;
}
