#include "multipaxos.h"
#include "json.h"

MultiPaxos::MultiPaxos(Log* /* log */, json const& config)
    : id_(config["id"]), ballot_(kMaxNumPeers) {}

Status MultiPaxos::Heartbeat(ServerContext* context,
                             const HeartbeatRequest*,
                             HeartbeatResponse*) {
  DLOG(INFO) << id_ << " received heartbeat rpc from " << context->peer();
  return Status::OK;
}
