#include "multipaxos.h"
#include "json.h"

MultiPaxos::MultiPaxos(Log* /* log */, json const& config)
    : id_(config["id"]), port_(config["peers"][id_]), ballot_(kMaxNumPeers) {}

void MultiPaxos::Start(void) {
  ServerBuilder builder;
  builder.AddListeningPort(port_, grpc::InsecureServerCredentials());
  builder.RegisterService(this);
  server_ = builder.BuildAndStart();
  CHECK(server_);
  DLOG(INFO) << "starting rpc server at " << port_;
  server_->Wait();
}

void MultiPaxos::Shutdown(void) {
  CHECK(server_);
  DLOG(INFO) << "stopping rpc server at " << port_;
  server_->Shutdown();
}

Status MultiPaxos::Heartbeat(ServerContext* context,
                             const HeartbeatRequest*,
                             HeartbeatResponse*) {
  DLOG(INFO) << id_ << " received heartbeat rpc from " << context->peer();
  return Status::OK;
}
