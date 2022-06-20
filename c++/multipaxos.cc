#include "multipaxos.h"
#include "json.h"

MultiPaxos::MultiPaxos(Log* log, json const& config)
    : id_(config["id"]),
      port_(config["peers"][id_]),
      ballot_(kMaxNumPeers),
      log_(log) {
  ServerBuilder builder;
  builder.AddListeningPort(port_, grpc::InsecureServerCredentials());
  builder.RegisterService(this);
  server_ = builder.BuildAndStart();
}

void MultiPaxos::Start(void) {
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
                             const HeartbeatRequest* request,
                             HeartbeatResponse* response) {
  DLOG(INFO) << id_ << " received heartbeat rpc from " << context->peer();
  bool stale_rpc = false;
  {
    std::scoped_lock lock(mu_);
    if (request->ballot() >= ballot_) {
      stale_rpc = true;
      last_heartbeat_ = std::chrono::steady_clock::now();
      ballot_ = request->ballot();
    }
  }
  if (!stale_rpc) {
    log_->CommitUntil(request->last_executed(), request->ballot());
    log_->TrimUntil(request->global_last_executed());
  }
  response->set_last_executed(log_->LastExecuted());
  return Status::OK;
}
