#include "multipaxos.h"
#include "json.h"

MultiPaxos::MultiPaxos(Log* log, json const& config)
    : running_(false),
      id_(config["id"]),
      port_(config["peers"][id_]),
      ballot_(kMaxNumPeers),
      log_(log) {
  for (std::string const peer : config["peers"])
    rpc_peers_.emplace_back(MultiPaxosRPC::NewStub(
        grpc::CreateChannel(peer, grpc::InsecureChannelCredentials())));

  ServerBuilder builder;
  builder.AddListeningPort(port_, grpc::InsecureServerCredentials());
  builder.RegisterService(this);
  rpc_server_ = builder.BuildAndStart();
}

void MultiPaxos::Start(void) {
  CHECK(rpc_server_);
  DLOG(INFO) << "starting rpc server at " << port_;
  rpc_server_->Wait();
}

void MultiPaxos::Shutdown(void) {
  CHECK(rpc_server_);
  DLOG(INFO) << "stopping rpc server at " << port_;
  rpc_server_->Shutdown();
}

Status MultiPaxos::Heartbeat(ServerContext* context,
                             const HeartbeatRequest* request,
                             HeartbeatResponse* response) {
  DLOG(INFO) << id_ << " received heartbeat rpc from " << context->peer();
  std::scoped_lock lock(mu_);
  if (request->ballot() >= ballot_) {
    last_heartbeat_ = std::chrono::steady_clock::now();
    ballot_ = request->ballot();
    log_->CommitUntil(request->last_executed(), request->ballot());
    log_->TrimUntil(request->global_last_executed());
  }
  response->set_last_executed(log_->LastExecuted());
  return Status::OK;
}
