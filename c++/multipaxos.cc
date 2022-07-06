#include "multipaxos.h"
#include "json.h"

#include <chrono>
#include <thread>

using nlohmann::json;

using grpc::ClientContext;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using multipaxos::MultiPaxosRPC;
using multipaxos::ResponseType::OK;
using multipaxos::ResponseType::REJECT;

using multipaxos::HeartbeatRequest;
using multipaxos::HeartbeatResponse;
using multipaxos::PrepareRequest;
using multipaxos::PrepareResponse;

MultiPaxos::MultiPaxos(Log* log, json const& config)
    : running_(false),
      id_(config["id"]),
      port_(config["peers"][id_]),
      ballot_(kMaxNumPeers),
      heartbeat_pause_(config["heartbeat_pause"]),
      log_(log),
      tp_(config["threadpool_size"]) {
  for (std::string const peer : config["peers"])
    rpc_peers_.emplace_back(MultiPaxosRPC::NewStub(
        grpc::CreateChannel(peer, grpc::InsecureChannelCredentials())));

  ServerBuilder builder;
  builder.AddListeningPort(port_, grpc::InsecureServerCredentials());
  builder.RegisterService(this);
  rpc_server_ = builder.BuildAndStart();
}

void MultiPaxos::Start() {
  CHECK(!running_);
  running_ = true;
  heartbeat_thread_ = std::thread(&MultiPaxos::HeartbeatThread, this);
  CHECK(rpc_server_);
  DLOG(INFO) << id_ << " starting rpc server at " << port_;
  rpc_server_->Wait();
}

void MultiPaxos::Shutdown() {
  CHECK(running_);
  running_ = false;
  cv_leader_.notify_all();
  heartbeat_thread_.join();
  CHECK(rpc_server_);
  DLOG(INFO) << id_ << " stopping rpc server at " << port_;
  rpc_server_->Shutdown();
}

void MultiPaxos::HeartbeatThread() {
  DLOG(INFO) << id_ << " starting heartbeat thread";
  while (running_) {
    WaitUntilLeader();
    auto global_last_executed = log_->GlobalLastExecuted();
    while (running_) {
      heartbeat_num_responses_ = 0;
      heartbeat_ok_responses_.clear();
      {
        std::scoped_lock lock(mu_);
        heartbeat_request_.set_ballot(ballot_);
      }
      heartbeat_request_.set_last_executed(log_->LastExecuted());
      heartbeat_request_.set_global_last_executed(global_last_executed);
      for (auto& peer : rpc_peers_) {
        asio::post(tp_, [this, &peer] {
          ClientContext context;
          HeartbeatResponse response;
          Status status =
              peer->Heartbeat(&context, heartbeat_request_, &response);
          DLOG(INFO) << id_ << " sent heartbeat to " << context.peer();
          {
            std::scoped_lock lock(heartbeat_mu_);
            ++heartbeat_num_responses_;
            if (status.ok())
              heartbeat_ok_responses_.push_back(response.last_executed());
          }
          heartbeat_cv_.notify_one();
        });
      }
      {
        std::unique_lock lock(heartbeat_mu_);
        while (IsLeader() && heartbeat_num_responses_ != rpc_peers_.size())
          heartbeat_cv_.wait(lock);
        if (heartbeat_ok_responses_.size() == rpc_peers_.size())
          global_last_executed =
              *min_element(std::begin(heartbeat_ok_responses_),
                           std::end(heartbeat_ok_responses_));
      }
      std::this_thread::sleep_for(heartbeat_pause_);
      if (!IsLeader())
        break;
    }
  }
  DLOG(INFO) << id_ << " stopping heartbeat thread";
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

Status MultiPaxos::Prepare(ServerContext* context,
                           const PrepareRequest* request,
                           PrepareResponse* response) {
  DLOG(INFO) << id_ << " received prepare rpc from " << context->peer();
  std::scoped_lock lock(mu_);
  if (request->ballot() >= ballot_) {
    ballot_ = request->ballot();
    response->set_type(OK);
    for (auto& i : log_->InstancesForPrepare())
      *response->add_instances() = std::move(i);
  } else {
    response->set_type(REJECT);
  }
  return Status::OK;
}
