#include "multipaxos.h"
#include "json.h"

#include <chrono>
#include <thread>

using namespace std::chrono;

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
      ready_(false),
      ballot_(kMaxNumPeers),
      log_(log),
      id_(config["id"]),
      heartbeat_interval_(config["heartbeat_interval"]),
      engine_(id_),
      dist_(config["heartbeat_delta"],
            heartbeat_interval_ - static_cast<long>(config["heartbeat_delta"])),
      port_(config["peers"][id_]),
      last_heartbeat_(0),
      rpc_server_running_(false),
      thread_pool_(config["threadpool_size"]),
      heartbeat_num_rpcs_(0),
      prepare_num_rpcs_(0) {
  int64_t id = 0;
  for (std::string const peer : config["peers"])
    rpc_peers_.emplace_back(id++,
                            MultiPaxosRPC::NewStub(grpc::CreateChannel(
                                peer, grpc::InsecureChannelCredentials())));
}

void MultiPaxos::Start() {
  CHECK(!running_);
  running_ = true;
  heartbeat_thread_ = std::thread(&MultiPaxos::HeartbeatThread, this);
  prepare_thread_ = std::thread(&MultiPaxos::PrepareThread, this);

  DLOG(INFO) << id_ << " starting rpc server at " << port_;
  ServerBuilder builder;
  builder.AddListeningPort(port_, grpc::InsecureServerCredentials());
  builder.RegisterService(this);
  rpc_server_ = builder.BuildAndStart();
  {
    std::scoped_lock lock(mu_);
    rpc_server_running_ = true;
    rpc_server_running_cv_.notify_one();
  }
  rpc_server_->Wait();
}

void MultiPaxos::Stop() {
  CHECK(running_);
  running_ = false;

  cv_leader_.notify_one();
  heartbeat_thread_.join();

  cv_follower_.notify_one();
  prepare_thread_.join();

  thread_pool_.join();

  {
    std::unique_lock lock(mu_);
    while (!rpc_server_running_)
      rpc_server_running_cv_.wait(lock);
  }
  DLOG(INFO) << id_ << " stopping rpc server at " << port_;
  rpc_server_->Shutdown();
}

void MultiPaxos::HeartbeatThread() {
  DLOG(INFO) << id_ << " starting heartbeat thread";
  while (running_) {
    WaitUntilLeader();
    auto global_last_executed = log_->GlobalLastExecuted();
    while (running_ && IsLeader()) {
      heartbeat_num_rpcs_ = 0;
      heartbeat_responses_.clear();
      {
        std::scoped_lock lock(mu_);
        heartbeat_request_.set_ballot(ballot_);
      }
      heartbeat_request_.set_sender(id_);
      heartbeat_request_.set_last_executed(log_->LastExecuted());
      heartbeat_request_.set_global_last_executed(global_last_executed);
      for (auto& peer : rpc_peers_) {
        asio::post(thread_pool_, [this, &peer] {
          ClientContext context;
          HeartbeatResponse response;
          Status status =
              peer.stub_->Heartbeat(&context, heartbeat_request_, &response);
          DLOG(INFO) << id_ << " sent heartbeat to " << peer.id_;
          {
            std::scoped_lock lock(heartbeat_mu_);
            ++heartbeat_num_rpcs_;
            if (status.ok())
              heartbeat_responses_.push_back(response.last_executed());
          }
          heartbeat_cv_.notify_one();
        });
      }
      {
        std::unique_lock lock(heartbeat_mu_);
        while (IsLeader() && heartbeat_num_rpcs_ != rpc_peers_.size())
          heartbeat_cv_.wait(lock);
        if (heartbeat_responses_.size() == rpc_peers_.size())
          global_last_executed = *min_element(std::begin(heartbeat_responses_),
                                              std::end(heartbeat_responses_));
      }
      std::this_thread::sleep_for(milliseconds(heartbeat_interval_));
    }
  }
  DLOG(INFO) << id_ << " stopping heartbeat thread";
}

void MultiPaxos::PrepareThread() {
  DLOG(INFO) << id_ << " starting prepare thread";
  while (running_) {
    WaitUntilFollower();
    while (running_ && !IsLeader()) {
      auto sleep_time = heartbeat_interval_ + dist_(engine_);
      DLOG(INFO) << id_ << " prepare thread sleeping for " << sleep_time;
      std::this_thread::sleep_for(milliseconds(sleep_time));
      DLOG(INFO) << id_ << " prepare thread woke up";
      auto now = time_point_cast<milliseconds>(steady_clock::now())
                     .time_since_epoch()
                     .count();
      if (now - last_heartbeat_ < heartbeat_interval_)
        continue;

      prepare_num_rpcs_ = 0;
      prepare_ok_responses_.clear();
      prepare_request_.set_sender(id_);
      prepare_request_.set_ballot(NextBallot());
      for (auto& peer : rpc_peers_) {
        asio::post(thread_pool_, [this, &peer] {
          ClientContext context;
          PrepareResponse response;
          Status status =
              peer.stub_->Prepare(&context, prepare_request_, &response);
          DLOG(INFO) << id_ << " sent prepare request to " << peer.id_;
          {
            std::scoped_lock lock(prepare_mu_);
            ++prepare_num_rpcs_;
            if (status.ok()) {
              if (response.type() == OK) {
                log_vector_t log;
                for (int i = 0; i < response.instances_size(); ++i)
                  log.push_back(std::move(response.instances(i)));
                prepare_ok_responses_.push_back(std::move(log));
              } else {
                CHECK(response.type() == REJECT);
                std::scoped_lock lock(mu_);
                SetBallot(response.ballot());
              }
            }
          }
          prepare_cv_.notify_one();
        });
      }
      {
        std::unique_lock lock(prepare_mu_);
        while (IsLeader() &&
               prepare_ok_responses_.size() <= rpc_peers_.size() / 2 &&
               prepare_num_rpcs_ != rpc_peers_.size()) {
          prepare_cv_.wait(lock);
        }
        if (prepare_ok_responses_.size() <= rpc_peers_.size() / 2)
          continue;
      }
      {
        std::scoped_lock lock(mu_);
        if (IsLeaderLockless()) {
          ready_ = true;
          DLOG(INFO) << id_ << " leader and ready to serve";
        }
      }
    }
  }
  DLOG(INFO) << id_ << " stopping prepare thread";
}

Status MultiPaxos::Heartbeat(ServerContext*,
                             const HeartbeatRequest* request,
                             HeartbeatResponse* response) {
  DLOG(INFO) << id_ << " received heartbeat rpc from " << request->sender();
  std::scoped_lock lock(mu_);
  if (request->ballot() >= ballot_) {
    last_heartbeat_ = time_point_cast<milliseconds>(steady_clock::now())
                          .time_since_epoch()
                          .count();
    SetBallot(request->ballot());
    log_->CommitUntil(request->last_executed(), request->ballot());
    log_->TrimUntil(request->global_last_executed());
  }
  response->set_last_executed(log_->LastExecuted());
  return Status::OK;
}

Status MultiPaxos::Prepare(ServerContext*,
                           const PrepareRequest* request,
                           PrepareResponse* response) {
  DLOG(INFO) << id_ << " received prepare rpc from " << request->sender();
  std::scoped_lock lock(mu_);
  if (request->ballot() >= ballot_) {
    SetBallot(request->ballot());
    for (auto& i : log_->InstancesSinceGlobalLastExecuted())
      *response->add_instances() = std::move(i);
    response->set_type(OK);
  } else {
    response->set_ballot(ballot_);
    response->set_type(REJECT);
  }
  return Status::OK;
}
