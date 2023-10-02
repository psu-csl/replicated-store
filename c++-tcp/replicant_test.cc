#include <gtest/gtest.h>
#include <chrono>
#include <memory>
#include <thread>

#include "json.h"
#include "replicant.h"
#include "test_util.h"

using namespace std::chrono;

using nlohmann::json;

static const int kNumPeers = 3;

class ReplicantTest : public testing::Test {
 public:
  ReplicantTest() : io_contexts_(kNumPeers) {
    for (auto id = 0; id < kNumPeers; ++id) {
      auto config = json::parse(MakeConfig(id, kNumPeers));
      auto replicant = std::make_shared<Replicant>(&io_contexts_[id], config);
      configs_.push_back(std::move(config));
      replicants_.push_back(std::move(replicant));
    }
  }

  void StartServer() {
    for (auto id = 0; id < kNumPeers; ++id) {
      threads_.push_back(std::thread([this, id] {
        io_contexts_[id].run();
      }));
    }
  }

  void StopServer() {
    for (auto id = 0; id < kNumPeers; ++id) {
      io_contexts_[id].stop();
      threads_[id].join();
    }
  }

 protected:
  std::vector<asio::io_context> io_contexts_;
  std::vector<json> configs_;
  std::vector<std::shared_ptr<Replicant>> replicants_;
  std::vector<std::thread> threads_;
};

TEST_F(ReplicantTest, Constructor) {
  std::thread t0([this] { replicants_[0]->Start(); });
  std::thread t1([this] { replicants_[1]->Start(); });
  std::thread t2([this] { replicants_[2]->Start(); });
  std::this_thread::sleep_for(milliseconds(500));
  StartServer();
  std::this_thread::sleep_for(seconds(2));

  std::thread t4([this] { replicants_[0]->Stop(); });
  std::thread t5([this] { replicants_[1]->Stop(); });
  std::thread t6([this] { replicants_[2]->Stop(); });
  std::this_thread::sleep_for(milliseconds(500));
  StopServer();

  t0.join();
  t1.join();
  t2.join();
  t4.join();
  t5.join();
  t6.join();
}
