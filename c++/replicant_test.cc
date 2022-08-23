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

 protected:
  std::vector<asio::io_context> io_contexts_;
  std::vector<json> configs_;
  std::vector<std::shared_ptr<Replicant>> replicants_;
};

TEST_F(ReplicantTest, Constructor) {
  std::thread t0([this] { replicants_[0]->Start(); });
  std::thread t1([this] { replicants_[1]->Start(); });
  std::thread t2([this] { replicants_[2]->Start(); });
  std::this_thread::sleep_for(seconds(2));
  replicants_[0]->Stop();
  replicants_[1]->Stop();
  replicants_[2]->Stop();
  t0.join();
  t1.join();
  t2.join();
}
