#include <gflags/gflags.h>
#include <glog/logging.h>
#include <fstream>
#include <iostream>
#include <memory>

#include "json.h"
#include "replicant.h"

using nlohmann::json;

DEFINE_uint32(id, 0, "my id in the peers array in configuration file");
DEFINE_string(config, "config.json", "path to the configuration file");

int main(int argc, char* argv[]) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_logtostderr = 1;

  std::ifstream f(FLAGS_config);
  CHECK(f);

  json config;
  f >> config;

  CHECK(FLAGS_id < config["peers"].size());

  config["id"] = FLAGS_id;

  asio::io_context io_context;
  auto replicant = std::make_shared<Replicant>(&io_context, config);

  asio::signal_set signals(io_context, SIGINT, SIGTERM);
  signals.async_wait([&](const asio::error_code&, int) { replicant->Stop(); });

  replicant->Start();
}
