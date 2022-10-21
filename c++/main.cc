#include <gflags/gflags.h>
#include <glog/logging.h>
#include <fstream>
#include <iostream>
#include <memory>

#include "json.h"
#include "replicant.h"

using nlohmann::json;

DEFINE_uint32(id, 0, "my id in the peers array in configuration file");
DEFINE_string(config_path, "config.json", "path to the configuration file");
DEFINE_uint32(num_threads, 4, "number of threads for running asio callbacks");

int main(int argc, char* argv[]) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_logtostderr = 1;

  std::ifstream f(FLAGS_config_path);
  CHECK(f);

  json config;
  f >> config;

  CHECK(FLAGS_id < config["peers"].size());
  config["id"] = FLAGS_id;

  CHECK(FLAGS_num_threads > 0);
  asio::io_context io_context(FLAGS_num_threads);

  auto replicant = std::make_shared<Replicant>(&io_context, config);

  asio::signal_set signals(io_context, SIGINT, SIGTERM);
  signals.async_wait([&](const asio::error_code&, int) {
    replicant->Stop();
    io_context.stop();
  });

  replicant->Start();

  std::vector<std::thread> threads;
  threads.reserve(--FLAGS_num_threads);
  while (FLAGS_num_threads--)
    threads.emplace_back([&io_context] { io_context.run(); });
  io_context.run();

  for (auto& t : threads)
    t.join();
}
