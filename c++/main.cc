#include <gflags/gflags.h>
#include <glog/logging.h>
#include <cassert>
#include <fstream>
#include <iostream>
#include <memory>

#include "json.h"
#include "replicant.h"

using nlohmann::json;

DEFINE_uint32(me, 0, "my index in the peer list in the configuration file");
DEFINE_string(config, "config.json", "path to the configuration file");

int main(int argc, char* argv[]) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_logtostderr = 1;

  std::ifstream f(FLAGS_config);
  LOG_IF(FATAL, !f) << "failed to open " << FLAGS_config;

  json config;
  f >> config;

  LOG_IF(FATAL, FLAGS_me >= config["peers"].size()) << FLAGS_me << config;
  config["me"] = FLAGS_me;

  Replicant replicant(config);
  replicant.Run();
}
