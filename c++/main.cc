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

  std::ifstream f(FLAGS_config);
  assert(f);

  json config;
  f >> config;

  assert(FLAGS_me < config["peers"].size());
  config["me"] = FLAGS_me;

  Replicant replicant(config);
  replicant.Run();
}
