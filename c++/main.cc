#include <gflags/gflags.h>
#include <glog/logging.h>
#include <cassert>
#include <fstream>
#include <iostream>
#include <memory>

#include "json.h"
#include "replicant.h"

using nlohmann::json;

int main(int argc, char* argv[]) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  json j;
  std::ifstream f("config.json");
  assert(f);

  f >> j;

  Replicant replicant;
  replicant.Run();
}
