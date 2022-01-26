#include <glog/logging.h>
#include <cassert>
#include <fstream>
#include <iostream>
#include <memory>

#include "json.h"
#include "replicant.h"

using nlohmann::json;

int main(int, char* argv[]) {
  google::InitGoogleLogging(argv[0]);

  json j;
  std::ifstream f("config.json");

  if (!f) {
    LOG(INFO) << "failed to open config.json";
    exit(EXIT_FAILURE);
  }

  f >> j;
  std::cout << j << std::endl;

  Replicant replicant;
  replicant.Run();
}
