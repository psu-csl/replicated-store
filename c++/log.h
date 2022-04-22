#ifndef LOG_H_
#define LOG_H_

#include <cstdint>
#include <unordered_map>

#include "instance.h"

class Log {
 private:
  std::unordered_map<int64_t, Instance> log_;
  int64_t last_inprogress_index_;
  int64_t last_executed_index_;
  int64_t global_last_executed_index_;


#endif
