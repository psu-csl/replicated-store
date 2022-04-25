#ifndef LOG_H_
#define LOG_H_

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <unordered_map>

#include "instance.h"

class Log {
 public:
  Log() = default;
  Log(Log const& log) = delete;
  Log& operator=(Log const& log) = delete;
  Log(Log&& log) = delete;
  Log& operator=(Log&& log) = delete;

 private:
  std::unordered_map<int64_t, Instance> log_;
  int64_t last_index_ = 0;
  int64_t last_executed_index_ = 0;
  int64_t global_last_executed_index_ = 0;
  std::mutex mu_;
  std::condition_variable cv_;
};

#endif
