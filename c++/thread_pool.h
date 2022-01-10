#ifndef THREAD_POOL_H_
#define THREAD_POOL_H_

#include <atomic>
#include <thread>
#include <vector>
#include <functional>

#include "joiner.h"
#include "tqueue.h"

class thread_pool {
 private:
  std::atomic_bool done_;
  tqueue<std::function<void()>> wq_;
  std::vector<std::thread> threads_;
  joiner joiner_;

  void worker() {
    while (!done_) {
      std::function<void()> task;
      if (wq_.try_pop(task))
        task();
      else
        std::this_thread::yield();
    }
  }

 public:
  thread_pool() : done_(false), joiner_(threads_) {
    auto num_threads = std::thread::hardware_concurrency();
    try {
      for (size_t i = 0; i < num_threads; ++i)
        threads_.push_back(std::thread(&thread_pool::worker, this));
    } catch (...) {
      done_ = true;
      throw;
    }
  }

  ~thread_pool() {
    done_ = true;
  }

  template<typename Function>
  void submit(Function f) {
    wq_.push(std::function<void()>(f));
  }
};

#endif
