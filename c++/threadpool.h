// Based on the code in ``C++ Concurrency in Action'', 2nd edition.

#ifndef THREAD_POOL_H_
#define THREAD_POOL_H_

#include <atomic>
#include <thread>
#include <vector>
#include <functional>
#include <chrono>

#include "joiner.h"
#include "queue.h"

class ThreadPool {
 private:
  std::atomic_bool done_;
  Queue<std::function<void()>> wq_;
  std::vector<std::thread> threads_;
  Joiner joiner_;

  void Worker() {
    while (!done_) {
      std::function<void()> task;
      if (wq_.TryPop(task)) {
        task();
      } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        std::this_thread::yield();
      }
    }
  }

 public:
  ThreadPool() : done_(false), joiner_(threads_) {
    auto num_threads = std::thread::hardware_concurrency();
    try {
      for (size_t i = 0; i < num_threads; ++i)
        threads_.push_back(std::thread(&ThreadPool::Worker, this));
    } catch (...) {
      done_ = true;
      throw;
    }
  }

  ~ThreadPool() {
    done_ = true;
  }

  template<typename Function>
  void Submit(Function f) {
    wq_.Push(std::function<void()>(f));
  }
};

#endif
