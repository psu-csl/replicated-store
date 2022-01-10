#ifndef JOINER_H_
#define JOINER_H_

#include <thread>
#include <vector>

class joiner {
  std::vector<std::thread>& threads_;
 public:
  explicit joiner(std::vector<std::thread>& threads)
      : threads_(threads) {}
  ~joiner() {
    for (size_t i = 0; i < threads_.size(); ++i)
      if (threads_[i].joinable())
        threads_[i].join();
  }
};

#endif
