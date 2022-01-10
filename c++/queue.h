// Based on the code in ``C++ Concurrency in Action'', 2nd edition.

#ifndef QUEUE_H_
#define QUEUE_H_

#include <queue>
#include <memory>
#include <mutex>
#include <condition_variable>

template<typename T>
class Queue {
 private:
  mutable std::mutex m;
  std::queue<T> q;
  std::condition_variable c;

 public:
  Queue() {}

  Queue(const Queue& rhs) {
    std::scoped_lock g(rhs.m);
    q = rhs.q;
  }

  Queue& operator=(const Queue&) = delete;

  void Push(T new_value) {
    std::scoped_lock g(m);
    q.push(new_value);
    c.notify_one();
  }

  void WaitAndPop(T& value) {
    std::unique_lock l(m);
    c.wait(l, [this] { return !q.empty(); });
    value = q.front();
    q.pop();
  }

  std::shared_ptr<T> WaitAndPop() {
    std::unique_lock l(m);
    q.wait(l, [this] { return !q.empty(); });
    std::shared_ptr<T> r(std::make_shared<T>(q.front()));
    q.pop();
    return r;
  }

  bool TryPop(T& value) {
    std::scoped_lock l(m);
    if (q.empty())
      return false;
    value = q.front();
    q.pop();
    return true;
  }

  std::shared_ptr<T> TryPop() {
    std::scoped_lock l(m);
    if (q.empty())
      return std::shared_ptr<T>();
    std::shared_ptr r(std::make_shared<T>(q.front()));
    q.pop();
    return r;
  }

  bool Empty() const {
    std::scoped_lock l(m);
    return q.empty();
  }
};

#endif
