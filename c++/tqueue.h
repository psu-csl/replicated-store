#ifndef TQUEUE_H_
#define TQUEUE_H_

#include <queue>
#include <memory>
#include <mutex>
#include <condition_variable>

template<typename T>
class tqueue {
 private:
  mutable std::mutex m;
  std::queue<T> q;
  std::condition_variable c;

 public:
  tqueue() {}

  tqueue(const tqueue& rhs) {
    std::scoped_lock g(rhs.m);
    q = rhs.q;
  }

  tqueue& operator=(const tqueue&) = delete;

  void push(T new_value) {
    std::scoped_lock g(m);
    q.push(new_value);
    c.notify_one();
  }

  void wait_and_pop(T& value) {
    std::unique_lock l(m);
    c.wait(l, [this] { return !q.empty(); });
    value = q.front();
    q.pop();
  }

  std::shared_ptr<T> wait_and_pop() {
    std::unique_lock l(m);
    q.wait(l, [this] { return !q.empty(); });
    std::shared_ptr<T> r(std::make_shared<T>(q.front()));
    q.pop();
    return r;
  }

  bool try_pop(T& value) {
    std::scoped_lock l(m);
    if (q.empty())
      return false;
    value = q.front();
    q.pop();
    return true;
  }

  std::shared_ptr<T> try_pop() {
    std::scoped_lock l(m);
    if (q.empty())
      return std::shared_ptr<T>();
    std::shared_ptr r(std::make_shared<T>(q.front()));
    q.pop();
    return r;
  }

  bool empty() const {
    std::scoped_lock l(m);
    return q.empty();
  }
};

#endif
