#ifndef PAXOS_H_
#define PAXOS_H_

#include <condition_variable>
#include <memory>
#include <mutex>

#include "consensus.h"
#include "kvstore.h"

class Paxos : public Consensus {
 public:
  Paxos(KVStore* store);
  ~Paxos() = default;
  Result AgreeAndExecute(Command command) override;

 private:
  void HeartBeat(void);

  bool leader_;
  std::condition_variable cv_;
  std::mutex mu_;
  std::unique_ptr<KVStore> store_;
};

#endif
