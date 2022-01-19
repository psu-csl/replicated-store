#ifndef PAXOS_H_
#define PAXOS_H_

#include <memory>

#include "consensus.h"
#include "kvstore.h"

class Paxos : public Consensus {
 public:
  Paxos(KVStore* store) : store_(store) {}
  ~Paxos() = default;
  Result AgreeAndExecute(Command command) override;

 private:
  std::unique_ptr<KVStore> store_;
};

#endif
