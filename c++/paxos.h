#ifndef PAXOS_H_
#define PAXOS_H_

#include <memory>

#include "kvstore.h"
#include "consensus.h"

class Paxos : public Consensus {
public:
  Paxos(KVStore* store) : store_(store) {}
  ~Paxos() = default;
  bool AgreeAndExecute(const Command& command) override;
private:
  std::unique_ptr<KVStore> store_;
};

#endif
