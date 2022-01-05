#ifndef REPLICANT_H_
#define REPLICANT_H_

#include "memstore.h"
#include "paxos.h"

class Replicant {
public:
  Replicant();
  void Run();
private:
  std::unique_ptr<Consensus> peer_;
};

#endif
