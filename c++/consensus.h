#ifndef CONSENSUS_H_
#define CONSENSUS_H_

#include "command.h"

class Consensus {
public:
  virtual ~Consensus() = default;
  virtual bool AgreeAndExecute(const Command& command) = 0;
};

#endif
