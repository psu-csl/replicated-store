#include <iostream>
#include <memory>
#include <cassert>

#include "kvstore.h"
#include "memstore.h"
#include "consensus.h"
#include "command.h"
#include "paxos.h"

int main() {
  std::unique_ptr<Consensus> paxos(new Paxos(new MemStore()));

  auto r = paxos->AgreeAndExecute(Command{CommandType::kGet, "foo", ""});
  assert(r);
}
