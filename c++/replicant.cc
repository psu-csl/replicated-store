#include "replicant.h"

Replicant::Replicant() : peer_(new Paxos(new MemStore())) {}

void Replicant::Run() {
}
