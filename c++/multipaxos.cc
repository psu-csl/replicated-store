#include "json.h"

#include "multipaxos.h"

MultiPaxos::MultiPaxos(Log* /* log */, json const& config)
    : id_(config["id"]), ballot_(kMaxNumPeers) {}
