# Replicant

Replicant is a MultiPaxos-based linearizable distributed key-value store that
uses MultiPaxos for state machine replication (SMR) implementation. Replicant
includes the following modules:

* MultiPaxos module: MultiPaxos implementation. It communicates with other
  MultiPaxos modules running on other Replicants to reach a consensus on decided
  values in the log.
* Log module: MultiPaxos Protocol Log. It is a thread-safe unbounded
  producer-consumer queue.
* KVStore module: key-value store wrapper. Currently, it supports two types of
  stores, in-memory hash table and RocksDB.

Replicant handles commands from clients and pass commands to the MultiPaxos
module You can find specific interface designs and pseudocode in `doc`.

We have six versions using different languages (C++, Java, Go, and Rust) and
network communication protocols (gRPC or TCP). All of them are independent and
follow the same architecture and interface design mentioned above.

You can find how to run C++ version in the
[README.MD](https://github.com/psu-csl/replicated-store/blob/main/c%2B%2B/README.MD)
under the c++ folder.