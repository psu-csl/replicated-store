mod kvstore;
mod consensus;
mod replicant;

use kvstore::traits::KVStore;
use kvstore::memstore::MemStore;
use consensus::traits::Consensus;
use consensus::dummy_paxos::DummyPaxos;
use replicant::Replicant;

fn main() {
    let store = Box::new(MemStore::new());
    let paxos = Box::new(DummyPaxos::new(store));
    let replicant = Replicant::new(paxos);
}
