use crate::kvstore::traits::KVStore;
use crate::kvstore::memstore::MemStore;
use crate::consensus::traits::Consensus;

pub struct DummyPaxos<T: KVStore> {
    store: T
}

impl<T: KVStore> Consensus<T> for DummyPaxos<T> {
    fn new(s: T) -> Self {
        DummyPaxos { store: s }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create() {
        let store: MemStore = KVStore::new();
        let con: DummyPaxos<MemStore> = Consensus::new(store);
    }
}
