use crate::kvstore::traits::KVStore;

pub trait Consensus<T: KVStore> {
    fn new(store: T) -> Self;
}
