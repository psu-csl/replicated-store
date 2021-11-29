mod kvstore;
mod consensus;

use kvstore::traits::KVStore;
use kvstore::memstore::MemStore;

fn main() {
    let _kv_store: MemStore = KVStore::new();
    println!("Hello, world!");
}
