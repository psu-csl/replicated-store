mod kvstore;

use kvstore::memkvstore::MemKVStore;
use kvstore::KVStore;

fn main() {
    let store = MemKVStore::new();

    store.get("foo");
}
