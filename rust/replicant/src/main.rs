mod kvstore;
mod log;

use kvstore::memkvstore::MemKVStore;
use kvstore::Command;
use kvstore::KVStore;
use log::Log;

fn main() {
    let store = Box::new(MemKVStore::new());
    let log = Log::new(store);
}
