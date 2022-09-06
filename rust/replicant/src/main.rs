mod kvstore;
mod log;

use kvstore::memkvstore::MemKVStore;
use log::Log;

fn main() {
    let store = Box::new(MemKVStore::new());
    let _log = Log::new(store);
}
