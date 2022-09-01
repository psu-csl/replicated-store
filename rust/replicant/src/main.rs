mod kvstore;

use kvstore::memkvstore::MemKVStore;
use kvstore::Command;
use kvstore::KVStore;

fn main() {
    let mut store = MemKVStore::new();
    let command = Command::Put(String::from("foo"), String::from("bar"));
    command.execute(&mut store);
    command.execute(&mut store);
}
