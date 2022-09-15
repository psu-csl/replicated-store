use ::log::debug;

mod kvstore;
mod log;
mod multipaxos;

use crate::log::Log;
use kvstore::memkvstore::MemKVStore;
use multipaxos::MultiPaxos;
use std::{thread, time};

fn main() {
    env_logger::init();

    let str = r#"{
      "id": 0,
      "threadpool_size": 8,
      "commit_interval": 3000,
      "peers": ["127.0.0.1:10000",
                "127.0.0.1:11000",
                "127.0.0.1:12000"]
     }"#;

    let config = serde_json::from_str(str).unwrap();

    let log = Log::new(Box::new(MemKVStore::new()));
    let mut mp = MultiPaxos::new(log, &config);
    debug!("starting...");
    mp.start();
    thread::sleep(time::Duration::from_millis(4000));
    mp.stop();
}
