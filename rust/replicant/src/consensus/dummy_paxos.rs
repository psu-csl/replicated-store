use crate::kvstore::traits::KVStore;
use crate::kvstore::memstore::MemStore;
use crate::kvstore::command::Command;
use crate::consensus::traits::Consensus;

pub struct DummyPaxos<T: KVStore> {
    store: T
}

impl<T: KVStore> Consensus<T> for DummyPaxos<T> {
    fn new(store: T) -> Self {
        DummyPaxos { store }
    }

    fn agree_and_execute(&mut self, cmd: Command) -> String {
        match cmd {
            Command::Get(key) =>
                match self.store.get(&key) {
                    Some(value) => value.clone(),
                    None => "not found".to_string()
                }
            Command::Put(key, value) =>
                match self.store.put(key, value) {
                    Ok(()) => "ok".to_string(),
                    Err(message) => message.to_string()
                }
            Command::Del(key) =>
                match self.store.del(&key) {
                    Ok(()) => "ok".to_string(),
                    Err(message) => message.to_string()
                }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_put_del() {
        let mut paxos = DummyPaxos::new(MemStore::new());

        // get and del non-existent
        let cmd = Command::Get("foo".to_string());
        assert_eq!(paxos.agree_and_execute(cmd), "not found".to_string());
        let cmd = Command::Del("foo".to_string());
        assert_eq!(paxos.agree_and_execute(cmd), "no such value".to_string());

        // put followed by get
        let cmd = Command::Put("foo".to_string(), "bar".to_string());
        assert_eq!(paxos.agree_and_execute(cmd), "ok".to_string());
        let cmd = Command::Get("foo".to_string());
        assert_eq!(paxos.agree_and_execute(cmd), "bar".to_string());

        // update followed by get
        let cmd = Command::Put("foo".to_string(), "baz".to_string());
        assert_eq!(paxos.agree_and_execute(cmd), "ok".to_string());
        let cmd = Command::Get("foo".to_string());
        assert_eq!(paxos.agree_and_execute(cmd), "baz".to_string());

        // del followed by get
        let cmd = Command::Del("foo".to_string());
        assert_eq!(paxos.agree_and_execute(cmd), "ok".to_string());
        let cmd = Command::Get("foo".to_string());
        assert_eq!(paxos.agree_and_execute(cmd), "not found".to_string());
    }
}
