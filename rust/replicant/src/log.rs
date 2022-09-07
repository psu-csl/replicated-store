use crate::kvstore::memkvstore::MemKVStore;
use crate::kvstore::Command;
use crate::kvstore::KVStore;
use std::cmp;
use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

#[derive(PartialEq, Debug, Copy, Clone)]
enum State {
    InProgress,
    Committed,
    Executed,
}

type LogResult = (i64, Result<Option<String>, &'static str>);

#[derive(PartialEq, Debug, Clone)]
struct Instance {
    ballot: i64,
    index: i64,
    state: State,
    command: Command,
    client_id: i64,
}

impl Instance {
    fn new(ballot: i64, index: i64, state: State, command: Command) -> Instance {
        let client_id = 0;
        Instance {
            index,
            ballot,
            state,
            command,
            client_id,
        }
    }

    fn is_in_progress(&self) -> bool {
        self.state == State::InProgress
    }

    fn is_committed(&self) -> bool {
        self.state == State::Committed
    }

    fn is_executed(&self) -> bool {
        self.state == State::Executed
    }

    fn commit(&mut self) {
        self.state = State::Committed;
    }

    fn execute(&mut self, store: &mut Box<dyn KVStore>) -> Result<Option<String>, &'static str> {
        self.state = State::Executed;
        self.command.execute(store)
    }
}

type VectorLog = Vec<Instance>;

struct MapLog {
    running: bool,
    map: HashMap<i64, Instance>,
    last_index: i64,
    last_executed: i64,
    global_last_executed: i64,
}

impl MapLog {
    fn new() -> Self {
        MapLog {
            map: HashMap::new(),
            running: true,
            last_index: 0,
            last_executed: 0,
            global_last_executed: 0,
        }
    }

    fn insert(&mut self, instance: Instance) -> bool {
        let it = self.map.get(&instance.index);
        if let None = it {
            self.map.insert(instance.index, instance);
            return true;
        }
        let it = it.unwrap();
        if it.is_committed() || it.is_executed() {
            assert_eq!(it.command, instance.command, "insert case 2");
            return false;
        }
        if instance.ballot > it.ballot {
            self.map.insert(instance.index, instance);
            return false;
        }
        if instance.ballot == it.ballot {
            assert_eq!(it.command, instance.command, "insert case 3");
        }
        false
    }

    fn is_executable(&self) -> bool {
        match self.map.get(&(self.last_executed + 1)) {
            Some(instance) => instance.is_committed(),
            None => false,
        }
    }

    fn execute(&mut self, store: &mut Box<dyn KVStore>) -> LogResult {
        self.last_executed += 1;
        let it = self.map.get_mut(&self.last_executed);
        assert!(it.is_some());
        let instance = it.unwrap();
        (instance.client_id, instance.execute(store))
    }
}

pub struct Log {
    log: Mutex<MapLog>,
    cv_executable: Condvar,
    cv_committable: Condvar,
    kv_store: Box<dyn KVStore>,
}

impl Log {
    pub fn new(kv_store: Box<dyn KVStore>) -> Self {
        Log {
            log: Mutex::new(MapLog::new()),
            cv_executable: Condvar::new(),
            cv_committable: Condvar::new(),
            kv_store: kv_store,
        }
    }

    fn last_executed(&self) -> i64 {
        let log = self.log.lock().unwrap();
        log.last_executed
    }

    fn global_last_executed(&self) -> i64 {
        let log = self.log.lock().unwrap();
        log.global_last_executed
    }

    fn advance_last_index(&self) -> i64 {
        let mut log = self.log.lock().unwrap();
        log.last_index += 1;
        log.last_index
    }

    fn stop(&self) {
        let mut log = self.log.lock().unwrap();
        log.running = false;
        self.cv_executable.notify_one();
    }

    fn append(&self, instance: Instance) {
        let mut log = self.log.lock().unwrap();
        let i = instance.index;
        if i <= log.global_last_executed {
            return;
        }
        if log.insert(instance) {
            log.last_index = cmp::max(log.last_index, i);
            self.cv_committable.notify_all();
        }
    }

    fn commit(&self, index: i64) {
        assert!(index > 0, "invalid index");
        let mut log = self.log.lock().unwrap();
        let mut it = log.map.get_mut(&index);
        while let None = it {
            log = self.cv_committable.wait(log).unwrap();
            it = log.map.get_mut(&index);
        }
        let instance = it.unwrap();
        if instance.is_in_progress() {
            instance.commit();
        }
        if log.is_executable() {
            self.cv_executable.notify_one();
        }
    }

    fn execute(&mut self) -> Option<LogResult> {
        let mut log = self.log.lock().unwrap();
        while log.running && !log.is_executable() {
            log = self.cv_executable.wait(log).unwrap();
        }
        if !log.running {
            return None;
        }
        Some(log.execute(&mut self.kv_store))
    }

    fn commit_until(&self, leader_last_executed: i64, ballot: i64) {
        assert!(leader_last_executed >= 0, "invalid leader_last_executed");
        assert!(ballot >= 0, "invalid ballot");

        let mut log = self.log.lock().unwrap();
        for i in log.last_executed + 1..=leader_last_executed {
            let it = log.map.get_mut(&i);
            if let None = it {
                break;
            }
            let instance = it.unwrap();
            assert!(ballot >= instance.ballot, "commit_until case 2");
            if instance.ballot == ballot {
                instance.commit();
            }
        }
        if log.is_executable() {
            self.cv_executable.notify_one();
        }
    }

    fn trim_until(&self, leader_global_last_executed: i64) {
        let mut log = &mut *self.log.lock().unwrap();
        while log.global_last_executed < leader_global_last_executed {
            log.global_last_executed += 1;
            let it = log.map.remove(&log.global_last_executed);
            assert!(it.unwrap().is_executed());
        }
    }

    fn instances(&self) -> VectorLog {
        let log = self.log.lock().unwrap();
        let mut instances: VectorLog = Vec::new();
        for i in log.global_last_executed + 1..=log.last_index {
            if let Some(instance) = log.map.get(&i) {
                instances.push(instance.clone());
            }
        }
        instances
    }

    fn is_executable(&self) -> bool {
        let log = self.log.lock().unwrap();
        log.is_executable()
    }

    fn at(&self, index: i64) -> Option<Instance> {
        let log = self.log.lock().unwrap();
        match log.map.get(&index) {
            Some(instance) => Some(instance.clone()),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructor() {
        let store = Box::new(MemKVStore::new());
        let log = Log::new(store);

        assert_eq!(0, log.last_executed());
        assert_eq!(0, log.global_last_executed());
        assert!(!log.is_executable());

        assert_eq!(None, log.at(0));
        assert_eq!(None, log.at(-1));
        assert_eq!(None, log.at(3));
    }

    #[test]
    fn insert() {
        let put = Command::Put(String::from(""), String::from(""));
        let index = 1;
        let ballot = 1;
        let instance1 = Instance::new(ballot, index, State::InProgress, put.clone());
        let instance2 = instance1.clone();
        let mut log = MapLog::new();

        assert!(log.insert(instance1));
        assert_eq!(put, log.map[&index].command);
        assert!(!log.insert(instance2));
    }

    #[test]
    fn insert_update_in_progress() {
        let put = Command::Put(String::from(""), String::from(""));
        let del = Command::Del(String::from(""));
        let index = 1;
        let ballot = 1;
        let instance1 = Instance::new(ballot, index, State::InProgress, put.clone());
        let instance2 = Instance::new(ballot + 1, index, State::InProgress, del.clone());
        let mut log = MapLog::new();

        assert!(log.insert(instance1));
        assert_eq!(put, log.map[&index].command);
        assert!(!log.insert(instance2));
        assert_eq!(del, log.map[&index].command);
    }

    #[test]
    fn insert_update_commited() {
        let put = Command::Put(String::from(""), String::from(""));
        let index = 1;
        let ballot = 1;
        let instance1 = Instance::new(ballot, index, State::Committed, put.clone());
        let instance2 = Instance::new(ballot + 1, index, State::InProgress, put);
        let mut log = MapLog::new();

        assert!(log.insert(instance1));
        assert!(!log.insert(instance2));
    }

    #[test]
    fn insert_stale() {
        let put = Command::Put(String::from(""), String::from(""));
        let del = Command::Del(String::from(""));
        let index = 1;
        let ballot = 1;
        let instance1 = Instance::new(ballot, index, State::InProgress, put.clone());
        let instance2 = Instance::new(ballot - 1, index, State::InProgress, del.clone());
        let mut log = MapLog::new();

        assert!(log.insert(instance1));
        assert_eq!(put, log.map[&index].command);
        assert!(!log.insert(instance2));
        assert_eq!(put, log.map[&index].command);
    }

    #[test]
    #[should_panic(expected = "insert case 2")]
    fn insert_case2_committed() {
        let put = Command::Put(String::from(""), String::from(""));
        let del = Command::Del(String::from(""));
        let index = 1;
        let ballot = 0;
        let instance1 = Instance::new(ballot, index, State::Committed, put);
        let instance2 = Instance::new(ballot, index, State::InProgress, del);
        let mut log = MapLog::new();

        assert!(log.insert(instance1));
        log.insert(instance2);
    }

    #[test]
    #[should_panic(expected = "insert case 2")]
    fn insert_case2_executed() {
        let put = Command::Put(String::from(""), String::from(""));
        let del = Command::Del(String::from(""));
        let index = 1;
        let ballot = 0;
        let instance1 = Instance::new(ballot, index, State::Executed, put);
        let instance2 = Instance::new(ballot, index, State::InProgress, del);
        let mut log = MapLog::new();

        assert!(log.insert(instance1));
        log.insert(instance2);
    }

    #[test]
    #[should_panic(expected = "insert case 3")]
    fn insert_case3() {
        let put = Command::Put(String::from(""), String::from(""));
        let del = Command::Del(String::from(""));
        let index = 1;
        let ballot = 0;
        let instance1 = Instance::new(ballot, index, State::InProgress, put);
        let instance2 = Instance::new(ballot, index, State::InProgress, del);
        let mut log = MapLog::new();

        assert!(log.insert(instance1));
        log.insert(instance2);
    }

    #[test]
    fn append() {
        let store = Box::new(MemKVStore::new());
        let log = Log::new(store);

        let get = Command::Get(String::from(""));
        let ballot = 0;
        let instance1 = Instance::new(
            ballot,
            log.advance_last_index(),
            State::InProgress,
            get.clone(),
        );
        let instance2 = Instance::new(
            ballot,
            log.advance_last_index(),
            State::InProgress,
            get.clone(),
        );

        log.append(instance1);
        log.append(instance2);
        assert_eq!(1, log.at(1).unwrap().index);
        assert_eq!(2, log.at(2).unwrap().index);
    }

    #[test]
    fn append_with_gap() {
        let store = Box::new(MemKVStore::new());
        let log = Log::new(store);

        let get = Command::Get(String::from(""));
        let ballot = 0;
        let index = 42;
        let instance = Instance::new(ballot, index, State::InProgress, get.clone());

        log.append(instance);
        assert_eq!(index, log.at(index).unwrap().index);
        assert_eq!(index + 1, log.advance_last_index());
    }

    #[test]
    fn append_fill_gaps() {
        let store = Box::new(MemKVStore::new());
        let log = Log::new(store);

        let get = Command::Get(String::from(""));
        let ballot = 0;
        let index = 42;
        let instance1 = Instance::new(ballot, index, State::InProgress, get.clone());
        let instance2 = Instance::new(ballot, index - 10, State::InProgress, get.clone());

        log.append(instance1);
        log.append(instance2);
        assert_eq!(index + 1, log.advance_last_index());
    }

    #[test]
    fn append_high_ballot_override() {
        let store = Box::new(MemKVStore::new());
        let log = Log::new(store);

        let put = Command::Put(String::from(""), String::from(""));
        let del = Command::Del(String::from(""));
        let index = 1;
        let lo_ballot = 0;
        let hi_ballot = 1;

        let instance1 = Instance::new(lo_ballot, index, State::InProgress, put.clone());
        let instance2 = Instance::new(hi_ballot, index, State::InProgress, del.clone());

        log.append(instance1);
        log.append(instance2);
        assert_eq!(del, log.at(index).unwrap().command);
    }

    #[test]
    fn append_low_ballot_no_effect() {
        let store = Box::new(MemKVStore::new());
        let log = Log::new(store);

        let put = Command::Put(String::from(""), String::from(""));
        let del = Command::Del(String::from(""));
        let index = 1;
        let lo_ballot = 0;
        let hi_ballot = 1;

        let instance1 = Instance::new(hi_ballot, index, State::InProgress, put.clone());
        let instance2 = Instance::new(lo_ballot, index, State::InProgress, del.clone());

        log.append(instance1);
        log.append(instance2);
        assert_eq!(put, log.at(index).unwrap().command);
    }

    #[test]
    fn commit() {
        let store = Box::new(MemKVStore::new());
        let log = Log::new(store);

        let get = Command::Get(String::from(""));
        let ballot = 0;
        let index1 = 1;
        let index2 = 2;
        let instance1 = Instance::new(ballot, index1, State::InProgress, get.clone());
        let instance2 = Instance::new(ballot, index2, State::InProgress, get.clone());

        log.append(instance1);
        log.append(instance2);
        assert!(log.at(index1).unwrap().is_in_progress());
        assert!(log.at(index2).unwrap().is_in_progress());
        assert!(!log.is_executable());

        log.commit(index2);

        assert!(log.at(index1).unwrap().is_in_progress());
        assert!(log.at(index2).unwrap().is_committed());
        assert!(!log.is_executable());

        log.commit(index1);

        assert!(log.at(index1).unwrap().is_committed());
        assert!(log.at(index2).unwrap().is_committed());
        assert!(log.is_executable());
    }

    #[test]
    fn commit_before_append() {
        let store = Box::new(MemKVStore::new());
        let log = Arc::new(Log::new(store));

        let get = Command::Get(String::from(""));
        let ballot = 0;
        let index = log.advance_last_index();
        let instance = Instance::new(ballot, index, State::InProgress, get);

        let thread_log = Arc::clone(&log);
        let commit_thread = thread::spawn(move || {
            thread_log.commit(index);
        });

        log.append(instance);
        commit_thread.join().unwrap();
        assert!(log.at(index).unwrap().is_committed());
    }
}
