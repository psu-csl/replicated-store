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

    fn execute(
        &mut self,
        store: &mut Box<dyn KVStore + Sync + Send>,
    ) -> Result<Option<String>, &'static str> {
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
    kv_store: Box<dyn KVStore + Sync + Send>,
}

impl MapLog {
    fn new(kv_store: Box<dyn KVStore + Sync + Send>) -> Self {
        MapLog {
            map: HashMap::new(),
            running: true,
            last_index: 0,
            last_executed: 0,
            global_last_executed: 0,
            kv_store: kv_store,
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

    fn execute(&mut self) -> LogResult {
        self.last_executed += 1;
        let it = self.map.get_mut(&self.last_executed);
        assert!(it.is_some());
        let instance = it.unwrap();
        (instance.client_id, instance.execute(&mut self.kv_store))
    }
}

pub struct Log {
    log: Mutex<MapLog>,
    cv_executable: Condvar,
    cv_committable: Condvar,
}

impl Log {
    pub fn new(kv_store: Box<dyn KVStore + Sync + Send>) -> Self {
        Log {
            log: Mutex::new(MapLog::new(kv_store)),
            cv_executable: Condvar::new(),
            cv_committable: Condvar::new(),
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

    fn execute(&self) -> Option<LogResult> {
        let mut log = self.log.lock().unwrap();
        while log.running && !log.is_executable() {
            log = self.cv_executable.wait(log).unwrap();
        }
        if !log.running {
            return None;
        }
        Some(log.execute())
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
        let store = Box::new(MemKVStore::new());
        let mut log = MapLog::new(store);

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
        let store = Box::new(MemKVStore::new());
        let mut log = MapLog::new(store);

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
        let store = Box::new(MemKVStore::new());
        let mut log = MapLog::new(store);

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
        let store = Box::new(MemKVStore::new());
        let mut log = MapLog::new(store);

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
        let store = Box::new(MemKVStore::new());
        let mut log = MapLog::new(store);

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
        let store = Box::new(MemKVStore::new());
        let mut log = MapLog::new(store);

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
        let store = Box::new(MemKVStore::new());
        let mut log = MapLog::new(store);

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

        let commit_thread = {
            let log = Arc::clone(&log);
            thread::spawn(move || {
                log.commit(index);
            })
        };
        thread::yield_now();

        log.append(instance);
        commit_thread.join().unwrap();
        assert!(log.at(index).unwrap().is_committed());
    }

    #[test]
    fn append_commit_execute() {
        let store = Box::new(MemKVStore::new());
        let log = Arc::new(Log::new(store));

        let get = Command::Get(String::from(""));
        let ballot = 0;
        let index = log.advance_last_index();
        let instance = Instance::new(ballot, index, State::InProgress, get);

        let execute_thread = {
            let log = Arc::clone(&log);
            thread::spawn(move || {
                log.execute();
            })
        };

        log.append(instance);
        log.commit(index);
        execute_thread.join().unwrap();

        assert!(log.at(index).unwrap().is_executed());
        assert_eq!(index, log.last_executed());
    }

    #[test]
    fn append_commit_execute_out_of_order() {
        let store = Box::new(MemKVStore::new());
        let log = Arc::new(Log::new(store));

        let get = Command::Get(String::from(""));
        let ballot = 0;
        let index1 = 1;
        let instance1 = Instance::new(ballot, index1, State::InProgress, get.clone());
        let index2 = 2;
        let instance2 = Instance::new(ballot, index2, State::InProgress, get.clone());
        let index3 = 3;
        let instance3 = Instance::new(ballot, index3, State::InProgress, get.clone());

        let execute_thread = {
            let log = Arc::clone(&log);
            thread::spawn(move || {
                log.execute();
                log.execute();
                log.execute();
            })
        };

        log.append(instance1);
        log.append(instance2);
        log.append(instance3);

        log.commit(index3);
        log.commit(index2);
        log.commit(index1);

        execute_thread.join().unwrap();

        assert!(log.at(index1).unwrap().is_executed());
        assert!(log.at(index2).unwrap().is_executed());
        assert!(log.at(index3).unwrap().is_executed());
        assert_eq!(index3, log.last_executed());
    }

    #[test]
    fn commit_until() {
        let store = Box::new(MemKVStore::new());
        let log = Arc::new(Log::new(store));

        let get = Command::Get(String::from(""));
        let ballot = 0;
        let index1 = 1;
        let instance1 = Instance::new(ballot, index1, State::InProgress, get.clone());
        let index2 = 2;
        let instance2 = Instance::new(ballot, index2, State::InProgress, get.clone());
        let index3 = 3;
        let instance3 = Instance::new(ballot, index3, State::InProgress, get.clone());

        log.append(instance1);
        log.append(instance2);
        log.append(instance3);
        log.commit_until(index2, ballot);

        assert!(log.at(index1).unwrap().is_committed());
        assert!(log.at(index2).unwrap().is_committed());
        assert!(!log.at(index3).unwrap().is_committed());
        assert!(log.is_executable());
    }

    #[test]
    fn commit_until_higher_ballot() {
        let store = Box::new(MemKVStore::new());
        let log = Arc::new(Log::new(store));

        let get = Command::Get(String::from(""));
        let ballot = 0;
        let index1 = 1;
        let instance1 = Instance::new(ballot, index1, State::InProgress, get.clone());
        let index2 = 2;
        let instance2 = Instance::new(ballot, index2, State::InProgress, get.clone());
        let index3 = 3;
        let instance3 = Instance::new(ballot, index3, State::InProgress, get.clone());

        log.append(instance1);
        log.append(instance2);
        log.append(instance3);
        log.commit_until(index3, ballot + 1);

        assert!(!log.at(index1).unwrap().is_committed());
        assert!(!log.at(index2).unwrap().is_committed());
        assert!(!log.at(index3).unwrap().is_committed());
        assert!(!log.is_executable());
    }

    #[test]
    #[should_panic(expected = "commit_until case 2")]
    fn commit_until_case2() {
        let store = Box::new(MemKVStore::new());
        let log = Arc::new(Log::new(store));

        let get = Command::Get(String::from(""));
        let ballot = 5;
        let index1 = 1;
        let instance1 = Instance::new(ballot, index1, State::InProgress, get.clone());
        let index2 = 2;
        let instance2 = Instance::new(ballot, index2, State::InProgress, get.clone());
        let index3 = 3;
        let instance3 = Instance::new(ballot, index3, State::InProgress, get.clone());

        log.append(instance1);
        log.append(instance2);
        log.append(instance3);
        log.commit_until(index3, ballot - 1);
    }

    #[test]
    fn commit_until_with_gap() {
        let store = Box::new(MemKVStore::new());
        let log = Arc::new(Log::new(store));

        let get = Command::Get(String::from(""));
        let ballot = 0;
        let index1 = 1;
        let instance1 = Instance::new(ballot, index1, State::InProgress, get.clone());
        let index3 = 3;
        let instance3 = Instance::new(ballot, index3, State::InProgress, get.clone());
        let index4 = 4;
        let instance4 = Instance::new(ballot, index4, State::InProgress, get.clone());

        log.append(instance1);
        log.append(instance3);
        log.append(instance4);
        log.commit_until(index4, ballot);

        assert!(log.at(index1).unwrap().is_committed());
        assert!(!log.at(index3).unwrap().is_committed());
        assert!(!log.at(index4).unwrap().is_committed());
    }

    #[test]
    fn append_commit_until_execute() {
        let store = Box::new(MemKVStore::new());
        let log = Arc::new(Log::new(store));

        let get = Command::Get(String::from(""));
        let ballot = 0;
        let index1 = 1;
        let instance1 = Instance::new(ballot, index1, State::InProgress, get.clone());
        let index2 = 2;
        let instance2 = Instance::new(ballot, index2, State::InProgress, get.clone());
        let index3 = 3;
        let instance3 = Instance::new(ballot, index3, State::InProgress, get.clone());

        let execute_thread = {
            let log = Arc::clone(&log);
            thread::spawn(move || {
                log.execute();
                log.execute();
                log.execute();
            })
        };

        log.append(instance1);
        log.append(instance2);
        log.append(instance3);

        log.commit_until(index3, ballot);

        execute_thread.join().unwrap();

        assert!(log.at(index1).unwrap().is_executed());
        assert!(log.at(index2).unwrap().is_executed());
        assert!(log.at(index3).unwrap().is_executed());
        assert!(!log.is_executable());
    }

    #[test]
    fn append_commit_until_execute_trim_until() {
        let store = Box::new(MemKVStore::new());
        let log = Arc::new(Log::new(store));

        let get = Command::Get(String::from(""));
        let ballot = 0;
        let index1 = 1;
        let instance1 = Instance::new(ballot, index1, State::InProgress, get.clone());
        let index2 = 2;
        let instance2 = Instance::new(ballot, index2, State::InProgress, get.clone());
        let index3 = 3;
        let instance3 = Instance::new(ballot, index3, State::InProgress, get.clone());

        let execute_thread = {
            let log = Arc::clone(&log);
            thread::spawn(move || {
                log.execute();
                log.execute();
                log.execute();
            })
        };

        log.append(instance1);
        log.append(instance2);
        log.append(instance3);

        log.commit_until(index3, ballot);
        execute_thread.join().unwrap();

        log.trim_until(index3);

        assert_eq!(None, log.at(index1));
        assert_eq!(None, log.at(index2));
        assert_eq!(None, log.at(index3));
        assert_eq!(index3, log.last_executed());
        assert_eq!(index3, log.global_last_executed());
        assert!(!log.is_executable());
    }

    #[test]
    fn append_at_trimmed_index() {
        let store = Box::new(MemKVStore::new());
        let log = Arc::new(Log::new(store));

        let get = Command::Get(String::from(""));
        let ballot = 0;
        let index1 = 1;
        let instance1 = Instance::new(ballot, index1, State::InProgress, get.clone());
        let index2 = 2;
        let instance2 = Instance::new(ballot, index2, State::InProgress, get.clone());

        let execute_thread = {
            let log = Arc::clone(&log);
            thread::spawn(move || {
                log.execute();
                log.execute();
            })
        };

        log.append(instance1.clone());
        log.append(instance2.clone());

        log.commit_until(index2, ballot);
        execute_thread.join().unwrap();

        log.trim_until(index2);

        assert_eq!(None, log.at(index1));
        assert_eq!(None, log.at(index2));
        assert_eq!(index2, log.last_executed());
        assert_eq!(index2, log.global_last_executed());
        assert!(!log.is_executable());

        log.append(instance1);
        log.append(instance2);

        assert_eq!(None, log.at(index1));
        assert_eq!(None, log.at(index2));
    }

    #[test]
    fn instances() {
        let store = Box::new(MemKVStore::new());
        let log = Arc::new(Log::new(store));

        let get = Command::Get(String::from(""));
        let ballot = 0;
        let index1 = 1;
        let instance1 = Instance::new(ballot, index1, State::InProgress, get.clone());
        let index2 = 2;
        let instance2 = Instance::new(ballot, index2, State::InProgress, get.clone());
        let index3 = 3;
        let instance3 = Instance::new(ballot, index3, State::InProgress, get.clone());

        let expected = vec![instance1.clone(), instance2.clone(), instance3.clone()];

        let execute_thread = {
            let log = Arc::clone(&log);
            thread::spawn(move || {
                log.execute();
                log.execute();
            })
        };

        log.append(instance1);
        log.append(instance2);
        log.append(instance3);

        assert_eq!(expected, log.instances());

        log.commit_until(index2, ballot);

        execute_thread.join().unwrap();

        log.trim_until(index2);

        assert_eq!(&expected[index2 as usize..], log.instances());
    }

    #[test]
    fn calling_stop_unblocks_executor() {
        let store = Box::new(MemKVStore::new());
        let log = Arc::new(Log::new(store));

        let execute_thread = {
            let log = Arc::clone(&log);
            thread::spawn(move || {
                let r = log.execute();
                assert_eq!(None, r);
            })
        };
        thread::yield_now();
        log.stop();
        execute_thread.join().unwrap();
    }
}
