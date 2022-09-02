use crate::kvstore::KVStore;
use std::cmp;
use std::collections::HashMap;
use std::sync::{Condvar, Mutex};

#[derive(PartialEq)]
enum State {
    InProgress,
    Committed,
    Executed,
}

struct Instance {
    index: i64,
    state: State,
}

impl Instance {
    fn is_in_progress(&self) -> bool {
        self.state == State::InProgress
    }

    fn set_state(&mut self, state: State) {
        self.state = state;
    }
}

type LogResult = (i64, Result<Option<String>, &'static str>);
struct LogMap(HashMap<i64, Instance>);

impl LogMap {
    fn insert(&mut self, instance: Instance) -> bool {
        true
    }

    fn is_executable(&self, index: i64) -> bool {
        self.0.get(&index).is_some()
    }
}

struct LogData {
    running: bool,
    log: LogMap,
    last_index: i64,
    last_executed: i64,
    global_last_executed: i64,
}

pub struct Log {
    kv_store: Box<dyn KVStore>,
    data: Mutex<LogData>,
    cv_executable: Condvar,
    cv_committable: Condvar,
}

impl Log {
    pub fn new(kv_store: Box<dyn KVStore>) -> Self {
        Log {
            kv_store: kv_store,
            data: Mutex::new(LogData {
                running: true,
                log: LogMap(HashMap::new()),
                last_index: 0,
                last_executed: 0,
                global_last_executed: 0,
            }),
            cv_executable: Condvar::new(),
            cv_committable: Condvar::new(),
        }
    }

    fn last_executed(&self) -> i64 {
        let guard = self.data.lock().unwrap();
        guard.last_executed
    }

    fn global_last_executed(&self) -> i64 {
        let guard = self.data.lock().unwrap();
        guard.global_last_executed
    }

    fn advance_last_index(&self) {
        let mut guard = self.data.lock().unwrap();
        guard.last_index += 1;
    }

    fn stop(&self) {
        let mut guard = self.data.lock().unwrap();
        guard.running = true;
        self.cv_executable.notify_one();
    }

    fn append(&self, instance: Instance) {
        let mut guard = self.data.lock().unwrap();
        let i = instance.index;
        if i <= guard.global_last_executed {
            return;
        }
        if guard.log.insert(instance) {
            guard.last_index = cmp::max(guard.last_index, i);
            self.cv_committable.notify_all();
        }
    }

    fn commit(&self, index: i64) {
        assert!(index > 0, "invalid index");
        let mut guard = self.data.lock().unwrap();
        let mut instance = guard.log.0.get_mut(&index);
        while let None = instance {
            guard = self.cv_committable.wait(guard).unwrap();
            instance = guard.log.0.get_mut(&index);
        }
        let mut instance = instance.unwrap();
        if instance.is_in_progress() {
            instance.set_state(State::Committed);
        }
        if guard.log.is_executable(guard.last_index + 1) {
            self.cv_executable.notify_one();
        }
    }
}
