use super::kvstore::memkvstore::MemKVStore;
use super::kvstore::{KVStore, KVStoreError};
use futures::future::join_all;
use parking_lot::Mutex;
use std::cmp;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Notify;

use super::multipaxos::rpc::Command;
use super::multipaxos::rpc::{Instance, InstanceState};

impl Instance {
    pub fn inprogress(
        ballot: i64,
        index: i64,
        command: &Command,
        client_id: i64,
    ) -> Self {
        Self::new(ballot, index, InstanceState::Inprogress, command, client_id)
    }

    pub fn inprogress_get(ballot: i64, index: i64) -> Self {
        Self::new(
            ballot,
            index,
            InstanceState::Inprogress,
            &Command::get(""),
            0,
        )
    }

    pub fn inprogress_put(ballot: i64, index: i64) -> Self {
        Self::new(
            ballot,
            index,
            InstanceState::Inprogress,
            &Command::put("", ""),
            0,
        )
    }

    pub fn inprogress_del(ballot: i64, index: i64) -> Self {
        Self::new(
            ballot,
            index,
            InstanceState::Inprogress,
            &Command::del(""),
            0,
        )
    }

    fn committed(
        ballot: i64,
        index: i64,
        command: &Command,
        client_id: i64,
    ) -> Self {
        Self::new(ballot, index, InstanceState::Committed, command, client_id)
    }

    pub fn committed_get(ballot: i64, index: i64) -> Self {
        Self::new(
            ballot,
            index,
            InstanceState::Committed,
            &Command::get(""),
            0,
        )
    }

    pub fn committed_put(ballot: i64, index: i64) -> Self {
        Self::new(
            ballot,
            index,
            InstanceState::Committed,
            &Command::put("", ""),
            0,
        )
    }

    pub fn committed_del(ballot: i64, index: i64) -> Self {
        Self::new(
            ballot,
            index,
            InstanceState::Committed,
            &Command::del(""),
            0,
        )
    }

    fn executed(
        ballot: i64,
        index: i64,
        command: &Command,
        client_id: i64,
    ) -> Self {
        Self::new(ballot, index, InstanceState::Executed, command, client_id)
    }

    pub fn executed_get(ballot: i64, index: i64) -> Self {
        Self::new(ballot, index, InstanceState::Executed, &Command::get(""), 0)
    }

    pub fn executed_del(ballot: i64, index: i64) -> Self {
        Self::new(ballot, index, InstanceState::Executed, &Command::del(""), 0)
    }

    fn new(
        ballot: i64,
        index: i64,
        state: InstanceState,
        command: &Command,
        client_id: i64,
    ) -> Self {
        Self {
            ballot,
            index,
            client_id,
            state: state as i32,
            command: Some(command.clone()),
        }
    }

    pub fn is_inprogress(&self) -> bool {
        self.state == InstanceState::Inprogress as i32
    }

    pub fn is_committed(&self) -> bool {
        self.state == InstanceState::Committed as i32
    }

    pub fn is_executed(&self) -> bool {
        self.state == InstanceState::Executed as i32
    }

    pub fn commit(&mut self) {
        self.state = InstanceState::Committed as i32;
    }

    fn execute(
        &mut self,
        store: &mut Box<dyn KVStore + Sync + Send>,
    ) -> Result<String, KVStoreError> {
        self.state = InstanceState::Executed as i32;
        self.command.as_ref().unwrap().execute(store)
    }
}

pub fn insert(log: &mut HashMap<i64, Instance>, instance: Instance) -> bool {
    let it = log.get(&instance.index);
    if it.is_none() {
        log.insert(instance.index, instance);
        return true;
    }
    let it = it.unwrap();
    if it.is_committed() || it.is_executed() {
        assert_eq!(it.command, instance.command, "insert case 2");
        return false;
    }
    if instance.ballot > it.ballot {
        log.insert(instance.index, instance);
        return false;
    }
    if instance.ballot == it.ballot {
        assert_eq!(it.command, instance.command, "insert case 3");
    }
    false
}

struct LogInner {
    running: bool,
    kv_store: Box<dyn KVStore + Sync + Send>,
    log: HashMap<i64, Instance>,
    last_index: i64,
    last_executed: i64,
    global_last_executed: i64,
}

impl LogInner {
    fn new(kv_store: Box<dyn KVStore + Sync + Send>) -> Self {
        LogInner {
            running: true,
            kv_store,
            log: HashMap::new(),
            last_index: 0,
            last_executed: 0,
            global_last_executed: 0,
        }
    }

    fn insert(&mut self, instance: Instance) -> bool {
        insert(&mut self.log, instance)
    }

    fn is_executable(&self) -> bool {
        match self.log.get(&(self.last_executed + 1)) {
            Some(instance) => instance.is_committed(),
            None => false,
        }
    }

    fn execute(&mut self) -> (i64, Result<String, KVStoreError>) {
        self.last_executed += 1;
        let it = self.log.get_mut(&self.last_executed);
        assert!(it.is_some());
        let instance = it.unwrap();
        (instance.client_id, instance.execute(&mut self.kv_store))
    }
}

pub struct Log {
    inner: Mutex<LogInner>,
    executable: Notify,
    committable: Notify,
}

impl Log {
    pub fn new(kv_store: Box<dyn KVStore + Sync + Send>) -> Self {
        Log {
            inner: Mutex::new(LogInner::new(kv_store)),
            executable: Notify::new(),
            committable: Notify::new(),
        }
    }

    pub fn last_executed(&self) -> i64 {
        let inner = self.inner.lock();
        inner.last_executed
    }

    pub fn global_last_executed(&self) -> i64 {
        let inner = self.inner.lock();
        inner.global_last_executed
    }

    pub fn advance_last_index(&self) -> i64 {
        let mut inner = self.inner.lock();
        inner.last_index += 1;
        inner.last_index
    }

    pub fn set_last_index(&self, last_index: i64) {
        let mut inner = self.inner.lock();
        inner.last_index = last_index;
    }

    pub fn last_index(&self) -> i64 {
        let inner = self.inner.lock();
        inner.last_index
    }

    pub fn stop(&self) {
        let mut inner = self.inner.lock();
        inner.running = false;
        self.executable.notify_one();
    }

    pub fn append(&self, instance: Instance) {
        let mut inner = self.inner.lock();
        let i = instance.index;
        if i <= inner.global_last_executed {
            return;
        }
        let mut committable = false;
        if inner.insert(instance) {
            inner.last_index = cmp::max(inner.last_index, i);
            committable = true;
        }
        drop(inner);
        if committable {
            self.committable.notify_waiters();
        }
    }

    pub async fn commit(&self, index: i64) {
        assert!(index > 0, "invalid index");
        let mut executable = false;
        loop {
            let future = self.committable.notified();
            {
                let mut inner = self.inner.lock();
                let instance = inner.log.get_mut(&index);
                if instance.is_some() {
                    let instance = instance.unwrap();
                    if instance.is_inprogress() {
                        instance.commit();
                    }
                    if inner.is_executable() {
                        executable = true;
                    }
                    break;
                }
            }
            future.await;
        }
        if executable {
            self.executable.notify_one();
        }
    }

    pub async fn execute(&self) -> Option<(i64, Result<String, KVStoreError>)> {
        loop {
            let future = self.executable.notified();
            {
                let mut inner = self.inner.lock();
                if !inner.running {
                    return None;
                }
                if inner.is_executable() {
                    return Some(inner.execute());
                }
            }
            future.await;
        }
    }

    pub fn commit_until(&self, leader_last_executed: i64, ballot: i64) {
        assert!(leader_last_executed >= 0, "invalid leader_last_executed");
        assert!(ballot >= 0, "invalid ballot");

        let mut inner = self.inner.lock();
        for i in inner.last_executed + 1..=leader_last_executed {
            let it = inner.log.get_mut(&i);
            if let Some(instance) = it {
                assert!(ballot >= instance.ballot, "commit_until case 2");
                if instance.ballot == ballot {
                    instance.commit();
                }
            } else {
                break;
            }
        }
        if inner.is_executable() {
            self.executable.notify_one();
        }
    }

    pub fn trim_until(&self, leader_global_last_executed: i64) {
        let mut inner = &mut *self.inner.lock();
        while inner.global_last_executed < leader_global_last_executed {
            inner.global_last_executed += 1;
            let it = inner.log.remove(&inner.global_last_executed);
            assert!(it.unwrap().is_executed());
        }
    }

    pub fn instances(&self) -> Vec<Instance> {
        let inner = self.inner.lock();
        let mut instances = Vec::new();
        for i in inner.global_last_executed + 1..=inner.last_index {
            if let Some(instance) = inner.log.get(&i) {
                instances.push(instance.clone());
            }
        }
        instances
    }

    fn is_executable(&self) -> bool {
        let inner = self.inner.lock();
        inner.is_executable()
    }

    pub fn at(&self, index: i64) -> Option<Instance> {
        let inner = self.inner.lock();
        inner.log.get(&index).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_commands() -> (Command, Command, Command) {
        (Command::get(""), Command::put("", ""), Command::del(""))
    }

    #[test]
    fn constructor() {
        let log = Log::new(Box::new(MemKVStore::new()));

        assert_eq!(0, log.last_executed());
        assert_eq!(0, log.global_last_executed());
        assert!(!log.is_executable());

        assert_eq!(None, log.at(0));
        assert_eq!(None, log.at(-1));
        assert_eq!(None, log.at(3));
    }

    #[test]
    fn insert() {
        let mut log = LogInner::new(Box::new(MemKVStore::new()));
        let (_, put, _) = make_commands();
        let index = 1;
        let ballot = 1;
        let instance1 = Instance::inprogress(ballot, index, &put, 0);
        let instance2 = instance1.clone();

        assert!(log.insert(instance1));
        assert_eq!(put, *log.log[&index].command.as_ref().unwrap());
        assert!(!log.insert(instance2));
    }

    #[test]
    fn insert_update_inprogress() {
        let mut log = LogInner::new(Box::new(MemKVStore::new()));
        let (_, put, del) = make_commands();
        let index = 1;
        let ballot = 1;
        let instance1 = Instance::inprogress(ballot, index, &put, 0);
        let instance2 = Instance::inprogress(ballot + 1, index, &del, 0);

        assert!(log.insert(instance1));
        assert_eq!(put, *log.log[&index].command.as_ref().unwrap());
        assert!(!log.insert(instance2));
        assert_eq!(del, *log.log[&index].command.as_ref().unwrap());
    }

    #[test]
    fn insert_update_commited() {
        let mut log = LogInner::new(Box::new(MemKVStore::new()));
        let (_, put, _) = make_commands();
        let index = 1;
        let ballot = 1;
        let instance1 = Instance::committed(ballot, index, &put, 0);
        let instance2 = Instance::inprogress(ballot + 1, index, &put, 0);

        assert!(log.insert(instance1));
        assert!(!log.insert(instance2));
    }

    #[test]
    fn insert_stale() {
        let mut log = LogInner::new(Box::new(MemKVStore::new()));
        let (_, put, del) = make_commands();
        let index = 1;
        let ballot = 1;
        let instance1 = Instance::inprogress(ballot, index, &put, 0);
        let instance2 = Instance::inprogress(ballot - 1, index, &del, 0);

        assert!(log.insert(instance1));
        assert_eq!(put, *log.log[&index].command.as_ref().unwrap());
        assert!(!log.insert(instance2));
        assert_eq!(put, *log.log[&index].command.as_ref().unwrap());
    }

    #[test]
    #[should_panic(expected = "insert case 2")]
    fn insert_case2_committed() {
        let mut log = LogInner::new(Box::new(MemKVStore::new()));
        let (_, put, del) = make_commands();
        let index = 1;
        let ballot = 0;
        let instance1 = Instance::committed(ballot, index, &put, 0);
        let instance2 = Instance::inprogress(ballot, index, &del, 0);

        assert!(log.insert(instance1));
        log.insert(instance2);
    }

    #[test]
    #[should_panic(expected = "insert case 2")]
    fn insert_case2_executed() {
        let mut log = LogInner::new(Box::new(MemKVStore::new()));
        let (_, put, del) = make_commands();
        let index = 1;
        let ballot = 0;
        let instance1 = Instance::executed(ballot, index, &put, 0);
        let instance2 = Instance::inprogress(ballot, index, &del, 0);

        assert!(log.insert(instance1));
        log.insert(instance2);
    }

    #[test]
    #[should_panic(expected = "insert case 3")]
    fn insert_case3() {
        let mut log = LogInner::new(Box::new(MemKVStore::new()));
        let (_, put, del) = make_commands();
        let index = 1;
        let ballot = 0;
        let instance1 = Instance::inprogress(ballot, index, &put, 0);
        let instance2 = Instance::inprogress(ballot, index, &del, 0);

        assert!(log.insert(instance1));
        log.insert(instance2);
    }

    #[test]
    fn append() {
        let log = Log::new(Box::new(MemKVStore::new()));
        let (get, _, _) = make_commands();
        let ballot = 0;
        let instance1 =
            Instance::inprogress(ballot, log.advance_last_index(), &get, 0);
        let instance2 =
            Instance::inprogress(ballot, log.advance_last_index(), &get, 0);

        log.append(instance1);
        log.append(instance2);
        assert_eq!(1, log.at(1).unwrap().index);
        assert_eq!(2, log.at(2).unwrap().index);
    }

    #[test]
    fn append_with_gap() {
        let log = Log::new(Box::new(MemKVStore::new()));
        let (get, _, _) = make_commands();
        let ballot = 0;
        let index = 42;
        let instance = Instance::inprogress(ballot, index, &get, 0);

        log.append(instance);
        assert_eq!(index, log.at(index).unwrap().index);
        assert_eq!(index + 1, log.advance_last_index());
    }

    #[test]
    fn append_fill_gaps() {
        let log = Log::new(Box::new(MemKVStore::new()));
        let (get, _, _) = make_commands();
        let ballot = 0;
        let index = 42;
        let instance1 = Instance::inprogress(ballot, index, &get, 0);
        let instance2 = Instance::inprogress(ballot, index - 10, &get, 0);

        log.append(instance1);
        log.append(instance2);
        assert_eq!(index + 1, log.advance_last_index());
    }

    #[test]
    fn append_high_ballot_override() {
        let log = Log::new(Box::new(MemKVStore::new()));
        let (_, put, del) = make_commands();
        let index = 1;
        let lo_ballot = 0;
        let hi_ballot = 1;
        let instance1 = Instance::inprogress(lo_ballot, index, &put, 0);
        let instance2 = Instance::inprogress(hi_ballot, index, &del, 0);

        log.append(instance1);
        log.append(instance2);
        assert_eq!(del, log.at(index).unwrap().command.unwrap());
    }

    #[test]
    fn append_low_ballot_no_effect() {
        let log = Log::new(Box::new(MemKVStore::new()));
        let (_, put, del) = make_commands();
        let index = 1;
        let lo_ballot = 0;
        let hi_ballot = 1;
        let instance1 = Instance::inprogress(hi_ballot, index, &put, 0);
        let instance2 = Instance::inprogress(lo_ballot, index, &del, 0);

        log.append(instance1);
        log.append(instance2);
        assert_eq!(put, log.at(index).unwrap().command.unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn commit() {
        let log = Log::new(Box::new(MemKVStore::new()));
        let (get, _, _) = make_commands();
        let ballot = 0;
        let index1 = 1;
        let index2 = 2;
        let instance1 = Instance::inprogress(ballot, index1, &get, 0);
        let instance2 = Instance::inprogress(ballot, index2, &get, 0);

        log.append(instance1);
        log.append(instance2);
        assert!(log.at(index1).unwrap().is_inprogress());
        assert!(log.at(index2).unwrap().is_inprogress());
        assert!(!log.is_executable());

        log.commit(index2).await;

        assert!(log.at(index1).unwrap().is_inprogress());
        assert!(log.at(index2).unwrap().is_committed());
        assert!(!log.is_executable());

        log.commit(index1).await;

        assert!(log.at(index1).unwrap().is_committed());
        assert!(log.at(index2).unwrap().is_committed());
        assert!(log.is_executable());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn commit_before_append() {
        let log = Arc::new(Log::new(Box::new(MemKVStore::new())));
        let (get, _, _) = make_commands();
        let ballot = 0;
        let index = log.advance_last_index();
        let instance = Instance::inprogress(ballot, index, &get, 0);

        let commit_task = {
            let log = Arc::clone(&log);
            tokio::spawn(async move {
                log.commit(index).await;
            })
        };
        log.append(instance);
        commit_task.await;
        assert!(log.at(index).unwrap().is_committed());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn append_commit_execute() {
        let log = Arc::new(Log::new(Box::new(MemKVStore::new())));
        let (get, _, _) = make_commands();
        let ballot = 0;
        let index = log.advance_last_index();
        let instance = Instance::inprogress(ballot, index, &get, 0);

        let execute_task = {
            let log = Arc::clone(&log);
            tokio::spawn(async move {
                log.execute().await;
            })
        };

        log.append(instance);
        log.commit(index).await;
        execute_task.await;

        assert!(log.at(index).unwrap().is_executed());
        assert_eq!(index, log.last_executed());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn append_commit_execute_out_of_order() {
        let log = Arc::new(Log::new(Box::new(MemKVStore::new())));
        let (get, _, _) = make_commands();
        let ballot = 0;
        let index1 = 1;
        let instance1 = Instance::inprogress(ballot, index1, &get, 0);
        let index2 = 2;
        let instance2 = Instance::inprogress(ballot, index2, &get, 0);
        let index3 = 3;
        let instance3 = Instance::inprogress(ballot, index3, &get, 0);

        let execute_task = {
            let log = Arc::clone(&log);
            tokio::spawn(async move {
                log.execute().await;
                log.execute().await;
                log.execute().await;
            })
        };

        log.append(instance1);
        log.append(instance2);
        log.append(instance3);

        join_all(vec![
            log.commit(index3),
            log.commit(index2),
            log.commit(index1),
        ])
        .await;

        execute_task.await;

        assert!(log.at(index1).unwrap().is_executed());
        assert!(log.at(index2).unwrap().is_executed());
        assert!(log.at(index3).unwrap().is_executed());
        assert_eq!(index3, log.last_executed());
    }

    #[test]
    fn commit_until() {
        let log = Arc::new(Log::new(Box::new(MemKVStore::new())));
        let (get, _, _) = make_commands();
        let ballot = 0;
        let index1 = 1;
        let instance1 = Instance::inprogress(ballot, index1, &get, 0);
        let index2 = 2;
        let instance2 = Instance::inprogress(ballot, index2, &get, 0);
        let index3 = 3;
        let instance3 = Instance::inprogress(ballot, index3, &get, 0);

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
        let log = Arc::new(Log::new(Box::new(MemKVStore::new())));
        let (get, _, _) = make_commands();
        let ballot = 0;
        let index1 = 1;
        let instance1 = Instance::inprogress(ballot, index1, &get, 0);
        let index2 = 2;
        let instance2 = Instance::inprogress(ballot, index2, &get, 0);
        let index3 = 3;
        let instance3 = Instance::inprogress(ballot, index3, &get, 0);

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
        let log = Arc::new(Log::new(Box::new(MemKVStore::new())));
        let (get, _, _) = make_commands();
        let ballot = 5;
        let index1 = 1;
        let instance1 = Instance::inprogress(ballot, index1, &get, 0);
        let index2 = 2;
        let instance2 = Instance::inprogress(ballot, index2, &get, 0);
        let index3 = 3;
        let instance3 = Instance::inprogress(ballot, index3, &get, 0);

        log.append(instance1);
        log.append(instance2);
        log.append(instance3);
        log.commit_until(index3, ballot - 1);
    }

    #[test]
    fn commit_until_with_gap() {
        let log = Arc::new(Log::new(Box::new(MemKVStore::new())));
        let (get, _, _) = make_commands();
        let ballot = 0;
        let index1 = 1;
        let instance1 = Instance::inprogress(ballot, index1, &get, 0);
        let index3 = 3;
        let instance3 = Instance::inprogress(ballot, index3, &get, 0);
        let index4 = 4;
        let instance4 = Instance::inprogress(ballot, index4, &get, 0);

        log.append(instance1);
        log.append(instance3);
        log.append(instance4);
        log.commit_until(index4, ballot);

        assert!(log.at(index1).unwrap().is_committed());
        assert!(!log.at(index3).unwrap().is_committed());
        assert!(!log.at(index4).unwrap().is_committed());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn append_commit_until_execute() {
        let log = Arc::new(Log::new(Box::new(MemKVStore::new())));
        let (get, _, _) = make_commands();
        let ballot = 0;
        let index1 = 1;
        let instance1 = Instance::inprogress(ballot, index1, &get, 0);
        let index2 = 2;
        let instance2 = Instance::inprogress(ballot, index2, &get, 0);
        let index3 = 3;
        let instance3 = Instance::inprogress(ballot, index3, &get, 0);

        let execute_task = {
            let log = Arc::clone(&log);
            tokio::spawn(async move {
                log.execute().await;
                log.execute().await;
                log.execute().await;
            })
        };

        log.append(instance1);
        log.append(instance2);
        log.append(instance3);

        log.commit_until(index3, ballot);

        execute_task.await;

        assert!(log.at(index1).unwrap().is_executed());
        assert!(log.at(index2).unwrap().is_executed());
        assert!(log.at(index3).unwrap().is_executed());
        assert!(!log.is_executable());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn append_commit_until_execute_trim_until() {
        let log = Arc::new(Log::new(Box::new(MemKVStore::new())));
        let (get, _, _) = make_commands();
        let ballot = 0;
        let index1 = 1;
        let instance1 = Instance::inprogress(ballot, index1, &get, 0);
        let index2 = 2;
        let instance2 = Instance::inprogress(ballot, index2, &get, 0);
        let index3 = 3;
        let instance3 = Instance::inprogress(ballot, index3, &get, 0);

        let execute_task = {
            let log = Arc::clone(&log);
            tokio::spawn(async move {
                log.execute().await;
                log.execute().await;
                log.execute().await;
            })
        };

        log.append(instance1);
        log.append(instance2);
        log.append(instance3);

        log.commit_until(index3, ballot);
        execute_task.await;

        log.trim_until(index3);

        assert_eq!(None, log.at(index1));
        assert_eq!(None, log.at(index2));
        assert_eq!(None, log.at(index3));
        assert_eq!(index3, log.last_executed());
        assert_eq!(index3, log.global_last_executed());
        assert!(!log.is_executable());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn append_at_trimmed_index() {
        let log = Arc::new(Log::new(Box::new(MemKVStore::new())));
        let (get, _, _) = make_commands();
        let ballot = 0;
        let index1 = 1;
        let instance1 = Instance::inprogress(ballot, index1, &get, 0);
        let index2 = 2;
        let instance2 = Instance::inprogress(ballot, index2, &get, 0);

        let execute_task = {
            let log = Arc::clone(&log);
            tokio::spawn(async move {
                log.execute().await;
                log.execute().await;
            })
        };

        log.append(instance1.clone());
        log.append(instance2.clone());

        log.commit_until(index2, ballot);
        execute_task.await;

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn instances() {
        let log = Arc::new(Log::new(Box::new(MemKVStore::new())));
        let (get, _, _) = make_commands();
        let ballot = 0;
        let index1 = 1;
        let instance1 = Instance::inprogress(ballot, index1, &get, 0);
        let index2 = 2;
        let instance2 = Instance::inprogress(ballot, index2, &get, 0);
        let index3 = 3;
        let instance3 = Instance::inprogress(ballot, index3, &get, 0);

        let expected =
            vec![instance1.clone(), instance2.clone(), instance3.clone()];

        let execute_task = {
            let log = Arc::clone(&log);
            tokio::spawn(async move {
                log.execute().await;
                log.execute().await;
            })
        };

        log.append(instance1);
        log.append(instance2);
        log.append(instance3);

        assert_eq!(expected, log.instances());

        log.commit_until(index2, ballot);

        execute_task.await;

        log.trim_until(index2);

        assert_eq!(&expected[index2 as usize..], log.instances());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn calling_stop_unblocks_executor() {
        let log = Arc::new(Log::new(Box::new(MemKVStore::new())));

        let execute_task = {
            let log = Arc::clone(&log);
            tokio::spawn(async move {
                let r = log.execute().await;
                assert!(r.is_none());
            })
        };
        log.stop();
        execute_task.await;
    }
}
