use crate::kvstore::traits::KVStore;
use crate::kvstore::command::Command;

pub trait Consensus<S: KVStore> {
    fn new(store: S) -> Self;

    fn agree_and_execute(&mut self, cmd: Command) -> String;
}
