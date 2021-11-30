use crate::kvstore::traits::KVStore;
use crate::kvstore::command::Command;

pub trait Consensus {
    fn new(store: Box<dyn KVStore>) -> Self where Self: Sized;

    fn agree_and_execute(&mut self, cmd: Command) -> Result<&str, &str>;
}
