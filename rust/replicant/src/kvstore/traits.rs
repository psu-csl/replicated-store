// The key-value store interface.
pub trait KVStore {
    fn new() -> Self where Self: Sized;

    fn get(&self, key: &str) -> Result<&str, &str>;

    fn put(&mut self, key: String, value: String) -> Result<&str, &str>;

    fn del(&mut self, key: &str) -> Result<&str, &str>;
}
