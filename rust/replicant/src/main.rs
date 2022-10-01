mod replicant;

use replicant::Replicant;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() {
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

    let replicant = Replicant::new(&config);
    let shutdown = replicant.start();
    sleep(Duration::from_millis(10000)).await;
    replicant.stop(shutdown);
}
