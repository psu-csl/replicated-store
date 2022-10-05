mod replicant;

use clap::Parser;
use replicant::Replicant;
use serde_json::json;
use serde_json::Value as json;
use std::fs;
use std::io::BufReader;
use std::path::PathBuf;
use tokio::signal;
use tokio::time::{sleep, Duration};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// my id in the peers array in configuration file
    #[arg(short, long, default_value_t = 0)]
    id: i64,

    /// path to the configuration file
    #[arg(short, long, default_value_t = String::from("config.json"))]
    config_path: String,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Args::parse();
    let file = fs::File::open(PathBuf::from(args.config_path)).unwrap();
    let mut config: json =
        serde_json::from_reader(BufReader::new(file)).unwrap();

    let num_peers = config["peers"].as_array().unwrap().len() as i64;

    assert!(args.id < num_peers);

    config
        .as_object_mut()
        .unwrap()
        .insert(String::from("id"), json!(args.id));

    let replicant = Replicant::new(&config);
    let shutdown = replicant.start();

    sleep(Duration::from_millis(10000)).await;
    replicant.stop(shutdown);
}
