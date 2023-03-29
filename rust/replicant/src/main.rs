mod replicant;

use clap::Parser;
use replicant::Replicant;
use serde_json::json;
use serde_json::Value as json;
use std::fs;
use std::io::BufReader;
use std::path::PathBuf;
use std::time::Duration;
use log::error;
use tokio::signal;

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

    let monitor = tokio_metrics::TaskMonitor::new();
    {
        let monitor = monitor.clone();
        tokio::spawn(async move {
            for interval in monitor.intervals() {
                println!("{:?}, mean_poll_duration: {}us, slow_poll_ratio: {}%, mean_slow_poll_duration: {}us, mean_first_poll_delay: {}us, mean_scheduled_duration: {}us, mean_idle_duration: {}us",
                             interval,
                             interval.mean_poll_duration().as_micros(),
                             interval.slow_poll_ratio(),
                             interval.mean_slow_poll_duration().as_micros(),
                             interval.mean_first_poll_delay().as_micros(),
                             interval.mean_scheduled_duration().as_micros(),
                             interval.mean_idle_duration().as_micros());
                tokio::time::sleep(Duration::from_millis(3000)).await;
            }
        });
    }

    let handle = tokio::runtime::Handle::current();
    {
        let runtime_monitor = tokio_metrics::RuntimeMonitor::new(&handle);
        tokio::spawn(async move {
            for interval in runtime_monitor.intervals() {
                eprintln!("{:?}", interval);
                tokio::time::sleep(Duration::from_millis(3000)).await;
            }
        });
    }

    let replicant = Replicant::new(&config, monitor).await;
    let shutdown = replicant.start();
    signal::ctrl_c().await.unwrap();
    replicant.stop(shutdown).await;
}
