use std::{
    ops::Range,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use clap::Parser;
use group_communication_middleware::{Middleware, MiddlewareArguments};
use tracing::{error, info};
use tracing_subscriber::{
    fmt, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DURATION: u64 = 2;
const CHUNK_SIZE: usize = 1024 * 1024; // 1 MB
const NUM_EXECUTIONS: usize = 3;

#[derive(Parser, Debug)]
pub struct Arguments {
    /// RabbitMQ server address
    #[arg(
        long = "rabbitmq-server",
        default_value = "amqp://guest:guest@localhost:5672",
        required = false
    )]
    pub rabbitmq_server: String,

    /// Number of thread pairs
    #[arg(required = true)]
    pub num_threads: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Setup logging

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .try_init()?;

    let args = Arguments::parse();

    info!("{:?}", args);

    let global_range = 0..args.num_threads;
    let local_range = 0..args.num_threads;

    let middleware = match Middleware::init_global(MiddlewareArguments::new(
        args.rabbitmq_server.clone(),
        global_range.clone(),
        local_range.clone(),
    ))
    .await
    {
        Ok(m) => m,
        Err(e) => {
            error!("{:?}", e);
            return Err(e);
        }
    };

    let mut results = vec![];

    for i in 0..NUM_EXECUTIONS {
        info!("Execution {} of {}", i + 1, NUM_EXECUTIONS);

        let mut handles = vec![];
        let mut start_times = vec![];
        let mut end_times = vec![];
        let mut total_bytes = vec![];

        for i in local_range.clone() {
            let mut middleware = middleware.clone();
            let global_range = global_range.clone();
            let local_range = local_range.clone();

            let start_time = Arc::new(Mutex::new(Instant::now()));
            let end_time = Arc::new(Mutex::new(Instant::now()));
            let bytes = Arc::new(Mutex::new(0));

            start_times.push(start_time.clone());
            end_times.push(end_time.clone());
            total_bytes.push(bytes.clone());

            handles.push(thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let r = rt.block_on(async {
                    middleware
                        .init_local(i)
                        .await
                        .expect("Failed to init middleware");

                    let r = worker(
                        middleware,
                        i,
                        global_range.clone(),
                        local_range.clone(),
                        DURATION,
                        start_time,
                        end_time,
                        bytes,
                    )
                    .await;
                    info!("runtime end: id={}", i);
                    r
                });
                info!("thread end: id={}", i);
                r
            }));
        }

        for handle in handles {
            if let Err(e) = handle.join().unwrap() {
                error!("{:?}", e);
            }
            info!("join end");
        }

        let start = start_times
            .iter()
            .map(|x| *x.lock().unwrap())
            .min()
            .unwrap();
        let end = end_times.iter().map(|x| *x.lock().unwrap()).max().unwrap();
        let elapsed_time = end.duration_since(start).as_secs_f64();

        let total_bytes: usize = total_bytes.iter().map(|x| *x.lock().unwrap()).sum();

        let bandwidth = total_bytes as f64 / elapsed_time;

        let mbytesps = bandwidth / 1024.0 / 1024.0;

        info!("Total bytes: {}", total_bytes);
        info!("Duration: {:.3} s", elapsed_time);
        info!("Bandwidth: {:.3} MB/s", mbytesps);

        results.push(mbytesps);
    }

    let avg = results.iter().sum::<f64>() / results.len() as f64;
    let stdev =
        (results.iter().map(|x| (x - avg) * (x - avg)).sum::<f64>() / results.len() as f64).sqrt();

    info!("Average,stdev: {},{}", avg, stdev);

    Ok(())
}

async fn worker(
    middleware: Middleware,
    id: u32,
    global_range: Range<u32>,
    local_range: Range<u32>,
    duration: u64,
    start_time: Arc<Mutex<Instant>>,
    end_time: Arc<Mutex<Instant>>,
    total_bytes: Arc<Mutex<usize>>,
) -> Result<()> {
    info!(
        "worker start: id={}, global_range={:?}, local_range={:?}",
        id, global_range, local_range
    );

    let data = vec![b'x'; CHUNK_SIZE];

    let mut elapsed_time = Duration::new(0, 0);
    let start = Instant::now();
    let mut start_time = start_time.lock().unwrap();
    *start_time = start.clone();
    drop(start_time); // Release the lock early

    let global_rng = global_range.clone();
    let mddwr = middleware.clone();
    let send = tokio::spawn(async move {
        while elapsed_time < Duration::from_secs(duration) {
            for receiver_id in global_rng.clone() {
                if receiver_id == id {
                    continue;
                }
                if let Err(e) = mddwr.send(receiver_id, data.clone()).await {
                    error!("Error: {}", e);
                }
            }
            elapsed_time = start.elapsed();
        }

        // Signal the end of data transfer to all receivers
        for receiver_id in global_rng.clone() {
            if receiver_id == id {
                continue;
            }
            if let Err(e) = mddwr.send(receiver_id, vec![]).await {
                error!("Error: {}", e);
            }
        }
    });

    let global_rng = global_range.clone();
    let mddwr = middleware.clone();
    let receive = tokio::spawn(async move {
        let mut received_bytes = 0;

        let mut num_empty = 0;

        while let Ok(msg) = mddwr.recv().await {
            if msg.data.is_empty() {
                num_empty += 1;
                if num_empty == global_rng.len() - 1 {
                    break;
                }
            }

            received_bytes += msg.data.len();
        }

        let mut total_bytes = total_bytes.lock().unwrap();
        *total_bytes = received_bytes;

        let mut end_time = end_time.lock().unwrap();
        *end_time = Instant::now();
    });

    let _ = tokio::join!(send, receive);

    info!("worker end: id={}", id);

    Ok(())
}
