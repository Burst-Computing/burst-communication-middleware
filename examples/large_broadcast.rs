use burst_communication_middleware::{
    BurstMiddleware, BurstOptions, Message, RabbitMQMImpl, RabbitMQOptions, RedisListImpl,
    RedisListOptions, RedisStreamOptions, S3Impl, S3Options, TokioChannelImpl, TokioChannelOptions,
};
use bytes::Bytes;
use log::{error, info};
use redis::FromRedisValue;
use std::{
    collections::{HashMap, HashSet},
    env, thread,
};

const BURST_SIZE: u32 = 64;
const GROUPS: u32 = 4;
const PAYLOAD_SIZE: u32 = 256 * 1024 * 1024; // 256 MB

#[tokio::main]
async fn main() {
    env_logger::init();

    if BURST_SIZE % GROUPS != 0 {
        panic!("BURST_SIZE must be divisible by GROPUS");
    }

    let group_size = BURST_SIZE / GROUPS;

    let group_ranges = (0..GROUPS)
        .map(|group_id| {
            (
                group_id.to_string(),
                ((group_size * group_id)..((group_size * group_id) + group_size)).collect(),
            )
        })
        .collect::<HashMap<String, HashSet<u32>>>();

    let mut threads = Vec::with_capacity(BURST_SIZE as usize);
    for group_id in 0..GROUPS {
        let burst_options = BurstOptions::new(
            "broadcast".to_string(),
            BURST_SIZE,
            group_ranges.clone(),
            group_id.to_string(),
            true,
            4 * 1024 * 1024,
        );

        let channel_options = TokioChannelOptions::new()
            .broadcast_channel_size(256)
            .build();

        // let backend_options = RabbitMQOptions::new("amqp://guest:guest@localhost:5672".to_string())
        //     .durable_queues(true)
        //     .ack(true)
        //     .build();
        // let s3_options = S3Options::new(env::var("S3_BUCKET").unwrap())
        //     .access_key_id(env::var("AWS_ACCESS_KEY_ID").unwrap())
        //     .secret_access_key(env::var("AWS_SECRET_ACCESS_KEY").unwrap())
        //     .session_token(Some(env::var("AWS_SESSION_TOKEN").unwrap()))
        //     .region(env::var("S3_REGION").unwrap())
        //     .endpoint(None)
        //     .enable_broadcast(true)
        //     .build();
        let backend_options = RedisListOptions::new("redis://127.0.0.1".to_string()).build();

        let proxies =
            match BurstMiddleware::create_proxies::<TokioChannelImpl, RedisListImpl, _, _>(
                burst_options,
                channel_options,
                backend_options,
            )
            .await
            {
                Ok(p) => p,
                Err(e) => {
                    error!("{:?}", e);
                    panic!();
                }
            };

        let group_threads = group(proxies).await;
        threads.extend(group_threads);
    }

    for thread in threads {
        thread.join().unwrap();
    }
}

async fn group(proxies: HashMap<u32, BurstMiddleware>) -> Vec<std::thread::JoinHandle<()>> {
    let mut threads = Vec::with_capacity(proxies.len());
    for (worker_id, proxy) in proxies {
        let thread = thread::spawn(move || {
            info!("thread start: id={}", worker_id);
            let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            let result = tokio_runtime.block_on(async { worker(proxy).await.unwrap() });
            info!("thread end: id={}", worker_id);
            result
        });
        threads.push(thread);
    }

    return threads;
}

pub async fn worker(burst_middleware: BurstMiddleware) -> Result<(), Box<dyn std::error::Error>> {
    let res: Message;
    if burst_middleware.info().worker_id == 0 {
        let data = Bytes::from(vec![0; PAYLOAD_SIZE as usize]);
        log::info!(
            "worker {} (root)  => sending broadcast message with size {}",
            burst_middleware.info().worker_id,
            data.len()
        );
        res = burst_middleware.broadcast(Some(data)).await.unwrap();
    } else {
        log::info!(
            "worker {} (group {}) => waiting for broadcast",
            burst_middleware.info().worker_id,
            burst_middleware.info().group_id
        );
        res = burst_middleware.broadcast(None).await.unwrap();
    }

    log::info!(
        "worker {} (group {}) => received broadcast message {:?}",
        burst_middleware.info().worker_id,
        burst_middleware.info().group_id,
        res
    );
    Ok(())
}
