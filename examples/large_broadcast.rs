use burst_communication_middleware::{
    BurstMiddleware, BurstOptions, Middleware, MiddlewareActorHandle, RabbitMQMImpl,
    RabbitMQOptions, RedisListImpl, RedisListOptions, RedisStreamImpl, RedisStreamOptions, S3Impl,
    S3Options, TokioChannelImpl, TokioChannelOptions,
};
use bytes::Bytes;
use log::{error, info};
use std::{
    collections::{HashMap, HashSet},
    env, thread,
};

const BURST_SIZE: u32 = 64;
const GROUPS: u32 = 4;
const PAYLOAD_SIZE: usize = 256 * 1024 * 1024; // 256MB

fn main() {
    env_logger::init();

    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

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
        let burst_options =
            BurstOptions::new(BURST_SIZE, group_ranges.clone(), group_id.to_string())
                .burst_id("broadcast".to_string())
                .enable_message_chunking(true)
                .message_chunk_size(4 * 1024 * 1024)
                .build();

        let channel_options = TokioChannelOptions::new()
            .broadcast_channel_size(256)
            .build();

        // let backend_options = RabbitMQOptions::new("amqp://guest:guest@localhost:5672".to_string())
        //     .durable_queues(true)
        //     .ack(true)
        //     .build();
        // let backend_options = S3Options::new(env::var("S3_BUCKET").unwrap())
        //     .access_key_id(env::var("AWS_ACCESS_KEY_ID").unwrap())
        //     .secret_access_key(env::var("AWS_SECRET_ACCESS_KEY").unwrap())
        //     .session_token(None)
        //     .region(env::var("S3_REGION").unwrap())
        //     .endpoint(Some("http://localhost:9000".to_string()))
        //     .enable_broadcast(true)
        //     .wait_time(3.0)
        //     .build();
        let backend_options = RedisListOptions::new("redis://127.0.0.1".to_string()).build();
        // let backend_options = RedisStreamOptions::new("redis://127.0.0.1".to_string());

        let fut = tokio_runtime.spawn(BurstMiddleware::create_proxies::<
            TokioChannelImpl,
            RedisListImpl,
            _,
            _,
        >(burst_options, channel_options, backend_options));
        let proxies = tokio_runtime.block_on(fut).unwrap().unwrap();

        let actors = proxies
            .into_iter()
            .map(|(worker_id, middleware)| {
                (
                    worker_id,
                    Middleware::new(middleware, tokio_runtime.handle().clone()),
                )
            })
            .collect::<HashMap<u32, Middleware<Bytes>>>();

        for (worker_id, actor) in actors {
            let thread = thread::spawn(move || {
                info!("thread start: id={}", worker_id);
                worker(actor);
                info!("thread end: id={}", worker_id);
            });
            threads.push(thread);
        }
    }

    for thread in threads {
        thread.join().unwrap();
    }
}

fn worker(burst_middleware: Middleware<Bytes>) {
    let burst_middleware = burst_middleware.get_actor_handle();
    let res = if burst_middleware.info.worker_id == 0 {
        let payload = Bytes::from(vec![0; PAYLOAD_SIZE]);
        log::info!(
            "worker {} (root)  => sending broadcast with size {}",
            burst_middleware.info.worker_id,
            payload.len()
        );
        burst_middleware.broadcast(Some(payload), 0).unwrap()
    } else {
        log::info!(
            "worker {} (group {}) => waiting for broadcast",
            burst_middleware.info.worker_id,
            burst_middleware.info.group_id
        );
        burst_middleware.broadcast(None, 0).unwrap()
    };
    log::info!(
        "worker {} (group {}) => received broadcast message with size {}",
        burst_middleware.info.worker_id,
        burst_middleware.info.group_id,
        res.len()
    );
}
