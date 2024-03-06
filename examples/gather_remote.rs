use burst_communication_middleware::{
    BurstMiddleware, BurstOptions, Message, Middleware, MiddlewareActorHandle, RabbitMQMImpl,
    RabbitMQOptions, RedisListImpl, RedisListOptions, RedisStreamImpl, RedisStreamOptions, S3Impl,
    S3Options, TokioChannelImpl, TokioChannelOptions,
};
use bytes::{Bytes, BytesMut};
use log::{error, info};
use std::{
    collections::{HashMap, HashSet},
    env, thread,
};

const BURST_SIZE: u32 = 128;
const GROUPS: u32 = 16;
const PAYLOAD_SIZE: usize = 4 * 1024 * 1024; // 4MB

#[derive(Debug)]
struct Msg(Vec<u32>);

impl From<Bytes> for Msg {
    fn from(bytes: Bytes) -> Self {
        let mut vec = Vec::new();
        for i in (0..bytes.len()).step_by(4) {
            let value = u32::from_le_bytes([bytes[i], bytes[i + 1], bytes[i + 2], bytes[i + 3]]);
            vec.push(value);
        }
        Msg(vec)
    }
}

impl From<Msg> for Bytes {
    fn from(msg: Msg) -> Self {
        let mut bytes = BytesMut::with_capacity(msg.0.len() * 4);
        for value in msg.0 {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes.freeze()
    }
}

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
                .burst_id("gather".to_string())
                .enable_message_chunking(true)
                .message_chunk_size(1 * 1024 * 1024)
                .build();

        let channel_options = TokioChannelOptions::new()
            .broadcast_channel_size(256)
            .build();

        let backend_options = RabbitMQOptions::new("amqp://guest:guest@localhost:5672".to_string())
            .durable_queues(true)
            .ack(true)
            .build();
        // let s3_options = S3Options::new(env::var("S3_BUCKET").unwrap())
        //     .access_key_id(env::var("AWS_ACCESS_KEY_ID").unwrap())
        //     .secret_access_key(env::var("AWS_SECRET_ACCESS_KEY").unwrap())
        //     .session_token(Some(env::var("AWS_SESSION_TOKEN").unwrap()))
        //     .region(env::var("S3_REGION").unwrap())
        //     .endpoint(None)
        //     .enable_broadcast(true)
        //     .build();
        // let backend_options = RedisListOptions::new("redis://127.0.0.1".to_string()).build();
        // let backend_options = RedisStreamOptions::new("redis://127.0.0.1".to_string()).build();

        let fut = tokio_runtime.spawn(BurstMiddleware::create_proxies::<
            TokioChannelImpl,
            RabbitMQMImpl,
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
            .collect::<HashMap<u32, Middleware>>();

        let group_threads = group(actors);
        threads.extend(group_threads);
    }

    for thread in threads {
        thread.join().unwrap();
    }
}

fn group(proxies: HashMap<u32, Middleware>) -> Vec<std::thread::JoinHandle<()>> {
    let mut threads = Vec::with_capacity(proxies.len());
    for (worker_id, proxy) in proxies {
        let thread = thread::spawn(move || {
            info!("thread start: id={}", worker_id);
            worker(proxy);
            info!("thread end: id={}", worker_id);
        });
        threads.push(thread);
    }

    threads
}

fn worker(burst_middleware: Middleware) {
    let burst_middleware = burst_middleware.get_actor_handle();
    // let msg = format!("hello from worker {}", burst_middleware.info.worker_id);
    // let data = Bytes::from(msg);
    let data = Msg((0..PAYLOAD_SIZE as u32 / 4).collect());

    if let Some(msgs) = burst_middleware.gather(data, 0).unwrap() {
        for msg in msgs {
            info!(
                "worker {} received message: {:?}",
                burst_middleware.info.worker_id, msg
            );
        }
    }
}
