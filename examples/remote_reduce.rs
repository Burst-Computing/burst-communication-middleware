use burst_communication_middleware::{
    BurstMiddleware, BurstOptions, Message, MiddlewareActorHandle, RabbitMQMImpl, RabbitMQOptions,
    RedisListImpl, RedisListOptions, RedisStreamImpl, RedisStreamOptions, S3Impl, S3Options,
    TokioChannelImpl, TokioChannelOptions,
};
use bytes::Bytes;
use log::{error, info};
use std::{
    collections::{HashMap, HashSet},
    env,
    ops::Add,
    thread,
};

const BURST_SIZE: u32 = 8;
const GROUPS: u32 = 4;

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

        // let actors = proxies
        //     .into_iter()
        //     .map(|(worker_id, middleware)| {
        //         let actor = MiddlewareActorHandle::new(middleware, &tokio_runtime);
        //         (worker_id, actor)
        //     })
        //     .collect::<HashMap<u32, MiddlewareActorHandle>>();

        let group_threads = group(proxies);
        threads.extend(group_threads);
    }

    for thread in threads {
        thread.join().unwrap();
    }
}

fn group(proxies: HashMap<u32, BurstMiddleware>) -> Vec<std::thread::JoinHandle<()>> {
    let mut threads = Vec::with_capacity(proxies.len());
    for (worker_id, proxy) in proxies {
        let thread = thread::spawn(move || {
            info!("thread start: id={}", worker_id);
            worker(proxy);
            info!("thread end: id={}", worker_id);
        });
        threads.push(thread);
    }

    return threads;
}

#[derive(Clone, Copy, Debug)]
struct Myi32(i32);

impl From<Bytes> for Myi32 {
    fn from(bytes: Bytes) -> Self {
        let val = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        Myi32(val)
    }
}

impl From<Myi32> for Bytes {
    fn from(val: Myi32) -> Self {
        let bytes = val.0.to_be_bytes();
        Bytes::copy_from_slice(&bytes)
    }
}

impl Add for Myi32 {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self(self.0 + other.0)
    }
}

fn worker(mut burst_middleware: BurstMiddleware) {
    let tokio_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let val: Myi32 = Myi32(1);
    let res = tokio_runtime
        .block_on(burst_middleware.reduce(val, |a, b| a + b))
        .unwrap();

    if let Some(res) = res {
        info!("----------> Reduced value is {:?} <----------", res);
    }
}
