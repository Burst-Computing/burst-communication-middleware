use burst_communication_middleware::{
    BurstMiddleware, BurstOptions, MiddlewareActorHandle, RabbitMQMImpl, RabbitMQOptions,
    RedisListImpl, RedisListOptions, S3Impl, S3Options, TokioChannelImpl, TokioChannelOptions,
};
use bytes::Bytes;
use log::{error, info};
use std::{
    collections::{HashMap, HashSet},
    env,
    thread::{self, JoinHandle},
    vec,
};

fn handle_group(
    group_id: String,
    group: HashMap<String, HashSet<u32>>,
    worker_id: u32,
    tokio_runtime: &tokio::runtime::Runtime,
) -> JoinHandle<()> {
    let fut = tokio_runtime.spawn(BurstMiddleware::create_proxies::<
        TokioChannelImpl,
        RabbitMQMImpl,
        _,
        _,
    >(
        BurstOptions::new(
            "hello_world".to_string(),
            2,
            group,
            group_id,
            true,
            1 * 1024 * 1024,
        ),
        TokioChannelOptions::new()
            .broadcast_channel_size(256)
            .build(),
        RabbitMQOptions::new("amqp://guest:guest@localhost:5672".to_string())
            .durable_queues(true)
            .ack(true)
            .build(),
        // S3Options::new(env::var("S3_BUCKET").unwrap())
        //     .access_key_id(env::var("AWS_ACCESS_KEY_ID").unwrap())
        //     .secret_access_key(env::var("AWS_SECRET_ACCESS_KEY").unwrap())
        //     .session_token(Some(env::var("AWS_SESSION_TOKEN").unwrap()))
        //     .region(env::var("S3_REGION").unwrap())
        //     .endpoint(None)
        //     .build(),
        // RedisListOptions::new("redis://127.0.0.1".to_string()),
    ));
    let mut proxies = tokio_runtime.block_on(fut).unwrap().unwrap();
    let proxy = proxies.remove(&worker_id).unwrap();

    let actor = MiddlewareActorHandle::new(proxy, &tokio_runtime);

    let thread = thread::spawn(move || {
        let worker_id = actor.info.worker_id;
        info!("thread start: id={}", worker_id);
        worker(actor);
        info!("thread end: id={}", worker_id);
    });
    return thread;
}

fn main() {
    env_logger::init();

    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let group_0 = vec![("0".to_string(), vec![0].into_iter().collect())]
        .into_iter()
        .collect::<HashMap<String, HashSet<u32>>>();

    let g1 = handle_group("0".to_string(), group_0, 0, &tokio_runtime);

    let group_1 = vec![("1".to_string(), vec![1].into_iter().collect())]
        .into_iter()
        .collect::<HashMap<String, HashSet<u32>>>();

    let g2 = handle_group("1".to_string(), group_1, 1, &tokio_runtime);

    g1.join().unwrap();
    g2.join().unwrap();
}

fn worker(burst_middleware: MiddlewareActorHandle) {
    info!("hi im worker with id={}", burst_middleware.info.worker_id);
    if burst_middleware.info.worker_id == 0 {
        info!("worker {} sending message", burst_middleware.info.worker_id);
        let message = "hello world".to_string();
        let payload = Bytes::from(message);
        burst_middleware.send(1, payload).unwrap();

        info!(
            "worker {} waiting for response...",
            burst_middleware.info.worker_id
        );
        let response = burst_middleware.recv(1).unwrap();
        info!(
            "worker {} received message: {:?}",
            burst_middleware.info.worker_id, response
        );
    } else {
        info!(
            "worker {} waiting for message...",
            burst_middleware.info.worker_id
        );
        let message = burst_middleware.recv(0).unwrap();
        info!(
            "worker {} received message: {:?}",
            burst_middleware.info.worker_id, message
        );
        let response = "bye!".to_string();
        let payload = Bytes::from(response);
        info!(
            "worker {} sending response",
            burst_middleware.info.worker_id
        );
        burst_middleware.send(0, payload).unwrap();
        info!("worker {} done!", burst_middleware.info.worker_id);
    }
}
