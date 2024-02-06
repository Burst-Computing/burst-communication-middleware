use burst_communication_middleware::{
    BurstMiddleware, BurstOptions, MiddlewareActorHandle, RabbitMQMImpl, RabbitMQOptions,
    TokioChannelImpl, TokioChannelOptions,
};
use bytes::Bytes;
use log::{error, info};
use std::{
    collections::{HashMap, HashSet},
    thread,
};

fn main() {
    env_logger::init();

    let group_ranges = vec![(0.to_string(), vec![0, 1].into_iter().collect())]
        .into_iter()
        .collect::<HashMap<String, HashSet<u32>>>();

    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let fut = tokio_runtime.spawn(BurstMiddleware::create_proxies::<
        TokioChannelImpl,
        RabbitMQMImpl,
        _,
        _,
    >(
        BurstOptions::new(2, group_ranges, 0.to_string())
            .burst_id("hello_world".to_string())
            .enable_message_chunking(true)
            .message_chunk_size(4 * 1024 * 1024)
            .build(),
        TokioChannelOptions::new()
            .broadcast_channel_size(256)
            .build(),
        RabbitMQOptions::new("amqp://guest:guest@localhost:5672".to_string())
            .durable_queues(true)
            .ack(true)
            .build(),
    ));
    let proxies = tokio_runtime.block_on(fut).unwrap().unwrap();

    let mut actors = proxies
        .into_iter()
        .map(|(worker_id, mid)| {
            let actor = MiddlewareActorHandle::new(mid, &tokio_runtime);
            (worker_id, actor)
        })
        .collect::<HashMap<u32, MiddlewareActorHandle>>();

    let p1 = actors.remove(&0).unwrap();
    let p2 = actors.remove(&1).unwrap();

    let thread_1 = thread::spawn(move || {
        let worker_id = p1.info.worker_id;
        info!("thread start: id={}", worker_id);
        worker(p1).unwrap();
        info!("thread end: id={}", worker_id);
    });

    let thread_2 = thread::spawn(move || {
        let worker_id = p2.info.worker_id;
        info!("thread start: id={}", worker_id);
        worker(p2).unwrap();
        info!("thread end: id={}", worker_id);
    });

    thread_1.join().unwrap();
    thread_2.join().unwrap();
}

fn worker(burst_middleware: MiddlewareActorHandle) -> Result<(), Box<dyn std::error::Error>> {
    info!("hi im worker 1: id={}", burst_middleware.info.worker_id);
    if burst_middleware.info.worker_id == 0 {
        info!("worker {} sending message", burst_middleware.info.worker_id);
        let message = "hello world".to_string();
        let payload = Bytes::from(message);
        burst_middleware.send(1, payload).unwrap();

        let response = burst_middleware.recv(1).unwrap();
        info!(
            "worker {} received message: {:?}, data: {:?}",
            burst_middleware.info.worker_id, response, response.data
        );
    } else {
        let message = burst_middleware.recv(0).unwrap();
        info!(
            "worker {} received message: {:?}, data: {:?}",
            burst_middleware.info.worker_id, message, message.data
        );
        let response = "bye!".to_string();
        let payload = Bytes::from(response);
        burst_middleware.send(0, payload).unwrap();
    }
    Ok(())
}
