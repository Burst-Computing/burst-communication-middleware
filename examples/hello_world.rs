use burst_communication_middleware::{
    BurstMiddleware, BurstOptions, RabbitMQMImpl, RabbitMQOptions, TokioChannelImpl,
    TokioChannelOptions,
};
use bytes::Bytes;
use log::{error, info};
use std::{
    collections::{HashMap, HashSet},
    thread,
};

#[tokio::main]
async fn main() {
    env_logger::init();

    let group_ranges = vec![(0.to_string(), vec![0, 1].into_iter().collect())]
        .into_iter()
        .collect::<HashMap<String, HashSet<u32>>>();

    let mut proxies =
        match BurstMiddleware::create_proxies::<TokioChannelImpl, RabbitMQMImpl, _, _>(
            BurstOptions::new(
                "hello_world".to_string(),
                2,
                group_ranges,
                0.to_string(),
                true,
                4 * 1024 * 1024,
            ),
            TokioChannelOptions::new()
                .broadcast_channel_size(256)
                .build(),
            RabbitMQOptions::new("amqp://guest:guest@localhost:5672".to_string())
                .durable_queues(true)
                .ack(true)
                .build(),
        )
        .await
        {
            Ok(p) => p,
            Err(e) => {
                error!("{:?}", e);
                panic!();
            }
        };

    let p1 = proxies.remove(&0).unwrap();
    let p2 = proxies.remove(&1).unwrap();

    let thread_1 = thread::spawn(move || {
        let worker_id = p1.info().worker_id;
        info!("thread start: id={}", worker_id);
        let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = tokio_runtime.block_on(async { worker(p1).await.unwrap() });
        info!("thread end: id={}", worker_id);
        result
    });

    let thread_2 = thread::spawn(move || {
        let worker_id = p2.info().worker_id;
        info!("thread start: id={}", worker_id);
        let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = tokio_runtime.block_on(async { worker(p2).await.unwrap() });
        info!("thread end: id={}", worker_id);
        result
    });

    thread_1.join().unwrap();
    thread_2.join().unwrap();
}

pub async fn worker(burst_middleware: BurstMiddleware) -> Result<(), Box<dyn std::error::Error>> {
    info!("hi im worker 1: id={}", burst_middleware.info().worker_id);
    if burst_middleware.info().worker_id == 0 {
        info!(
            "worker {} sending message",
            burst_middleware.info().worker_id
        );
        let message = "hello world".to_string();
        let payload = Bytes::from(message);
        burst_middleware.send(1, payload).await.unwrap();

        let response = burst_middleware.recv(1).await.unwrap();
        info!(
            "worker {} received message: {:?}, data: {:?}",
            burst_middleware.info().worker_id,
            response,
            response.data
        );
    } else {
        let message = burst_middleware.recv(0).await.unwrap();
        info!(
            "worker {} received message: {:?}, data: {:?}",
            burst_middleware.info().worker_id,
            message,
            message.data
        );
        let response = "bye!".to_string();
        let payload = Bytes::from(response);
        burst_middleware.send(0, payload).await.unwrap();
    }
    Ok(())
}
