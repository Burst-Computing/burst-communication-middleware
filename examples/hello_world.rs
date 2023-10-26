use burst_communication_middleware::{
    BurstMiddleware, BurstOptions, RabbitMQMiddleware, RabbitMQOptions,
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

    let burst_options =
        BurstOptions::new("hello_world".to_string(), 2, group_ranges, 0.to_string())
            .broadcast_channel_size(256)
            .build();

    let rabbitmq_options = RabbitMQOptions::new("amqp://guest:guest@localhost:5672".to_string())
        .durable_queues(true)
        .ack(true)
        .build();

    let rabbitmq_middleware =
        match RabbitMQMiddleware::new(burst_options.clone(), rabbitmq_options).await {
            Ok(m) => m,
            Err(e) => {
                error!("{:?}", e);
                panic!();
            }
        };

    let mut proxies =
        match BurstMiddleware::create_proxies(burst_options, rabbitmq_middleware).await {
            Ok(p) => p,
            Err(e) => {
                error!("{:?}", e);
                panic!();
            }
        };

    let p1 = proxies.remove(&0).unwrap();
    let p2 = proxies.remove(&1).unwrap();

    let thread_1 = thread::spawn(move || {
        info!("thread start: id={}", 0);
        let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = tokio_runtime.block_on(async { worker(p1).await.unwrap() });
        // println!("thread end: id={}", handle.worker_id);
        result
    });

    let thread_2 = thread::spawn(move || {
        info!("thread start: id={}", 0);
        let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = tokio_runtime.block_on(async { worker(p2).await.unwrap() });
        // println!("thread end: id={}", handle.worker_id);
        result
    });

    thread_1.join().unwrap();
    thread_2.join().unwrap();
}

pub async fn worker(burst_middleware: BurstMiddleware) -> Result<(), Box<dyn std::error::Error>> {
    println!("hi im worker 1: id={}", burst_middleware.info().worker_id);
    if burst_middleware.info().worker_id == 0 {
        println!(
            "worker {} sending message",
            burst_middleware.info().worker_id
        );
        let message = "hello world".to_string();
        let payload = Bytes::from(message);
        burst_middleware.send(1, payload).await.unwrap();

        let response = burst_middleware.recv().await.unwrap();
        println!(
            "worker {} received message: {:?}",
            burst_middleware.info().worker_id,
            response
        );
    } else {
        let message = burst_middleware.recv().await.unwrap();
        println!(
            "worker {} received message: {:?}",
            burst_middleware.info().worker_id,
            message
        );
        let response = "bye!".to_string();
        let payload = Bytes::from(response);
        burst_middleware.send(0, payload).await.unwrap();
    }
    Ok(())
}
