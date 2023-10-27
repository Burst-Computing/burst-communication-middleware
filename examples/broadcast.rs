use burst_communication_middleware::{
    BurstMiddleware, BurstOptions, Message, RabbitMQMImpl, RabbitMQOptions,
};
use bytes::Bytes;
use log::{error, info};
use std::{
    collections::{HashMap, HashSet},
    thread,
};

const BURST_SIZE: u32 = 64;
const GROUPS: u32 = 4;

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
        )
        .broadcast_channel_size(256)
        .build();

        let rabbitmq_options =
            RabbitMQOptions::new("amqp://guest:guest@localhost:5672".to_string())
                .durable_queues(true)
                .ack(true)
                .build();

        let group_threads = group(burst_options, rabbitmq_options).await;
        threads.extend(group_threads);
    }

    for thread in threads {
        thread.join().unwrap();
    }
}

async fn group(
    burst_options: BurstOptions,
    rabbitmq_options: RabbitMQOptions,
) -> Vec<std::thread::JoinHandle<()>> {
    let proxies =
        match BurstMiddleware::create_proxies::<RabbitMQMImpl, _>(burst_options, rabbitmq_options)
            .await
        {
            Ok(p) => p,
            Err(e) => {
                error!("{:?}", e);
                panic!();
            }
        };

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
        let msg = "hello world";
        let data = Bytes::from(msg);
        res = burst_middleware.broadcast(Some(data)).await.unwrap();
    } else {
        res = burst_middleware.broadcast(None).await.unwrap();
    }
    info!(
        "worker {} => broadcast result: {:?}",
        burst_middleware.info().worker_id,
        res
    );
    Ok(())
}
