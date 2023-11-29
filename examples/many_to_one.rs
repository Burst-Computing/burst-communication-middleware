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
use tokio::time::{sleep, Duration};

const REPEAT: u32 = 3;
const BURST_SIZE: u32 = 4;
const GROUPS: u32 = 2;

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
            "many_to_one".to_string(),
            BURST_SIZE,
            group_ranges.clone(),
            group_id.to_string(),
        );
        let channel_options = TokioChannelOptions::new()
            .broadcast_channel_size(256)
            .build();
        let rabbitmq_options =
            RabbitMQOptions::new("amqp://guest:guest@localhost:5672".to_string())
                .durable_queues(true)
                .ack(true)
                .build();

        let group_threads = group(burst_options, channel_options, rabbitmq_options).await;
        threads.extend(group_threads);
    }

    for thread in threads {
        thread.join().unwrap();
    }
}

async fn group<'a>(
    burst_options: BurstOptions,
    channel_options: TokioChannelOptions,
    rabbitmq_options: RabbitMQOptions,
) -> Vec<std::thread::JoinHandle<()>> {
    let proxies = match BurstMiddleware::create_proxies::<TokioChannelImpl, RabbitMQMImpl, _, _>(
        burst_options,
        channel_options,
        rabbitmq_options,
    )
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
    if burst_middleware.info().worker_id == 0 {
        let mut messages = Vec::new();
        for _ in 0..REPEAT {
            let msgs =
                futures::future::try_join_all((1..burst_middleware.info().burst_size).map(|i| {
                    let burst_middleware = burst_middleware.clone();
                    async move {
                        let msg = burst_middleware.recv(i).await.unwrap();
                        info!(
                            "[worker {}] received message {}",
                            burst_middleware.info().worker_id,
                            i
                        );
                        Ok::<_, Box<dyn std::error::Error>>(msg)
                    }
                }))
                .await?;
            messages.extend(msgs);
        }
        info!(
            "worker {} received a total of {} messages",
            burst_middleware.info().worker_id,
            messages.len()
        );
    } else {
        for i in 0..REPEAT {
            info!(
                "[worker {}] sending message {}...",
                burst_middleware.info().worker_id,
                i
            );
            let message = format!(
                "hello #{} from worker {}",
                i,
                burst_middleware.info().worker_id
            );
            let payload = Bytes::from(message);
            sleep(Duration::from_secs(1)).await;
            match burst_middleware.send(0, payload).await {
                Ok(_) => {}
                Err(e) => {
                    error!("{:?}", e);
                    panic!();
                }
            };
        }
    }
    info!("worker {} finished", burst_middleware.info().worker_id);
    Ok(())
}
