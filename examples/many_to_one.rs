use burst_communication_middleware::{create_group_handlers, BurstMiddleware, MiddlewareArguments};
use bytes::Bytes;
use env_logger;
use std::thread;
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

    let mut threads = Vec::with_capacity(BURST_SIZE as usize);
    for group_id in 0..GROUPS {
        let burst_args = MiddlewareArguments::new(
            "dev".to_string(),
            BURST_SIZE,
            GROUPS,
            group_id,
            (group_size * group_id)..((group_size * group_id) + group_size),
            "amqp://rabbit:123456@localhost:5672".to_string(),
            true,
            256,
        );
        let group_threads = group(burst_args).await;
        threads.extend(group_threads);
    }

    for thread in threads {
        thread.join().unwrap();
    }
}

async fn group(burst_args: MiddlewareArguments) -> Vec<std::thread::JoinHandle<()>> {
    let handles = create_group_handlers(burst_args).await.unwrap();

    let mut threads = Vec::with_capacity(handles.len());
    for handle in handles {
        let thread = thread::spawn(move || {
            let thread_id = handle.worker_id;
            // println!("thread start: id={}", thread_id);
            let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            let result = tokio_runtime.block_on(async { worker(handle).await.unwrap() });
            // println!("thread end: id={}", thread_id);
            result
        });
        threads.push(thread);
    }

    return threads;
}
pub async fn worker(
    mut burst_middleware: BurstMiddleware,
) -> Result<(), Box<dyn std::error::Error>> {
    if burst_middleware.worker_id == 0 {
        let mut count = 0;
        loop {
            let msg = burst_middleware.recv().await.unwrap();
            // println!(
            //     "worker {} received message: {:?}",
            //     burst_middleware.worker_id, msg
            // );
            count += 1;
            if count == REPEAT * (BURST_SIZE - 1) {
                break;
            }
        }
        println!(
            "worker {} received a total of {} messages",
            burst_middleware.worker_id, count
        );
    } else {
        for i in 0..REPEAT {
            // println!(
            //     "[worker {}] sending message {}...",
            //     burst_middleware.worker_id, i
            // );
            let message = format!("hello #{} from worker {}", i, burst_middleware.worker_id);
            let payload = Bytes::from(message);
            sleep(Duration::from_secs(1)).await;
            burst_middleware.send(0, payload).await.unwrap();
        }
    }
    println!("worker {} finished", burst_middleware.worker_id);
    Ok(())
}
