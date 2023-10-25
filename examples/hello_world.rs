use burst_communication_middleware::{create_group_handlers, BurstMiddleware, MiddlewareArguments};
use bytes::Bytes;
use std::error::Error;
use std::thread;

#[tokio::main]
async fn main() {
    env_logger::init();

    let burst_args = MiddlewareArguments::new(
        "dev".to_string(),
        2,
        1,
        0,
        0..2,
        "amqp://rabbit:123456@localhost:5672".to_string(),
        true,
        256,
    );
    let mut handles = create_group_handlers(burst_args).await.unwrap();
    let h1 = handles.pop().unwrap();
    let h2 = handles.pop().unwrap();

    let thread_1 = thread::spawn(move || {
        println!("thread start: id={}", h1.worker_id);
        let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = tokio_runtime.block_on(async { worker(h1).await.unwrap() });
        // println!("thread end: id={}", handle.worker_id);
        result
    });

    let thread_2 = thread::spawn(move || {
        println!("thread start: id={}", h2.worker_id);
        let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = tokio_runtime.block_on(async { worker(h2).await.unwrap() });
        // println!("thread end: id={}", handle.worker_id);
        result
    });

    thread_1.join().unwrap();
    thread_2.join().unwrap();
}

pub async fn worker(
    mut burst_middleware: BurstMiddleware,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("hi im worker 1: id={}", burst_middleware.worker_id);
    if burst_middleware.worker_id == 0 {
        println!("worker {} sending message", burst_middleware.worker_id);
        let message = "hello world".to_string();
        let payload = Bytes::from(message);
        burst_middleware.send(1, payload).await.unwrap();

        let response = burst_middleware.recv().await.unwrap();
        println!(
            "worker {} received message: {:?}",
            burst_middleware.worker_id, response
        );
    } else {
        let message = burst_middleware.recv().await.unwrap();
        println!(
            "worker {} received message: {:?}",
            burst_middleware.worker_id, message
        );
        let response = "bye!".to_string();
        let payload = Bytes::from(response);
        burst_middleware.send(0, payload).await.unwrap();
    }
    Ok(())
}
