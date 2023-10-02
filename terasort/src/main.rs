use std::fs::File;

use group_communication_middleware::{Middleware, MiddlewareArguments};
use serde_derive::{Deserialize, Serialize};
use serde_json::{Error, Value};
mod sort;

extern crate serde_json;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Input {
    bucket: String,
    key: String,
    obj_size: u32,
    sort_column: u32,
    delimiter: char,
    partitions: u32,
    partition_idx: u32,
    segment_bounds: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Output {
    name: String,
}

// main function acts as a wrapper of what the OW runtime would do
#[tokio::main]
async fn main() {
    // get input from file, this would be the payload from invokation request of OW
    let file = File::open("sort_payload.json").unwrap();

    // parse JSON into array of Input structs
    let inputs: Vec<Input> = serde_json::from_reader(file).unwrap();
    let mut outputs: Vec<Value> = Vec::new();
    // let mut threads = Vec::new();

    let input = inputs[0].clone();
    let middleware = Middleware::init_global(MiddlewareArguments::new(
        "amqp://rabbit:123456@localhost:5672".to_string(),
        0..3,
        0..3,
    ))
    .await
    .unwrap();

    let output = ow_main(serde_json::to_value(input).unwrap(), middleware)
        .await
        .unwrap();
    outputs.push(output);

    // for (idx, input) in inputs.iter().enumerate() {
    //     let input = input.clone();
    //     let bm: BurstMiddleware = BurstMiddleware {
    //         burst_size: burst_size,
    //         worker_id: idx.try_into().unwrap(),
    //     };
    //     let t = thread::spawn(move || {
    //         return ow_main(
    //             serde_json::to_value(input).unwrap(),
    //             bm,
    //         );
    //     });
    //     threads.push(t);
    // }

    // for t in threads {
    //     let result = t.join().unwrap();
    //     match result {
    //         Ok(output) => outputs.push(output),
    //         Err(_) => {
    //             println!("Error");
    //         }
    //     }
    // }

    // write output to file, this would be the response of OW invokation
    let file = File::create("output.json").unwrap();
    serde_json::to_writer(file, &outputs).unwrap();
}

// ow_main would be the entry point of an actual open whisk burst worker
pub async fn ow_main(args: Value, burst_middleware: Middleware) -> Result<Value, Error> {
    let input: Input = serde_json::from_value(args)?;

    println!("{:?}", input);

    let output = sort::sort(
        burst_middleware,
        input.bucket,
        input.key,
        input.obj_size,
        input.sort_column,
        input.delimiter,
        input.partitions,
        input.partition_idx,
        input.segment_bounds,
    )
    .await
    .unwrap();

    let output: Output = Output { name: output };
    println!("{:?}", output);

    serde_json::to_value(output)
}
