use std::fs::File;

use lib::BurstMiddleware;
use serde_derive::{Deserialize, Serialize};
use serde_json::{Error, Value};

mod lib;
mod sort;

extern crate serde_json;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Input {
    bucket: String,
    key: String,
    sort_column: i32,
    delimiter: char,
    partitions: i32,
    partition_idx: i32,
    bound: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Output {
    name: String,
}

// main function acts as a wrapper of what the OW runtime would do
fn main() {
    // get input from file, this would be the payload from invokation request of OW
    let file = File::open("sort_payload.json").unwrap();

    // parse JSON into array of Input structs
    let inputs: Vec<Input> = serde_json::from_reader(file).unwrap();
    let mut outputs: Vec<Value> = Vec::new();
    let burst_size = inputs.len().try_into().unwrap();
    // let mut threads = Vec::new();

    let input = inputs[0].clone();
    let bm: BurstMiddleware = BurstMiddleware {
        burst_size: burst_size,
        worker_id: 0,
    };
    let output = ow_main(serde_json::to_value(input).unwrap(), bm).unwrap();
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
pub fn ow_main(args: Value, burst_middleware: BurstMiddleware) -> Result<Value, Error> {
    let input: Input = serde_json::from_value(args)?;

    println!("Input: {:?}", input);

    let output = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(sort::sort(
            burst_middleware,
            input.bucket,
            input.key,
            input.sort_column,
            input.delimiter,
            input.partitions,
            input.partition_idx,
            input.bound,
        ));

    let output = Output { name: output };
    serde_json::to_value(output)
}
