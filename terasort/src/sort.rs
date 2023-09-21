use std::cmp::min;

use polars::prelude::{CsvReader, SortOptions};

use crate::lib::BurstMiddleware;

// pub fn testing() {
//     let s = r#"
//         sepal.length,sepal.width,petal.length,petal.width,variety
//         5.1,3.5,1.4,.2,Setosa
//         4.9,3,1.4,.2,Setosa
//         4.7,3.2,1.3,.2,Setosa"#;

//     let cursor = std::io::Cursor::new(s);

//     let df = CsvReader::new(cursor)
//         .infer_schema(Some(3))
//         .has_header(true)
//         .finish()
//         .unwrap();

//     print!("{:?}", df);

//     // // read csv from file
//     // let df = CsvReader::from_path("/media/aitor/SSDUSB/archive/custom_1988_2020.csv")
//     //     .unwrap()
//     //     .infer_schema(Some(1))
//     //     .has_header(false)
//     //     .finish()
//     //     .unwrap();

//     // // print dataframe schema
//     // println!("{:?}", df);

//     // let options = SortOptions {
//     //     descending: false,
//     //     nulls_last: true,
//     //     multithreaded: false,
//     //     maintain_order: true,
//     // };

//     // // print time to sort
//     // let t0 = std::time::Instant::now();
//     // let _ = df.sort_with_options("column_5", options);
//     // let duration = t0.elapsed();
//     // println!("Time elapsed is: {:?}", duration);
// }

pub async fn sort(
    burst_middleware: BurstMiddleware,
    bucket: String,
    key: String,
    sort_column: i32,
    delimiter: char,
    partitions: i32,
    partition_idx: i32,
    bound: String,
) -> String {
    // let conf = aws_config::from_env().load().await;
    // let s3_config_builder =
    //     aws_sdk_s3::config::Builder::from(&conf).endpoint_url("http://localhost:9000");
    // let s3_client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());
    // if worker_id == 0 {
    // } else {
    // }
    // let head_response = s3_client
    //     .head_object()
    //     .bucket(bucket_name)
    //     .key(object_key)
    //     .send()
    //     .await;

    // let obj_size = head_response.unwrap().content_length as u32;

    // println!("Object size: {}", obj_size);

    // let partition_size = obj_size / burst_size;

    // let lower_bound = worker_id * partition_size;
    // let upper_bound = min(lower_bound + partition_size + 1, obj_size);

    // println!(
    //     "Scanning bytes={}-{} ({})",
    //     lower_bound,
    //     upper_bound,
    //     upper_bound - lower_bound
    // );

    // let get_object_res = s3_client
    //     .get_object()
    //     .bucket(bucket_name)
    //     .key(object_key)
    //     .range(format!("bytes={}-{}", lower_bound, upper_bound))
    //     .send()
    //     .await;

    // let body = get_object_res
    //     .unwrap()
    //     .body
    //     .collect()
    //     .await
    //     .unwrap()
    //     .into_bytes();

    // let body_cursor = std::io::Cursor::new(body);

    // let df = CsvReader::new(body_cursor)
    //     .infer_schema(Some(1))
    //     .has_header(false)
    //     .finish()
    //     .unwrap();

    String::from("Hello")
}
