// use aws_config::meta::region::RegionProviderChain;
// use aws_sdk_s3::config::Region;
use bytes::Bytes;
use group_communication_middleware::Middleware;
use polars::prelude::*;
use rusoto_core::{ByteStream, Region};
use rusoto_credential::{EnvironmentProvider, ProvideAwsCredentials};
use rusoto_s3::{GetObjectRequest, HeadBucketRequest, S3Client, S3};
use std::{cmp::min, collections::HashMap, io::Cursor, time::Instant};
use tokio::io::AsyncReadExt;

const PADDING: u32 = 512;

fn trim_chunk(buff: &mut Vec<u8>, range: (u32, u32)) {
    let offset_0: usize = 0;

    if range.0 != 0 {
        // Find the first '\n' character from the beginning
        let offset_0 = match buff.as_slice().iter().position(|&c| c == b'\n') {
            Some(pos) => pos,
            None => 0, // No '\n' found, nothing to trim
        };

        // drain the vector from the beginning to the first '\n' character
        buff.drain(0..offset_0);
    }

    // Calculate the offset in the buff to beginning of the padding
    // accounting for the removed positions form the beginning (offset_0)
    let offset_1: usize = (range.1 - range.0) as usize - offset_0;

    let next_newline = match buff.as_slice()[(offset_1 as usize)..]
        .iter()
        .position(|&c| c == b'\n')
    {
        Some(pos) => pos,
        None => 0, // No '\n' found, nothing to trim
    };

    // drain the buffer from the last newline found in the padding to the end of the buffer
    buff.drain(offset_1 + next_newline..);
}

pub async fn sort(
    burst_middleware: Middleware,
    bucket: String,
    key: String,
    obj_size: u32,
    sort_column: u32,
    delimiter: char,
    num_partitions: u32,
    partition_idx: u32,
    bounds: Vec<String>,
) -> Result<String, Box<dyn std::error::Error>> {
    // let region_provider = RegionProviderChain::first_try(Region::new("us-east-1"));
    // let conf = aws_config::from_env().region(region_provider).load().await;
    // let s3_config_builder =
    // aws_sdk_s3::config::Builder::from(&conf).endpoint_url("http://localhost:9000");
    // let s3_client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());

    // using abandoned rusoto lib because aws sdk beta sucks and does not work with minio
    let client = rusoto_core::request::HttpClient::new().unwrap();
    let region = Region::Custom {
        name: "us-east-1".to_string(),
        endpoint: "http://127.0.0.1:9000".to_string(),
    };
    let creds = EnvironmentProvider::default();
    let s3_client = S3Client::new_with(client, creds, region);

    let partition_size = obj_size / num_partitions;

    let byte_range = (
        partition_idx * partition_size,
        min((partition_idx * partition_size) + partition_size, obj_size),
    );

    println!("Byte range: {:?}", byte_range);

    let get_res = s3_client
        .get_object(GetObjectRequest {
            bucket: bucket,
            key: key,
            range: Some(format!("bytes={}-{}", byte_range.0, byte_range.1 + PADDING)),
            ..Default::default()
        })
        .await
        .unwrap();

    let mut buffer: Vec<u8> = Vec::with_capacity((obj_size + PADDING) as usize);
    let mut reader = get_res.body.unwrap().into_async_read();
    while let Ok(sz) = reader.read_buf(&mut buffer).await {
        if sz == 0 {
            break;
        }
    }

    // print buffer size
    println!("Buffer size: {}", buffer.len());

    trim_chunk(&mut buffer, byte_range);

    println!("Buffer size: {}", buffer.len());

    let cursor = Cursor::new(buffer);

    let df: DataFrame = CsvReader::new(cursor)
        .infer_schema(Some(1))
        .has_header(false)
        .finish()
        .unwrap();
    let column_name = df.get_column_names()[sort_column as usize];

    // select column as series
    let search_start_t = Instant::now();

    // save index in a hashmap
    let mut partitions: HashMap<u32, Vec<u32>> = HashMap::new();

    for (idx, value) in df[column_name].iter().enumerate() {
        // println!("{}", x);
        let res = match value {
            AnyValue::Utf8(s) => bounds.binary_search(&s.to_string()),
            _ => panic!("Not a string"),
        };
        match res {
            Ok(x) => {
                partitions
                    .entry(x as u32)
                    .or_insert(Vec::new())
                    .push(idx as u32);
            }
            Err(x) => {
                partitions
                    .entry(x as u32)
                    .or_insert(Vec::new())
                    .push(idx as u32);
            }
        };
        // println!("{:?}", res);
    }
    let search_duration = search_start_t.elapsed();
    println!("Search time: {:?}", search_duration);

    for (partition, indexes) in partitions.iter() {
        let a = ChunkedArray::from_vec("partition", indexes.clone());
        let mut partition_df = df.take(&a).unwrap();

        let mut buf = Vec::new();
        let write_start_t = Instant::now();
        CsvWriter::new(&mut buf)
            .has_header(false)
            .finish(&mut partition_df)
            .unwrap();
        let write_duration = write_start_t.elapsed();
        println!("Write time: {:?}", write_duration);

        let res = burst_middleware.send(*partition, Bytes::from(buf)).await;
        match res {
            Ok(_) => println!("Sent"),
            Err(_) => println!("Error"),
        }
    }

    let agg_df = DataFrame::from_rows_and_schema(&[], &df.schema()).unwrap();
    for _ in 0..num_partitions {
        let res = burst_middleware.recv().await;
        match res {
            Ok(message) => {
                println!("Received message: {:?}", message);
                let cursor = Cursor::new(message.data);
                let mut partition_df: DataFrame = CsvReader::new(cursor)
                    .infer_schema(Some(1))
                    .has_header(false)
                    .finish()
                    .unwrap();
                agg_df.vstack(&mut partition_df).unwrap();
            }
            Err(_) => println!("Error"),
        }
    }

    let sort_options = SortOptions {
        descending: false,
        nulls_last: true,
        multithreaded: false,
        maintain_order: true,
    };

    let sort_start_t = Instant::now();
    let mut agg_df = agg_df.sort_with_options(column_name, sort_options).unwrap();
    let sort_duration = sort_start_t.elapsed();
    println!("Sort time: {:?}", sort_duration);

    // iterate over rows to check if the dataframe is sorted
    // let column = df.get(sort_column as usize).unwrap();
    // let mut prev = column.get(0).unwrap().clone();
    // for row in df
    //     .get(sort_column as usize)
    //     .unwrap()
    //     .get(1..)
    //     .unwrap()
    //     .iter()
    // {
    //     let curr = row;
    //     if prev > *curr {
    //         println!("Not sorted");
    //         break;
    //     }
    //     prev = curr.clone();
    // }

    // get column as series
    // let column = df.get(sort_column as usize).unwrap();
    // column.binary_search();

    let mut buf = Vec::new();
    let write_start_t = Instant::now();
    CsvWriter::new(&mut buf)
        .has_header(false)
        .finish(&mut agg_df)
        .unwrap();
    let write_duration = write_start_t.elapsed();
    println!("Write time: {:?}", write_duration);

    return Ok(String::from("Hello"));
    // Ok(String::from("Hello")
}
