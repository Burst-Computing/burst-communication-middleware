use core::panic;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use tokio::sync::Mutex;

use async_trait::async_trait;

use aws_config::{meta::region::RegionProviderChain, BehaviorVersion, Region};
use aws_credential_types::Credentials;

use aws_sdk_s3::{primitives::ByteStream, Client};
use bytes::Bytes;

use crate::{
    impl_chainable_setter, BroadcastSendProxy, BurstOptions, CollectiveType, Message, ReceiveProxy,
    Result, SendProxy, SendReceiveFactory, SendReceiveProxy,
};

#[derive(Clone, Debug)]
pub struct S3Options {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub region: String,
    pub endpoint: Option<String>,
    pub bucket: String,
    pub prefix: String,
    pub wait_time: f64,
    pub enable_broadcast: bool,
}

impl S3Options {
    pub fn new(bucket: String) -> Self {
        Self {
            bucket,
            ..Default::default()
        }
    }

    impl_chainable_setter! {
        access_key_id, String
    }

    impl_chainable_setter! {
        secret_access_key, String
    }

    impl_chainable_setter! {
        session_token, Option<String>
    }

    impl_chainable_setter! {
        region, String
    }

    impl_chainable_setter! {
        endpoint, Option<String>
    }

    impl_chainable_setter! {
        bucket, String
    }

    impl_chainable_setter! {
        prefix, String
    }

    impl_chainable_setter! {
        wait_time, f64
    }

    impl_chainable_setter! {
        enable_broadcast, bool
    }

    pub fn build(&self) -> Self {
        self.clone()
    }
}

impl Default for S3Options {
    fn default() -> Self {
        Self {
            access_key_id: "minioadmin".into(),
            secret_access_key: "minioadmin".into(),
            session_token: None,
            region: "us-east-1".into(),
            endpoint: Some("http://localhost:9000".into()),
            bucket: "burst-middleware".into(),
            prefix: "dev".into(),
            wait_time: 0.2,
            enable_broadcast: false,
        }
    }
}

pub struct S3Impl;

#[async_trait]
impl SendReceiveFactory<S3Options> for S3Impl {
    async fn create_proxies(
        burst_options: Arc<BurstOptions>,
        s3_options: S3Options,
        broadcast_proxy: Box<dyn BroadcastSendProxy>,
    ) -> Result<(
        HashMap<u32, Box<dyn SendReceiveProxy>>,
        Box<dyn BroadcastSendProxy>,
    )> {
        let credentials_provider = Credentials::from_keys(
            s3_options.access_key_id.clone(),
            s3_options.secret_access_key.clone(),
            s3_options.session_token.clone(),
        );
        let region_provider = RegionProviderChain::first_try(Region::new("us-east-1"));

        let config = match s3_options.endpoint.clone() {
            Some(endpoint) => {
                println!("Using endpoint: {}", endpoint);
                aws_config::defaults(BehaviorVersion::latest())
                    .region(region_provider)
                    .endpoint_url(endpoint)
                    .credentials_provider(credentials_provider)
                    .load()
                    .await
            }
            None => {
                aws_config::defaults(BehaviorVersion::latest())
                    .region(region_provider)
                    .credentials_provider(credentials_provider)
                    .load()
                    .await
            } // do nothing
        };

        let s3_client = Client::new(&config);

        let bucket = s3_client
            .head_bucket()
            .bucket(s3_options.bucket.clone())
            .send()
            .await?;
        log::debug!("Bucket: {:?}", bucket);

        let s3_options = Arc::new(s3_options);
        let current_group = burst_options
            .group_ranges
            .get(&burst_options.group_id)
            .unwrap();

        let mut hmap = HashMap::new();

        for worker_id in current_group {
            let p = S3Proxy::new(
                s3_client.clone(),
                s3_options.clone(),
                burst_options.clone(),
                *worker_id,
            );
            hmap.insert(*worker_id, Box::new(p) as Box<dyn SendReceiveProxy>);
        }

        if s3_options.enable_broadcast {
            let s3 = s3_client.clone();
            let burst_id = burst_options.burst_id.clone();
            let bucket = s3_options.bucket.clone();
            let wait_time = s3_options.wait_time.clone();
            tokio::spawn(async move {
                let mut received_messages: HashSet<String> = HashSet::new();
                loop {
                    log::debug!("Listing broadcast keys...");
                    let list_response = s3
                        .list_objects_v2()
                        .bucket(bucket.clone())
                        .prefix(format!("{}/broadcast/", burst_id))
                        .send()
                        .await
                        .unwrap();

                    let keys = list_response
                        .contents
                        .unwrap()
                        .iter()
                        .map(|obj| obj.key.clone().unwrap())
                        .collect::<HashSet<String>>();

                    let new_keys: HashSet<String> =
                        keys.difference(&received_messages).cloned().collect();

                    if new_keys.is_empty() {
                        log::debug!("No new broadcast keys found, sleeping");
                        tokio::time::sleep(tokio::time::Duration::from_secs_f64(wait_time)).await;
                        continue;
                    }

                    log::debug!("Found {} new keys for broadcast", new_keys.len());

                    for new_key in new_keys {
                        let obj = s3
                            .get_object()
                            .bucket(bucket.clone())
                            .key(new_key.clone())
                            .send()
                            .await
                            .unwrap();

                        let (sender_id, counter) = match obj.metadata() {
                            Some(metadata) => {
                                let sender_id = match metadata.get("sender_id") {
                                    Some(sender_id) => match sender_id.parse::<u32>() {
                                        Ok(sender_id) => sender_id,
                                        Err(err) => {
                                            log::error!("Failed to parse sender_id: {}", err);
                                            continue;
                                        }
                                    },
                                    None => {
                                        log::error!(
                                            "No sender_id found in metadata for key: {}",
                                            new_key
                                        );
                                        continue;
                                    }
                                };
                                let counter = match metadata.get("counter") {
                                    Some(counter) => match counter.parse::<u32>() {
                                        Ok(counter) => counter,
                                        Err(err) => {
                                            log::error!("Failed to parse counter: {}", err);
                                            continue;
                                        }
                                    },
                                    None => {
                                        log::error!(
                                            "No counter found in metadata for key: {}",
                                            new_key
                                        );
                                        continue;
                                    }
                                };
                                (sender_id, counter)
                            }
                            None => {
                                log::error!("No metadata found");
                                continue;
                            }
                        };
                        let bytes = obj.body.collect().await.unwrap().into_bytes();

                        log::debug!(
                            "Got object with key {} from {} with size {}",
                            new_key,
                            sender_id,
                            bytes.len()
                        );

                        let msg = Message {
                            sender_id: sender_id,
                            chunk_id: 0,
                            num_chunks: 1,
                            counter: counter,
                            collective: CollectiveType::Broadcast,
                            data: Bytes::from(bytes),
                        };
                        broadcast_proxy.broadcast_send(&msg).await.unwrap();
                        received_messages.insert(new_key);
                    }
                }
            });
        }

        Ok((
            hmap,
            Box::new(S3BroadcastSendProxy::new(
                s3_client,
                s3_options,
                burst_options,
            )),
        ))
    }
}

pub struct S3Proxy {
    receiver: Box<dyn ReceiveProxy>,
    sender: Box<dyn SendProxy>,
}

pub struct S3SendProxy {
    s3_client: Client,
    s3_options: Arc<S3Options>,
    burst_options: Arc<BurstOptions>,
    worker_id: u32,
}

pub struct S3ReceiveProxy {
    s3_client: Client,
    s3_options: Arc<S3Options>,
    burst_options: Arc<BurstOptions>,
    worker_id: u32,
    keys: Arc<Mutex<Vec<String>>>,
}

pub struct S3BroadcastSendProxy {
    _s3_client: Client,
    _s3_options: Arc<S3Options>,
    _burst_options: Arc<BurstOptions>,
}

impl SendReceiveProxy for S3Proxy {}

#[async_trait]
impl SendProxy for S3Proxy {
    async fn send(&self, dest: u32, msg: &Message) -> Result<()> {
        self.sender.send(dest, msg).await
    }
}

#[async_trait]
impl ReceiveProxy for S3Proxy {
    async fn recv(&self) -> Result<Message> {
        self.receiver.recv().await
    }
}

impl S3Proxy {
    pub fn new(
        s3_client: Client,
        s3_options: Arc<S3Options>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
    ) -> Self {
        Self {
            sender: Box::new(S3SendProxy::new(
                s3_client.clone(),
                s3_options.clone(),
                burst_options.clone(),
                worker_id,
            )),
            receiver: Box::new(S3ReceiveProxy::new(
                s3_client.clone(),
                s3_options.clone(),
                burst_options.clone(),
                worker_id,
            )),
        }
    }
}

impl S3SendProxy {
    pub fn new(
        s3_client: Client,
        s3_options: Arc<S3Options>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
    ) -> Self {
        Self {
            s3_client,
            s3_options,
            burst_options,
            worker_id,
        }
    }
}

#[async_trait]
impl SendProxy for S3SendProxy {
    async fn send(&self, dest: u32, msg: &Message) -> Result<()> {
        let byte_stream = ByteStream::from(msg.data.clone());
        let key = format!(
            "{}/worker-{}/{}",
            self.burst_options.burst_id,
            dest,
            uuid::Uuid::new_v4().to_string()
        );
        log::debug!("Send key: {}", key);
        self.s3_client
            .put_object()
            .bucket(self.s3_options.bucket.clone())
            .key(key)
            .body(byte_stream)
            .metadata("sender_id", self.worker_id.to_string())
            .metadata("collective", msg.collective.to_string())
            .metadata("counter", msg.counter.to_string())
            .send()
            .await?;
        Ok(())
    }
}

impl S3ReceiveProxy {
    pub fn new(
        s3_client: Client,
        s3_options: Arc<S3Options>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
    ) -> Self {
        Self {
            s3_client,
            s3_options,
            burst_options,
            worker_id,
            keys: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl ReceiveProxy for S3ReceiveProxy {
    async fn recv(&self) -> Result<Message> {
        loop {
            let key = get_next_object_key(self).await.unwrap();

            log::debug!("Fetch key: {}", key);
            let obj = self
                .s3_client
                .get_object()
                .bucket(self.s3_options.bucket.clone())
                .key(key.clone())
                .send()
                .await?;

            let (sender_id, collective_type, counter) = match obj.metadata() {
                Some(metadata) => {
                    let sender_id = match metadata.get("sender_id") {
                        Some(sender_id) => match sender_id.parse::<u32>() {
                            Ok(sender_id) => sender_id,
                            Err(err) => {
                                log::error!("Failed to parse sender_id: {}", err);
                                continue;
                            }
                        },
                        None => {
                            log::error!("No sender_id found in metadata for key: {}", key);
                            continue;
                        }
                    };
                    let collective_type = match metadata.get("collective") {
                        Some(collective) => match collective.as_str() {
                            "Direct" => CollectiveType::Direct,
                            "Broadcast" => CollectiveType::Broadcast,
                            "Scatter" => CollectiveType::Scatter,
                            "Gather" => CollectiveType::Gather,
                            "AllToAll" => CollectiveType::AllToAll,
                            _ => {
                                log::error!("Invalid collective type: {}", collective);
                                continue;
                            }
                        },
                        None => {
                            log::error!("No collective found in metadata for key: {}", key);
                            continue;
                        }
                    };
                    let counter = match metadata.get("counter") {
                        Some(counter) => match counter.parse::<u32>() {
                            Ok(counter) => counter,
                            Err(err) => {
                                log::error!("Failed to parse counter: {}", err);
                                continue;
                            }
                        },
                        None => {
                            log::error!("No counter found in metadata for key: {}", key);
                            continue;
                        }
                    };
                    (sender_id, collective_type, counter)
                }
                None => {
                    log::error!("No metadata found");
                    continue;
                }
            };
            let bytes = obj.body.collect().await?.into_bytes();

            log::debug!("Got object from {} with size {}", sender_id, bytes.len());

            self.s3_client
                .delete_object()
                .bucket(self.s3_options.bucket.clone())
                .key(key)
                .send()
                .await?;

            let msg = Message {
                sender_id: sender_id,
                chunk_id: 0,
                num_chunks: 1,
                counter: counter,
                collective: collective_type,
                data: Bytes::from(bytes),
            };
            return Ok(msg);
        }
    }
}

impl S3BroadcastSendProxy {
    pub fn new(
        s3_client: Client,
        s3_options: Arc<S3Options>,
        burst_options: Arc<BurstOptions>,
    ) -> Self {
        Self {
            _s3_client: s3_client,
            _s3_options: s3_options,
            _burst_options: burst_options,
        }
    }
}

#[async_trait]
impl BroadcastSendProxy for S3BroadcastSendProxy {
    async fn broadcast_send(&self, msg: &Message) -> Result<()> {
        if !self._s3_options.enable_broadcast {
            panic!("Broadcast not enabled");
        }

        let key = format!(
            "{}/broadcast/{}",
            self._burst_options.burst_id,
            uuid::Uuid::new_v4().to_string()
        );
        self._s3_client
            .put_object()
            .bucket(self._s3_options.bucket.clone())
            .key(key)
            .body(ByteStream::from(msg.data.clone()))
            .metadata("sender_id", msg.sender_id.to_string())
            .metadata("collective", msg.collective.to_string())
            .metadata("counter", msg.counter.to_string())
            .send()
            .await?;
        Ok(())
    }
}

async fn get_next_object_key(proxy: &S3ReceiveProxy) -> Result<String> {
    while proxy.keys.lock().await.is_empty() {
        let s3_keys: aws_sdk_s3::operation::list_objects::ListObjectsOutput = proxy
            .s3_client
            .list_objects()
            .bucket(proxy.s3_options.bucket.clone())
            .prefix(format!(
                "{}/worker-{}/",
                proxy.burst_options.burst_id, proxy.worker_id
            ))
            .send()
            .await?;

        match s3_keys.contents {
            Some(contents) => {
                log::debug!("Listed {} keys", contents.len());
                for object in contents {
                    let key = match object.key {
                        Some(key) => key,
                        None => {
                            log::error!("No key found in S3");
                            continue;
                        }
                    };
                    proxy.keys.lock().await.push(key);
                }
            }
            None => {
                log::debug!(
                    "No keys found, sleeping for {} seconds",
                    proxy.s3_options.wait_time
                );
                tokio::time::sleep(tokio::time::Duration::from_secs_f64(
                    proxy.s3_options.wait_time,
                ))
                .await;
            }
        }
    }

    let key = proxy.keys.lock().await.pop().unwrap();
    return Ok(key);
}
