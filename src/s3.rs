use core::panic;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use tokio::sync::Mutex;

use async_trait::async_trait;

use aws_config::Region;
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

    impl_chainable_setter!(access_key_id, String);
    impl_chainable_setter!(secret_access_key, String);
    impl_chainable_setter!(session_token, Option<String>);
    impl_chainable_setter!(region, String);
    impl_chainable_setter!(endpoint, Option<String>);
    impl_chainable_setter!(bucket, String);
    impl_chainable_setter!(prefix, String);
    impl_chainable_setter!(wait_time, f64);
    impl_chainable_setter!(enable_broadcast, bool);

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

        let config = match s3_options.endpoint.clone() {
            Some(endpoint) => {
                aws_sdk_s3::config::Builder::new()
                    .endpoint_url(endpoint)
                    .credentials_provider(credentials_provider)
                    .region(Region::new(s3_options.region.clone()))
                    .force_path_style(true) // apply bucketname as path param instead of pre-domain
                    .build()
            }
            None => aws_sdk_s3::config::Builder::new()
                .credentials_provider(credentials_provider)
                .region(Region::new(s3_options.region.clone()))
                .build(),
        };
        let bcast_config = config.clone();
        let s3_client = Client::from_conf(config.clone());

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
                Client::from_conf(config.clone()),
                s3_options.clone(),
                burst_options.clone(),
                *worker_id,
            );
            hmap.insert(*worker_id, Box::new(p) as Box<dyn SendReceiveProxy>);
        }

        if s3_options.enable_broadcast {
            tokio::spawn(broadcast_loop(
                burst_options.clone(),
                s3_options.clone(),
                s3_client,
                broadcast_proxy,
            ));
        }

        Ok((
            hmap,
            Box::new(S3BroadcastSendProxy::new(
                Client::from_conf(bcast_config),
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

pub struct Keys {
    pending: Vec<String>, // keys not yet processed, only contains keys that have not been seen
    seen: HashSet<String>, // keys that have been seen, may or may not have been processed
}

pub struct S3ReceiveProxy {
    s3_client: Client,
    s3_options: Arc<S3Options>,
    burst_options: Arc<BurstOptions>,
    worker_id: u32,
    keys: Arc<Mutex<Keys>>,
}

pub struct S3BroadcastSendProxy {
    _s3_client: Client,
    _s3_options: Arc<S3Options>,
    _burst_options: Arc<BurstOptions>,
}

impl SendReceiveProxy for S3Proxy {}

#[async_trait]
impl SendProxy for S3Proxy {
    async fn send(&self, dest: u32, msg: Message) -> Result<()> {
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
    async fn send(&self, dest: u32, msg: Message) -> Result<()> {
        let byte_stream = ByteStream::from(msg.data.clone());
        let key = format_message_key(&self.burst_options.burst_id, dest, &msg);
        self.s3_client
            .put_object()
            .bucket(self.s3_options.bucket.clone())
            .key(key.clone())
            .body(byte_stream)
            .metadata("sender_id", self.worker_id.to_string())
            .metadata("collective", msg.collective.to_string())
            .metadata("counter", msg.counter.to_string())
            .metadata("chunk_id", msg.chunk_id.to_string())
            .metadata("num_chunks", msg.num_chunks.to_string())
            .send()
            .await?;
        log::debug!("S3 Put object {}", key);
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
            keys: Arc::new(Mutex::new(Keys {
                pending: Vec::new(),
                seen: HashSet::new(),
            })),
        }
    }
}

#[async_trait]
impl ReceiveProxy for S3ReceiveProxy {
    async fn recv(&self) -> Result<Message> {
        loop {
            let key = get_next_object_key(self).await.unwrap();

            log::debug!("S3 Get object with key {}...", key);
            let obj = self
                .s3_client
                .get_object()
                .bucket(self.s3_options.bucket.clone())
                .key(key.clone())
                .send()
                .await?;

            let (sender_id, collective_type, counter, chunk_id, num_chunks) = match obj.metadata() {
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
                    let chunk_id = match metadata.get("chunk_id") {
                        Some(chunk_id) => match chunk_id.parse::<u32>() {
                            Ok(chunk_id) => chunk_id,
                            Err(err) => {
                                log::error!("Failed to parse chunk_id: {}", err);
                                continue;
                            }
                        },
                        None => {
                            log::error!("No chunk_id found in metadata for key: {}", key);
                            continue;
                        }
                    };
                    let num_chunks = match metadata.get("num_chunks") {
                        Some(num_chunks) => match num_chunks.parse::<u32>() {
                            Ok(num_chunks) => num_chunks,
                            Err(err) => {
                                log::error!("Failed to parse num_chunks: {}", err);
                                continue;
                            }
                        },
                        None => {
                            log::error!("No num_chunks found in metadata for key: {}", key);
                            continue;
                        }
                    };
                    (sender_id, collective_type, counter, chunk_id, num_chunks)
                }
                None => {
                    log::error!("No metadata found");
                    continue;
                }
            };
            let bytes = obj.body.collect().await?.into_bytes();

            log::debug!("S3 Got object {} with size {}", key, bytes.len());

            self.s3_client
                .delete_object()
                .bucket(self.s3_options.bucket.clone())
                .key(key)
                .send()
                .await?;

            let msg = Message {
                sender_id,
                chunk_id,
                num_chunks,
                counter,
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
    async fn broadcast_send(&self, msg: Message) -> Result<()> {
        if !self._s3_options.enable_broadcast {
            panic!("Broadcast not enabled");
        }

        let key = format!(
            "{}/broadcast/sender-{}/counter-{}/part-{}",
            self._burst_options.burst_id, msg.sender_id, msg.counter, msg.chunk_id
        );
        self._s3_client
            .put_object()
            .bucket(self._s3_options.bucket.clone())
            .key(key)
            .body(ByteStream::from(msg.data.clone()))
            .metadata("sender_id", msg.sender_id.to_string())
            .metadata("collective", msg.collective.to_string())
            .metadata("counter", msg.counter.to_string())
            .metadata("chunk_id", msg.chunk_id.to_string())
            .metadata("num_chunks", msg.num_chunks.to_string())
            .send()
            .await?;
        Ok(())
    }
}

async fn broadcast_loop(
    burst_options: Arc<BurstOptions>,
    s3_options: Arc<S3Options>,
    s3_client: Client,
    broadcast_proxy: Box<dyn BroadcastSendProxy>,
) {
    let mut received_messages: HashSet<String> = HashSet::new();
    loop {
        log::debug!("Listing broadcast keys...");
        let list_response = s3_client
            .list_objects_v2()
            .bucket(s3_options.bucket.clone())
            .prefix(format!("{}/broadcast/", burst_options.burst_id))
            .send()
            .await
            .unwrap();

        let keys = list_response
            .contents
            .unwrap()
            .iter()
            .map(|obj| obj.key.clone().unwrap())
            .collect::<HashSet<String>>();

        let new_keys: HashSet<String> = keys.difference(&received_messages).cloned().collect();

        if new_keys.is_empty() {
            log::debug!("No new broadcast keys found, sleeping");
            tokio::time::sleep(tokio::time::Duration::from_secs_f64(s3_options.wait_time)).await;
            continue;
        }

        log::debug!("Found {} new keys for broadcast", new_keys.len());

        for new_key in new_keys {
            let obj = s3_client
                .get_object()
                .bucket(s3_options.bucket.clone())
                .key(new_key.clone())
                .send()
                .await
                .unwrap();

            let (sender_id, counter, chunk_id, num_chunks) = match obj.metadata() {
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
                            log::error!("No sender_id found in metadata for key: {}", new_key);
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
                            log::error!("No counter found in metadata for key: {}", new_key);
                            continue;
                        }
                    };
                    let chunk_id = match metadata.get("chunk_id") {
                        Some(chunk_id) => match chunk_id.parse::<u32>() {
                            Ok(chunk_id) => chunk_id,
                            Err(err) => {
                                log::error!("Failed to parse chunk_id: {}", err);
                                continue;
                            }
                        },
                        None => {
                            log::error!("No chunk_id found in metadata for key: {}", new_key);
                            continue;
                        }
                    };
                    let num_chunks = match metadata.get("num_chunks") {
                        Some(num_chunks) => match num_chunks.parse::<u32>() {
                            Ok(num_chunks) => num_chunks,
                            Err(err) => {
                                log::error!("Failed to parse num_chunks: {}", err);
                                continue;
                            }
                        },
                        None => {
                            log::error!("No num_chunks found in metadata for key: {}", new_key);
                            continue;
                        }
                    };
                    (sender_id, counter, chunk_id, num_chunks)
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
                sender_id,
                chunk_id,
                num_chunks,
                counter,
                collective: CollectiveType::Broadcast,
                data: Bytes::from(bytes),
            };
            broadcast_proxy.broadcast_send(msg).await.unwrap();
            received_messages.insert(new_key);
        }
    }
}

fn format_message_key(burst_id: &str, dest: u32, msg: &Message) -> String {
    format!(
        "{}/worker-{}/message-{}/sender-{}/counter-{}/part-{}",
        burst_id, dest, msg.collective, msg.sender_id, msg.counter, msg.chunk_id
    )
}

async fn get_next_object_key(proxy: &S3ReceiveProxy) -> Result<String> {
    let mut keys = proxy.keys.lock().await;

    while keys.pending.is_empty() {
        let prefix = format!(
            "{}/worker-{}/",
            proxy.burst_options.burst_id, proxy.worker_id
        );
        log::debug!("Listing keys with prefix {}...", prefix);
        let s3_keys: aws_sdk_s3::operation::list_objects::ListObjectsOutput = proxy
            .s3_client
            .list_objects()
            .bucket(proxy.s3_options.bucket.clone())
            .prefix(prefix)
            .send()
            .await?;

        match s3_keys.contents {
            Some(contents) => {
                for object in contents {
                    let key = match object.key {
                        Some(key) => key,
                        None => {
                            log::error!("No key found in S3");
                            continue;
                        }
                    };
                    if !keys.seen.contains(&key) {
                        keys.seen.insert(key.clone());
                        keys.pending.push(key);
                    }
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

    log::debug!("{} pending keys", keys.pending.len());

    let key = keys.pending.pop().unwrap();
    return Ok(key);
}
