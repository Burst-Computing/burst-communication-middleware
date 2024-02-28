use core::panic;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use tokio::sync::{Mutex, Semaphore};

use async_trait::async_trait;

use aws_config::Region;
use aws_credential_types::Credentials;

use aws_sdk_s3::{primitives::ByteStream, Client};

use crate::{
    impl_chainable_setter, BroadcastProxy, BroadcastReceiveProxy, BroadcastSendProxy, BurstOptions,
    CollectiveType, Message, ReceiveProxy, Result, SendProxy, SendReceiveFactory, SendReceiveProxy,
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
    pub retry: u32,
    pub enable_broadcast: bool,
    pub semaphore_permits: usize,
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
    impl_chainable_setter!(retry, u32);
    impl_chainable_setter!(enable_broadcast, bool);
    impl_chainable_setter!(semaphore_permits, usize);

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
            retry: 100,
            enable_broadcast: false,
            semaphore_permits: 1024,
        }
    }
}

pub struct S3Impl;

#[async_trait]
impl SendReceiveFactory<S3Options> for S3Impl {
    async fn create_proxies(
        burst_options: Arc<BurstOptions>,
        s3_options: S3Options,
    ) -> Result<HashMap<u32, (Box<dyn SendReceiveProxy>, Box<dyn BroadcastProxy>)>> {
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

        let mut proxies = HashMap::new();

        // Seen and pending keys are shared between all send and receive proxies
        let direct_keys = Arc::new(Mutex::new(Keys {
            pending: Vec::new(),
            seen: HashSet::new(),
        }));
        // Same for broadcast proxies
        let bcast_keys = Arc::new(Mutex::new(Keys {
            pending: Vec::new(),
            seen: HashSet::new(),
        }));

        // Broadcast proxies all share the same client and semaphore
        // as only one worker will be using the broadcast proxy at a time
        let bcast_client = Client::from_conf(bcast_config.clone());
        let bcast_client_semaphore = Arc::new(Semaphore::new(s3_options.semaphore_permits));

        for worker_id in current_group {
            let proxy = S3Proxy::new(
                Client::from_conf(config.clone()),
                s3_options.clone(),
                burst_options.clone(),
                *worker_id,
                direct_keys.clone(),
            );
            let broadcast_proxy = S3BroadcastProxy::new(
                bcast_client.clone(),
                bcast_client_semaphore.clone(),
                s3_options.clone(),
                burst_options.clone(),
                bcast_keys.clone(),
            );
            proxies.insert(
                *worker_id,
                (
                    Box::new(proxy) as Box<dyn SendReceiveProxy>,
                    Box::new(broadcast_proxy) as Box<dyn BroadcastProxy>,
                ),
            );
        }

        Ok(proxies)
    }
}

// DIRECT PROXIES

pub struct S3Proxy {
    receiver: Box<dyn ReceiveProxy>,
    sender: Box<dyn SendProxy>,
}

pub struct S3SendProxy {
    s3_client: Client,
    client_semaphore: Arc<Semaphore>,
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
    client_semaphore: Arc<Semaphore>,
    s3_options: Arc<S3Options>,
    burst_options: Arc<BurstOptions>,
    worker_id: u32,
    keys: Arc<Mutex<Keys>>,
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
    async fn recv(&self, source: u32) -> Result<Message> {
        self.receiver.recv(source).await
    }
}

impl S3Proxy {
    pub fn new(
        s3_client: Client,
        s3_options: Arc<S3Options>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
        keys: Arc<Mutex<Keys>>,
    ) -> Self {
        // Each send-receive proxy pair has its own semaphore and client
        let client_semaphore = Arc::new(Semaphore::new(s3_options.semaphore_permits));
        Self {
            sender: Box::new(S3SendProxy::new(
                s3_client.clone(),
                Arc::clone(&client_semaphore),
                s3_options.clone(),
                burst_options.clone(),
                worker_id,
            )),
            receiver: Box::new(S3ReceiveProxy::new(
                s3_client.clone(),
                Arc::clone(&client_semaphore),
                s3_options.clone(),
                burst_options.clone(),
                worker_id,
                keys,
            )),
        }
    }
}

impl S3SendProxy {
    pub fn new(
        s3_client: Client,
        client_semaphore: Arc<Semaphore>,
        s3_options: Arc<S3Options>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
    ) -> Self {
        Self {
            s3_client,
            client_semaphore,
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
        let permit = self.client_semaphore.acquire().await.unwrap();
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
        drop(permit);
        Ok(())
    }
}

impl S3ReceiveProxy {
    pub fn new(
        s3_client: Client,
        client_semaphore: Arc<Semaphore>,
        s3_options: Arc<S3Options>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
        keys: Arc<Mutex<Keys>>,
    ) -> Self {
        Self {
            s3_client,
            client_semaphore,
            s3_options,
            burst_options,
            worker_id,
            keys,
        }
    }
}

#[async_trait]
impl ReceiveProxy for S3ReceiveProxy {
    async fn recv(&self, source: u32) -> Result<Message> {
        let permit = self.client_semaphore.acquire().await;
        if permit.is_err() {
            return Err("Failed to acquire semaphore permit".into());
        }

        let mut keys = self.keys.lock().await;
        let prefix = format!(
            "{}/worker-{}/sender-{}/",
            self.burst_options.burst_id, self.worker_id, source
        );
        let key = match get_next_object_key(
            &prefix,
            self.s3_client.clone(),
            self.s3_options.clone(),
            &mut keys,
        )
        .await
        {
            Ok(key) => key,
            Err(err) => {
                return Err(err);
            }
        };
        drop(keys);
        let bucket = self.s3_options.bucket.clone();
        let msg = match get_object_as_message(&bucket, &key, &self.s3_client).await? {
            Some(msg) => msg,
            None => {
                return Err(format!("Failed to get object {} as message", key).into());
            }
        };

        drop(permit);
        return Ok(msg);
    }
}

// BROADCAST PROXIES

pub struct S3BroadcastProxy {
    broadcast_sender: Box<dyn BroadcastSendProxy>,
    broadcast_receiver: Box<dyn BroadcastReceiveProxy>,
}

pub struct S3BroadcastSendProxy {
    s3_client: Client,
    client_semaphore: Arc<Semaphore>,
    s3_options: Arc<S3Options>,
    burst_options: Arc<BurstOptions>,
}

pub struct S3BroadcastReceiveProxy {
    s3_client: Client,
    client_semaphore: Arc<Semaphore>,
    s3_options: Arc<S3Options>,
    burst_options: Arc<BurstOptions>,
    keys: Arc<Mutex<Keys>>,
}

impl BroadcastProxy for S3BroadcastProxy {}

impl S3BroadcastProxy {
    pub fn new(
        s3_client: Client,
        s3_client_semaphore: Arc<Semaphore>,
        s3_options: Arc<S3Options>,
        burst_options: Arc<BurstOptions>,
        keys: Arc<Mutex<Keys>>,
    ) -> Self {
        Self {
            broadcast_sender: Box::new(S3BroadcastSendProxy::new(
                s3_client.clone(),
                s3_client_semaphore.clone(),
                s3_options.clone(),
                burst_options.clone(),
            )),
            broadcast_receiver: Box::new(S3BroadcastReceiveProxy::new(
                s3_client.clone(),
                s3_client_semaphore.clone(),
                s3_options.clone(),
                burst_options.clone(),
                keys,
            )),
        }
    }
}

#[async_trait]
impl BroadcastSendProxy for S3BroadcastProxy {
    async fn broadcast_send(&self, msg: Message) -> Result<()> {
        self.broadcast_sender.broadcast_send(msg).await
    }
}

#[async_trait]
impl BroadcastReceiveProxy for S3BroadcastProxy {
    async fn broadcast_recv(&self) -> Result<Message> {
        self.broadcast_receiver.broadcast_recv().await
    }
}

impl S3BroadcastSendProxy {
    pub fn new(
        s3_client: Client,
        client_semaphore: Arc<Semaphore>,
        s3_options: Arc<S3Options>,
        burst_options: Arc<BurstOptions>,
    ) -> Self {
        Self {
            s3_client,
            client_semaphore,
            s3_options,
            burst_options,
        }
    }
}

#[async_trait]
impl BroadcastSendProxy for S3BroadcastSendProxy {
    async fn broadcast_send(&self, msg: Message) -> Result<()> {
        if !self.s3_options.enable_broadcast {
            panic!("Broadcast not enabled");
        }

        let key = format!(
            "{}/broadcast/sender-{}/counter-{}/part-{}",
            self.burst_options.burst_id, msg.sender_id, msg.counter, msg.chunk_id
        );
        let permit = self.client_semaphore.acquire().await?;
        self.s3_client
            .put_object()
            .bucket(self.s3_options.bucket.clone())
            .key(&key)
            .body(ByteStream::from(msg.data))
            .metadata("sender_id", msg.sender_id.to_string())
            .metadata("collective", msg.collective.to_string())
            .metadata("counter", msg.counter.to_string())
            .metadata("chunk_id", msg.chunk_id.to_string())
            .metadata("num_chunks", msg.num_chunks.to_string())
            .send()
            .await?;
        log::debug!("S3 Put object {}", key);
        drop(permit);
        Ok(())
    }
}

impl S3BroadcastReceiveProxy {
    pub fn new(
        s3_client: Client,
        client_semaphore: Arc<Semaphore>,
        s3_options: Arc<S3Options>,
        burst_options: Arc<BurstOptions>,
        keys: Arc<Mutex<Keys>>,
    ) -> Self {
        Self {
            s3_client,
            client_semaphore,
            s3_options,
            burst_options,
            keys,
        }
    }
}

#[async_trait]
impl BroadcastReceiveProxy for S3BroadcastReceiveProxy {
    async fn broadcast_recv(&self) -> Result<Message> {
        let permit = self.client_semaphore.acquire().await;
        if permit.is_err() {
            return Err("Failed to acquire semaphore permit".into());
        }

        let mut keys = self.keys.lock().await;
        let prefix = format!("{}/broadcast/", self.burst_options.burst_id);
        let key = match get_next_object_key(
            prefix.as_str(),
            self.s3_client.clone(),
            self.s3_options.clone(),
            &mut keys,
        )
        .await
        {
            Ok(key) => key,
            Err(err) => {
                return Err(err);
            }
        };
        drop(keys);

        let bucket = self.s3_options.bucket.clone();
        let msg = match get_object_as_message(&bucket, &key, &self.s3_client).await? {
            Some(msg) => msg,
            None => {
                return Err(format!("Failed to get object {} as message", key).into());
            }
        };

        drop(permit);
        return Ok(msg);
    }
}

// Helper functions

fn format_message_key(burst_id: &str, dest: u32, msg: &Message) -> String {
    format!(
        "{}/worker-{}/sender-{}/counter-{}/part-{}",
        burst_id, dest, msg.sender_id, msg.counter, msg.chunk_id
    )
}

async fn get_object_as_message(
    bucket: &String,
    key: &String,
    s3_client: &Client,
) -> Result<Option<Message>> {
    log::debug!("S3 Get object with key {}...", key);
    let obj = s3_client
        .get_object()
        .bucket(bucket)
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
                        return Ok(None);
                    }
                },
                None => {
                    log::error!("No sender_id found in metadata for key: {}", key);
                    return Ok(None);
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
                        return Ok(None);
                    }
                },
                None => {
                    log::error!("No collective found in metadata for key: {}", key);
                    return Ok(None);
                }
            };
            let counter = match metadata.get("counter") {
                Some(counter) => match counter.parse::<u32>() {
                    Ok(counter) => counter,
                    Err(err) => {
                        log::error!("Failed to parse counter: {}", err);
                        return Ok(None);
                    }
                },
                None => {
                    log::error!("No counter found in metadata for key: {}", key);
                    return Ok(None);
                }
            };
            let chunk_id = match metadata.get("chunk_id") {
                Some(chunk_id) => match chunk_id.parse::<u32>() {
                    Ok(chunk_id) => chunk_id,
                    Err(err) => {
                        log::error!("Failed to parse chunk_id: {}", err);
                        return Ok(None);
                    }
                },
                None => {
                    log::error!("No chunk_id found in metadata for key: {}", key);
                    return Ok(None);
                }
            };
            let num_chunks = match metadata.get("num_chunks") {
                Some(num_chunks) => match num_chunks.parse::<u32>() {
                    Ok(num_chunks) => num_chunks,
                    Err(err) => {
                        log::error!("Failed to parse num_chunks: {}", err);
                        return Ok(None);
                    }
                },
                None => {
                    log::error!("No num_chunks found in metadata for key: {}", key);
                    return Ok(None);
                }
            };
            (sender_id, collective_type, counter, chunk_id, num_chunks)
        }
        None => {
            log::error!("No metadata found");
            return Ok(None);
        }
    };
    let bytes = obj.body.collect().await?.into_bytes();

    log::debug!("S3 Got object {} with size {}", key, bytes.len());

    s3_client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    Ok(Some(Message {
        sender_id,
        chunk_id,
        num_chunks,
        counter,
        collective: collective_type,
        data: bytes,
    }))
}

async fn get_next_object_key(
    prefix: &str,
    s3_client: Client,
    s3_options: Arc<S3Options>,
    keys: &mut Keys,
) -> Result<String> {
    let mut retries = 0;

    while keys.pending.is_empty() {
        log::debug!("Listing keys with prefix {}...", prefix);
        let s3_keys: aws_sdk_s3::operation::list_objects::ListObjectsOutput = s3_client
            .list_objects()
            .bucket(&s3_options.bucket)
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
                    s3_options.wait_time
                );
                tokio::time::sleep(tokio::time::Duration::from_secs_f64(s3_options.wait_time))
                    .await;
                retries += 1;
            }
        }

        if retries >= s3_options.retry {
            return Err("Failed to get next object key".into());
        }
    }

    log::debug!("{} pending keys", keys.pending.len());

    let key = keys.pending.pop().unwrap();
    Ok(key)
}
