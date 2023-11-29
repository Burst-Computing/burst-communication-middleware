use std::{collections::HashMap, sync::Arc};

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
            wait_time: 1.0,
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
}

pub struct S3BroadcastSendProxy {
    s3_client: Client,
    s3_options: Arc<S3Options>,
    burst_options: Arc<BurstOptions>,
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
        let key = format!(
            "{}{}",
            format_worker_mailbox_prefix(&self.burst_options, dest),
            uuid::Uuid::new_v4().to_string()
        );
        let byte_stream = ByteStream::from(msg.data.clone());
        log::debug!("Send key: {}", key);
        self.s3_client
            .put_object()
            .bucket(self.s3_options.bucket.clone())
            .key(key)
            .body(byte_stream)
            .metadata("sender_id", self.worker_id.to_string())
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
        }
    }
}

#[async_trait]
impl ReceiveProxy for S3ReceiveProxy {
    async fn recv(&self) -> Result<Message> {
        loop {
            let keys = self
                .s3_client
                .list_objects()
                .bucket(self.s3_options.bucket.clone())
                .prefix(format_worker_mailbox_prefix(
                    &self.burst_options,
                    self.worker_id,
                ))
                .send()
                .await?;

            if let Some(contents) = keys.contents {
                log::debug!("Listed {} keys", contents.len());
                for object in contents {
                    let key = match object.key {
                        Some(key) => key,
                        None => {
                            log::error!("No key found in object");
                            continue;
                        }
                    };
                    log::debug!("Fetch key: {}", key);
                    let obj = self
                        .s3_client
                        .get_object()
                        .bucket(self.s3_options.bucket.clone())
                        .key(key.clone())
                        .send()
                        .await?;

                    let sender_id = match obj.metadata() {
                        Some(metadata) => match metadata.get("sender_id") {
                            Some(sender_id) => match sender_id.parse::<u32>() {
                                Ok(sender_id) => sender_id,
                                Err(err) => {
                                    log::error!("Failed to parse sender_id: {}", err);
                                    continue;
                                }
                            },
                            None => {
                                log::error!("No sender_id found in metadata");
                                continue;
                            }
                        },
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
                        last_chunk: true,
                        counter: 0,
                        collective: CollectiveType::Direct,
                        data: Bytes::from(bytes),
                    };
                    return Ok(msg);
                }
            } else {
                log::debug!(
                    "No keys found, sleeping for {} seconds",
                    self.s3_options.wait_time
                );
                tokio::time::sleep(tokio::time::Duration::from_secs_f64(
                    self.s3_options.wait_time,
                ))
                .await;
            }
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
            s3_client,
            s3_options,
            burst_options,
        }
    }
}

#[async_trait]
impl BroadcastSendProxy for S3BroadcastSendProxy {
    async fn broadcast_send(&self, msg: &Message) -> Result<()> {
        unimplemented!()
    }
}

fn format_worker_mailbox_prefix(burst_options: &BurstOptions, worker_id: u32) -> String {
    format!("{}/{}/", burst_options.burst_id, worker_id)
}
