mod actor;
mod burst_message_relay;
mod chunk_store;
mod message_buffer;
mod middleware;
mod rabbitmq;
mod redis_list;
mod redis_stream;
mod s3;
mod tokio_channel;
mod types;
mod utils;

pub use actor::*;
pub use burst_message_relay::*;
use bytes::Bytes;
pub use middleware::*;
pub use rabbitmq::*;
pub use redis_list::*;
pub use redis_stream::*;
pub use s3::*;
use tokio::runtime::{Handle, Runtime};
pub use tokio_channel::*;
pub use types::*;

use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug)]
pub struct Config {
    pub backend: Backend,
    pub server: Option<String>,
    pub burst_id: String,
    pub burst_size: u32,
    pub group_ranges: HashMap<String, HashSet<u32>>,
    pub group_id: String,
    pub chunking: bool,
    pub chunk_size: usize,
    pub tokio_broadcast_channel_size: Option<usize>,
}

#[derive(Clone, Debug)]
pub enum Backend {
    /// Use S3 as backend
    S3 {
        /// S3 bucket name
        bucket: Option<String>,
        /// S3 region
        region: Option<String>,
        /// S3 access key id
        access_key_id: Option<String>,
        /// S3 secret access key
        secret_access_key: Option<String>,
        /// S3 session token
        session_token: Option<String>,
        // Semphore permits
        semaphore_permits: Option<usize>,
    },
    /// Use Redis Streams as backend
    RedisStream,
    /// Use Redis Lists as backend
    RedisList,
    /// Use RabbitMQ as backend
    Rabbitmq,
    /// Use burst message relay as backend
    MessageRelay,
}

pub struct Middleware<T> {
    middleware: BurstMiddleware<T>,
    runtime: Handle,
}

impl<T> Middleware<T> {
    pub fn new(middleware: BurstMiddleware<T>, runtime: Handle) -> Self {
        Self {
            middleware,
            runtime,
        }
    }

    pub fn get_actor_handle(self) -> MiddlewareActorHandle<T>
    where
        T: From<Bytes> + Into<Bytes> + Sync + Send + 'static,
    {
        MiddlewareActorHandle::new(self.middleware, &self.runtime)
    }

    pub fn info(&self) -> BurstInfo {
        self.middleware.info()
    }
}

pub fn create_actors<T>(
    conf: Config,
    tokio_runtime: &Runtime,
) -> Result<HashMap<u32, Middleware<T>>> {
    let burst_options = BurstOptions::new(
        conf.burst_size,
        conf.group_ranges,
        conf.group_id.to_string(),
    )
    .burst_id(conf.burst_id.to_string())
    .enable_message_chunking(conf.chunking)
    .message_chunk_size(conf.chunk_size)
    .build();

    let mut channel_options = TokioChannelOptions::new();
    if let Some(size) = conf.tokio_broadcast_channel_size {
        channel_options.broadcast_channel_size(size);
    }

    let actors = tokio_runtime.block_on(async move {
        match &conf.backend {
            Backend::S3 {
                bucket,
                region,
                access_key_id,
                secret_access_key,
                session_token,
                semaphore_permits,
            } => {
                let mut options = S3Options::default();
                if let Some(bucket) = bucket {
                    options.bucket(bucket.to_string());
                }
                if let Some(region) = region {
                    options.region(region.to_string());
                }
                if let Some(access_key_id) = access_key_id {
                    options.access_key_id(access_key_id.to_string());
                }
                if let Some(secret_access_key) = secret_access_key {
                    options.secret_access_key(secret_access_key.to_string());
                }
                options.session_token(session_token.clone());
                options.endpoint(conf.server.clone());
                if let Some(permits) = semaphore_permits {
                    options.semaphore_permits(*permits);
                }

                BurstMiddleware::create_proxies::<TokioChannelImpl, S3Impl, _, _>(
                    burst_options,
                    channel_options,
                    options,
                )
                .await
            }
            Backend::RedisStream => {
                let mut options = RedisStreamOptions::default();
                if let Some(server) = &conf.server {
                    options.redis_uri(server.to_string());
                }

                BurstMiddleware::create_proxies::<TokioChannelImpl, RedisStreamImpl, _, _>(
                    burst_options,
                    channel_options,
                    options,
                )
                .await
            }
            Backend::RedisList => {
                let mut options = RedisListOptions::default();
                if let Some(server) = &conf.server {
                    options.redis_uri(server.to_string());
                }
                BurstMiddleware::create_proxies::<TokioChannelImpl, RedisListImpl, _, _>(
                    burst_options,
                    channel_options,
                    options,
                )
                .await
            }
            Backend::Rabbitmq => {
                let mut options = RabbitMQOptions::default()
                    .durable_queues(true)
                    .ack(true)
                    .build();
                if let Some(server) = &conf.server {
                    options.rabbitmq_uri(server.to_string());
                }
                BurstMiddleware::create_proxies::<TokioChannelImpl, RabbitMQMImpl, _, _>(
                    burst_options,
                    channel_options,
                    options,
                )
                .await
            }
            Backend::MessageRelay => {
                let mut options = BurstMessageRelayOptions::default();
                if let Some(server) = &conf.server {
                    options.server_uri(server.to_string());
                }
                BurstMiddleware::create_proxies::<TokioChannelImpl, BurstMessageRelayImpl, _, _>(
                    burst_options,
                    channel_options,
                    options,
                )
                .await
            }
        }
    });

    Ok(actors?
        .into_iter()
        .map(|(worker_id, middleware)| {
            (
                worker_id,
                Middleware::new(middleware, tokio_runtime.handle().clone()),
            )
        })
        .collect())
}
