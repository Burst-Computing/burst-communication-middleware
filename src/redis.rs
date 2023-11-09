use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use redis::{
    aio::MultiplexedConnection,
    streams::{StreamReadOptions, StreamReadReply},
    AsyncCommands, Client,
};
use tokio::sync::RwLock;

use crate::{
    impl_chainable_setter, BurstOptions, CollectiveType, Message, ReceiveProxy, Result, SendProxy,
    SendReceiveFactory, SendReceiveProxy,
};

#[derive(Clone, Debug)]
pub struct RedisOptions {
    pub redis_uri: String,
    pub direct_stream_prefix: String,
    pub broadcast_stream_prefix: String,
}

impl RedisOptions {
    pub fn new(redis_uri: String) -> Self {
        Self {
            redis_uri,
            ..Default::default()
        }
    }

    impl_chainable_setter! {
        direct_stream_prefix, String
    }

    impl_chainable_setter! {
        broadcast_stream_prefix, String
    }

    pub fn build(&self) -> Self {
        self.clone()
    }
}

impl Default for RedisOptions {
    fn default() -> Self {
        Self {
            redis_uri: "redis://localhost:6379".into(),
            direct_stream_prefix: "direct_stream".into(),
            broadcast_stream_prefix: "broadcast_stream".into(),
        }
    }
}

pub struct RedisImpl;

#[async_trait]
impl SendReceiveFactory<RedisOptions> for RedisImpl {
    async fn create_proxies(
        burst_options: Arc<BurstOptions>,
        redis_options: RedisOptions,
    ) -> Result<HashMap<u32, Box<dyn SendReceiveProxy>>> {
        let redis_options = Arc::new(redis_options);
        let client = Client::open(redis_options.redis_uri.clone())?;

        let current_group = burst_options
            .group_ranges
            .get(&burst_options.group_id)
            .unwrap();

        let mut hmap = HashMap::new();

        futures::future::try_join_all(current_group.iter().map(|worker_id| {
            let c = client.clone();
            let r = redis_options.clone();
            let b = burst_options.clone();
            async move { RedisProxy::new(c.clone(), r.clone(), b.clone(), *worker_id).await }
        }))
        .await?
        .into_iter()
        .for_each(|proxy| {
            hmap.insert(
                proxy.worker_id,
                Box::new(proxy) as Box<dyn SendReceiveProxy>,
            );
        });

        Ok(hmap)
    }
}

pub struct RedisProxy {
    connection: MultiplexedConnection,
    redis_options: Arc<RedisOptions>,
    burst_options: Arc<BurstOptions>,
    worker_id: u32,
    receiver: Box<dyn ReceiveProxy>,
    sender: Box<dyn SendProxy>,
}

pub struct RedisSendProxy {
    connection: MultiplexedConnection,
    redis_options: Arc<RedisOptions>,
    burst_options: Arc<BurstOptions>,
}

pub struct RedisReceiveProxy {
    connection: MultiplexedConnection,
    redis_options: Arc<RedisOptions>,
    burst_options: Arc<BurstOptions>,
    worker_id: u32,
    last_id: Arc<RwLock<String>>,
    last_broadcast_id: Arc<RwLock<String>>,
}

#[async_trait]
impl SendReceiveProxy for RedisProxy {
    async fn broadcast(&self, msg: &Message) -> Result<()> {
        if msg.collective != CollectiveType::Broadcast {
            Err("Cannot send non-broadcast message to broadcast".into())
        } else {
            futures::future::try_join_all(
                self.burst_options
                    .group_ranges
                    .keys()
                    .filter(|dest| **dest != self.burst_options.group_id)
                    .map(|dest| {
                        send_broadcast(
                            self.connection.clone(),
                            msg,
                            dest,
                            &self.redis_options,
                            &self.burst_options,
                        )
                    }),
            )
            .await?;
            Ok(())
        }
    }
}

#[async_trait]
impl SendProxy for RedisProxy {
    async fn send(&self, dest: u32, msg: &Message) -> Result<()> {
        self.sender.send(dest, msg).await
    }
}

#[async_trait]
impl ReceiveProxy for RedisProxy {
    async fn recv(&self) -> Result<Message> {
        self.receiver.recv().await
    }
}

impl RedisProxy {
    pub async fn new(
        cliet: Client,
        redis_options: Arc<RedisOptions>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
    ) -> Result<Self> {
        Ok(Self {
            connection: cliet.get_multiplexed_async_connection().await?,
            worker_id,
            sender: Box::new(
                RedisSendProxy::new(
                    cliet.get_multiplexed_async_connection().await?,
                    redis_options.clone(),
                    burst_options.clone(),
                )
                .await?,
            ),
            receiver: Box::new(
                RedisReceiveProxy::new(
                    cliet.get_multiplexed_async_connection().await?,
                    redis_options.clone(),
                    burst_options.clone(),
                    worker_id,
                )
                .await?,
            ),
            redis_options,
            burst_options,
        })
    }
}

#[async_trait]
impl SendProxy for RedisSendProxy {
    async fn send(&self, dest: u32, msg: &Message) -> Result<()> {
        Ok(send_direct(
            self.connection.clone(),
            msg,
            dest,
            &self.redis_options,
            &self.burst_options,
        )
        .await?)
    }
}

impl RedisSendProxy {
    pub async fn new(
        connection: MultiplexedConnection,
        redis_options: Arc<RedisOptions>,
        burst_options: Arc<BurstOptions>,
    ) -> Result<Self> {
        Ok(Self {
            connection,
            redis_options,
            burst_options,
        })
    }
}

#[async_trait]
impl ReceiveProxy for RedisReceiveProxy {
    async fn recv(&self) -> Result<Message> {
        let last_id = self.last_id.read().await.clone();
        let last_broadcast_id = self.last_broadcast_id.read().await.clone();

        let (last_id, last_broadcast_id, msg) = receive_redis(
            self.connection.clone(),
            &self.burst_options.group_id,
            self.worker_id,
            &last_id,
            &last_broadcast_id,
            &self.redis_options,
            &self.burst_options,
        )
        .await?;

        if let Some(last_id) = last_id {
            *self.last_id.write().await = last_id;
        }
        if let Some(last_broadcast_id) = last_broadcast_id {
            *self.last_broadcast_id.write().await = last_broadcast_id;
        }

        Ok(msg)
    }
}

impl RedisReceiveProxy {
    pub async fn new(
        connection: MultiplexedConnection,
        redis_options: Arc<RedisOptions>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
    ) -> Result<Self> {
        Ok(Self {
            connection,
            redis_options,
            burst_options,
            worker_id,
            last_id: Arc::new(RwLock::new("0".to_string())),
            last_broadcast_id: Arc::new(RwLock::new("0".to_string())),
        })
    }
}

async fn send_direct(
    connection: MultiplexedConnection,
    msg: &Message,
    dest: u32,
    redis_options: &RedisOptions,
    burst_options: &BurstOptions,
) -> Result<()> {
    Ok(send_redis(
        connection,
        msg,
        &get_direct_stream_name(
            &redis_options.direct_stream_prefix,
            &burst_options.burst_id,
            dest,
        ),
    )
    .await?)
}

async fn send_broadcast(
    connection: MultiplexedConnection,
    msg: &Message,
    dest: &str,
    redis_options: &RedisOptions,
    burst_options: &BurstOptions,
) -> Result<()> {
    Ok(send_redis(
        connection,
        msg,
        &get_broadcast_stream_name(
            &redis_options.broadcast_stream_prefix,
            &burst_options.burst_id,
            dest,
        ),
    )
    .await?)
}

async fn send_redis(mut connection: MultiplexedConnection, msg: &Message, key: &str) -> Result<()> {
    let data = msg.clone().into_iter().collect::<Vec<_>>();
    connection.xadd(key, "*", &data).await?;
    Ok(())
}

async fn receive_redis(
    mut connection: MultiplexedConnection,
    group_id: &str,
    worker_id: u32,
    last_id: &str,
    last_broadcast_id: &str,
    redis_options: &RedisOptions,
    burst_options: &BurstOptions,
) -> Result<(Option<String>, Option<String>, Message)> {
    let direct_stream = get_direct_stream_name(
        &redis_options.direct_stream_prefix,
        &burst_options.burst_id,
        worker_id,
    );
    let broadcast_stream = get_broadcast_stream_name(
        &redis_options.broadcast_stream_prefix,
        &burst_options.burst_id,
        group_id,
    );
    let r: StreamReadReply = connection
        .xread_options(
            &[&direct_stream, &broadcast_stream],
            &[last_id, last_broadcast_id],
            &StreamReadOptions::default().count(1).block(0),
        )
        .await?;
    let stream_key = r.keys.iter().next().unwrap();
    let last_id = stream_key.ids.iter().next().unwrap().id.clone();
    match stream_key.key.as_str() {
        k if k == direct_stream => {
            let msg = Message::from(r);
            Ok((Some(last_id), None, msg))
        }
        k if k == broadcast_stream => {
            let msg = Message::from(r);
            Ok((None, Some(last_id), msg))
        }
        _ => Err("Invalid stream key".into()),
    }
}

fn get_direct_stream_name(prefix: &str, burst_id: &str, worker_id: u32) -> String {
    format!("{}:{}:worker_{}", prefix, burst_id, worker_id)
}

fn get_broadcast_stream_name(prefix: &str, burst_id: &str, group_id: &str) -> String {
    format!("{}:{}:group_{}", prefix, burst_id, group_id)
}
