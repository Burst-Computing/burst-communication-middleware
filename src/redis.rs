use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use redis::{
    aio::{ConnectionLike, MultiplexedConnection},
    streams::{StreamReadOptions, StreamReadReply},
    AsyncCommands, Client,
};
use tokio::sync::RwLock;

use crate::{
    impl_chainable_setter, BroadcastSendProxy, BurstOptions, CollectiveType, Message, ReceiveProxy,
    Result, SendProxy, SendReceiveFactory, SendReceiveProxy,
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
        broadcast_proxy: Box<dyn BroadcastSendProxy>,
    ) -> Result<(
        HashMap<u32, Box<dyn SendReceiveProxy>>,
        Box<dyn BroadcastSendProxy>,
    )> {
        let redis_options = Arc::new(redis_options);
        let client = Client::open(redis_options.redis_uri.clone())?;

        init_redis(
            client.clone(),
            burst_options.clone(),
            redis_options.clone(),
            broadcast_proxy,
        )
        .await?;

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

        Ok((
            hmap,
            Box::new(RedisBroadcastSendProxy::new(
                client.get_multiplexed_async_connection().await?,
                redis_options,
                burst_options,
            )) as Box<dyn BroadcastSendProxy>,
        ))
    }
}

async fn init_redis(
    client: Client,
    burst_options: Arc<BurstOptions>,
    redis_options: Arc<RedisOptions>,
    broadcast_proxy: Box<dyn BroadcastSendProxy>,
) -> Result<()> {
    // spawn task to receive broadcast messages and send them to the broadcast proxy
    let mut connection = client.get_async_connection().await?;
    let broadcast_stream = get_broadcast_stream_name(
        &redis_options.broadcast_stream_prefix,
        &burst_options.burst_id,
        &burst_options.group_id,
    );
    tokio::spawn(async move {
        let mut last_broadcast_id = "0".to_string();
        loop {
            let (last_id, msg) = read_redis(&mut connection, &broadcast_stream, &last_broadcast_id)
                .await
                .unwrap();
            last_broadcast_id = last_id;
            broadcast_proxy.broadcast_send(&msg).await.unwrap();
        }
    });

    Ok(())
}

pub struct RedisProxy {
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
}

pub struct RedisBroadcastSendProxy {
    connection: MultiplexedConnection,
    redis_options: Arc<RedisOptions>,
    burst_options: Arc<BurstOptions>,
}

impl SendReceiveProxy for RedisProxy {}

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
        client: Client,
        redis_options: Arc<RedisOptions>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
    ) -> Result<Self> {
        Ok(Self {
            worker_id,
            sender: Box::new(RedisSendProxy::new(
                client.get_multiplexed_async_connection().await?,
                redis_options.clone(),
                burst_options.clone(),
            )),
            receiver: Box::new(RedisReceiveProxy::new(
                client.get_multiplexed_async_connection().await?,
                redis_options.clone(),
                burst_options.clone(),
                worker_id,
            )),
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
    pub fn new(
        connection: MultiplexedConnection,
        redis_options: Arc<RedisOptions>,
        burst_options: Arc<BurstOptions>,
    ) -> Self {
        Self {
            connection,
            redis_options,
            burst_options,
        }
    }
}

#[async_trait]
impl ReceiveProxy for RedisReceiveProxy {
    async fn recv(&self) -> Result<Message> {
        let last_id = self.last_id.read().await.clone();

        let (last_id, msg) = read_redis(
            &mut self.connection.clone(),
            &get_direct_stream_name(
                &self.redis_options.direct_stream_prefix,
                &self.burst_options.burst_id,
                self.worker_id,
            ),
            &last_id,
        )
        .await?;

        *self.last_id.write().await = last_id;

        Ok(msg)
    }
}

impl RedisReceiveProxy {
    pub fn new(
        connection: MultiplexedConnection,
        redis_options: Arc<RedisOptions>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
    ) -> Self {
        Self {
            connection,
            redis_options,
            burst_options,
            worker_id,
            last_id: Arc::new(RwLock::new("0".to_string())),
        }
    }
}

impl RedisBroadcastSendProxy {
    pub fn new(
        connection: MultiplexedConnection,
        redis_options: Arc<RedisOptions>,
        burst_options: Arc<BurstOptions>,
    ) -> Self {
        Self {
            connection,
            redis_options,
            burst_options,
        }
    }
}

#[async_trait]
impl BroadcastSendProxy for RedisBroadcastSendProxy {
    async fn broadcast_send(&self, msg: &Message) -> Result<()> {
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

async fn send_direct<C>(
    connection: C,
    msg: &Message,
    dest: u32,
    redis_options: &RedisOptions,
    burst_options: &BurstOptions,
) -> Result<()>
where
    C: ConnectionLike + Send,
{
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

async fn send_broadcast<C>(
    connection: C,
    msg: &Message,
    dest: &str,
    redis_options: &RedisOptions,
    burst_options: &BurstOptions,
) -> Result<()>
where
    C: ConnectionLike + Send,
{
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

async fn send_redis<C>(mut connection: C, msg: &Message, key: &str) -> Result<()>
where
    C: ConnectionLike + Send,
{
    let data: Vec<(String, Vec<u8>)> = msg.clone().into_iter().collect::<Vec<_>>();
    connection.xadd(key, "*", &data).await?;
    Ok(())
}

async fn read_redis<C>(connection: &mut C, stream: &str, last_id: &str) -> Result<(String, Message)>
where
    C: ConnectionLike + Send,
{
    let r: StreamReadReply = connection
        .xread_options(
            &[stream],
            &[last_id],
            &StreamReadOptions::default().count(1).block(0),
        )
        .await?;
    let stream_key = r.keys.iter().next().unwrap();
    let last_id = stream_key.ids.iter().next().unwrap().id.clone();
    let msg = Message::from(r);
    Ok((last_id, msg))
}

fn get_direct_stream_name(prefix: &str, burst_id: &str, worker_id: u32) -> String {
    format!("{}:{}:worker_{}", prefix, burst_id, worker_id)
}

fn get_broadcast_stream_name(prefix: &str, burst_id: &str, group_id: &str) -> String {
    format!("{}:{}:group_{}", prefix, burst_id, group_id)
}
