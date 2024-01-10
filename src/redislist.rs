use std::{collections::HashMap, fmt::format, sync::Arc, vec};

use async_trait::async_trait;

use bytes::Bytes;
use futures::StreamExt;
use redis::{
    aio::{ConnectionLike, MultiplexedConnection},
    AsyncCommands, Client, FromRedisValue,
};
use tracing::Value;

use crate::{
    impl_chainable_setter, BroadcastSendProxy, BurstOptions, CollectiveType, Message, ReceiveProxy,
    Result, SendProxy, SendReceiveFactory, SendReceiveProxy,
};

#[derive(Clone, Debug)]
pub struct RedisListOptions {
    pub redis_uri: String,
    pub list_key_prefix: String,
    pub broadcast_topic_prefix: String,
}

impl RedisListOptions {
    pub fn new(redis_uri: String) -> Self {
        Self {
            redis_uri,
            ..Default::default()
        }
    }

    impl_chainable_setter! {
        redis_uri, String
    }

    impl_chainable_setter! {
        list_key_prefix, String
    }

    impl_chainable_setter! {
        broadcast_topic_prefix, String
    }

    pub fn build(&self) -> Self {
        self.clone()
    }
}

impl Default for RedisListOptions {
    fn default() -> Self {
        Self {
            redis_uri: "redis://localhost:6379".to_string(),
            list_key_prefix: "direct_stream".into(),
            broadcast_topic_prefix: "broadcast_stream".into(),
        }
    }
}

pub struct RedisListImpl;

#[async_trait]
impl SendReceiveFactory<RedisListOptions> for RedisListImpl {
    async fn create_proxies(
        burst_options: Arc<BurstOptions>,
        redis_options: RedisListOptions,
        broadcast_proxy: Box<dyn BroadcastSendProxy>,
    ) -> Result<(
        HashMap<u32, Box<dyn SendReceiveProxy>>,
        Box<dyn BroadcastSendProxy>,
    )> {
        let redis_options = Arc::new(redis_options);
        let client = Client::open(redis_options.redis_uri.clone())?;

        // spawn task to receive broadcast messages and send them to the broadcast proxy
        let mut broadcast_connection = client.get_async_connection().await?;
        let broadcast_list = get_broadcast_list_key(
            &redis_options.broadcast_topic_prefix,
            &burst_options.burst_id,
            &burst_options.group_id,
        );
        tokio::spawn(async move {
            loop {
                // wait for the next message containing the broadcast key
                log::debug!("waiting for broadcast message");
                let (_, bcast_key): (String, String) = broadcast_connection
                    .blpop(&broadcast_list, 0)
                    .await
                    .unwrap();
                log::debug!("received broadcast message with key {:?}", bcast_key);

                // get the message from redis using GET and send it to the broadcast proxy
                let header: Vec<u8> = broadcast_connection
                    .get(format!("{}-header", bcast_key))
                    .await
                    .unwrap();
                let payload: Vec<u8> = broadcast_connection
                    .get(format!("{}-payload", bcast_key))
                    .await
                    .unwrap();
                let msg = Message::from((header, payload));
                broadcast_proxy.broadcast_send(&msg).await.unwrap();
            }
        });

        let current_group = burst_options
            .group_ranges
            .get(&burst_options.group_id)
            .unwrap();

        let mut hmap = HashMap::new();

        futures::future::try_join_all(current_group.iter().map(|worker_id| {
            let c = client.clone();
            let r = redis_options.clone();
            let b = burst_options.clone();
            async move { RedisListProxy::new(c.clone(), r.clone(), b.clone(), *worker_id).await }
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
            Box::new(RedisListBroadcastSendProxy::new(
                client.get_multiplexed_async_connection().await?,
                redis_options,
                burst_options,
            )) as Box<dyn BroadcastSendProxy>,
        ))
    }
}

pub struct RedisListProxy {
    worker_id: u32,
    receiver: Box<dyn ReceiveProxy>,
    sender: Box<dyn SendProxy>,
}

pub struct RedisListSendProxy {
    connection: MultiplexedConnection,
    redis_options: Arc<RedisListOptions>,
    burst_options: Arc<BurstOptions>,
}

pub struct RedisListReceiveProxy {
    connection: MultiplexedConnection,
    redis_options: Arc<RedisListOptions>,
    burst_options: Arc<BurstOptions>,
    worker_id: u32,
}

pub struct RedisListBroadcastSendProxy {
    connection: MultiplexedConnection,
    redis_options: Arc<RedisListOptions>,
    burst_options: Arc<BurstOptions>,
}

impl SendReceiveProxy for RedisListProxy {}

#[async_trait]
impl SendProxy for RedisListProxy {
    async fn send(&self, dest: u32, msg: &Message) -> Result<()> {
        self.sender.send(dest, msg).await
    }
}

#[async_trait]
impl ReceiveProxy for RedisListProxy {
    async fn recv(&self) -> Result<Message> {
        self.receiver.recv().await
    }
}

impl RedisListProxy {
    pub async fn new(
        client: Client,
        redis_options: Arc<RedisListOptions>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
    ) -> Result<Self> {
        Ok(Self {
            worker_id,
            sender: Box::new(RedisListSendProxy::new(
                client.get_multiplexed_async_connection().await?,
                redis_options.clone(),
                burst_options.clone(),
            )),
            receiver: Box::new(RedisListReceiveProxy::new(
                client.get_multiplexed_async_connection().await?,
                redis_options.clone(),
                burst_options.clone(),
                worker_id,
            )),
        })
    }
}

impl RedisListSendProxy {
    pub fn new(
        connection: MultiplexedConnection,
        redis_options: Arc<RedisListOptions>,
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
impl SendProxy for RedisListSendProxy {
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

impl RedisListReceiveProxy {
    pub fn new(
        connection: MultiplexedConnection,
        redis_options: Arc<RedisListOptions>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
    ) -> Self {
        Self {
            connection,
            redis_options,
            burst_options,
            worker_id,
        }
    }
}

#[async_trait]
impl ReceiveProxy for RedisListReceiveProxy {
    async fn recv(&self) -> Result<Message> {
        let msg = read_redis(
            &mut self.connection.clone(),
            &get_redis_list_key(
                &self.redis_options.list_key_prefix,
                &self.burst_options.burst_id,
                self.worker_id,
            ),
        )
        .await?;

        Ok(msg)
    }
}

impl RedisListBroadcastSendProxy {
    pub fn new(
        connection: MultiplexedConnection,
        redis_options: Arc<RedisListOptions>,
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
impl BroadcastSendProxy for RedisListBroadcastSendProxy {
    async fn broadcast_send(&self, msg: &Message) -> Result<()> {
        let mut con = self.connection.clone();

        let bcast_key = format!(
            "{}:broadcast:{}",
            self.burst_options.burst_id,
            uuid::Uuid::new_v4().to_string()
        );
        let [header, payload]: [&[u8]; 2] = msg.into();
        log::debug!("sending message: {:?}", payload);
        con.set(format!("{}-header", bcast_key), &header).await?;
        con.set(format!("{}-payload", bcast_key), &payload).await?;

        if msg.collective != CollectiveType::Broadcast {
            return Err("Cannot send non-broadcast message to broadcast".into());
        }

        futures::future::try_join_all(
            self.burst_options
                .group_ranges
                .keys()
                .filter(|dest| **dest != self.burst_options.group_id)
                .map(|dest| {
                    get_broadcast_list_key(
                        &self.redis_options.broadcast_topic_prefix,
                        &self.burst_options.burst_id,
                        dest,
                    )
                })
                .map(|key| send_broadcast(con.clone(), key, &bcast_key)),
        )
        .await?;

        Ok(())
    }
}

async fn send_direct<C>(
    connection: C,
    msg: &Message,
    dest: u32,
    redis_options: &RedisListOptions,
    burst_options: &BurstOptions,
) -> Result<()>
where
    C: ConnectionLike + Send,
{
    Ok(send_redis(
        connection,
        msg,
        get_redis_list_key(
            &redis_options.list_key_prefix,
            &burst_options.burst_id,
            dest,
        ),
    )
    .await?)
}

async fn send_broadcast<C>(mut connection: C, topic: String, key: &String) -> Result<()>
where
    C: ConnectionLike + Send,
{
    connection.rpush(topic, key).await?;
    Ok(())
}

async fn send_redis<C>(mut connection: C, msg: &Message, key: String) -> Result<()>
where
    C: ConnectionLike + Send,
{
    let data: [&[u8]; 2] = msg.into();
    let payload = data.concat();
    // log::debug!("sending message: {:?}", payload);
    connection.rpush(key, payload).await?;
    Ok(())
}

async fn read_redis<C>(connection: &mut C, key: &str) -> Result<Message>
where
    C: ConnectionLike + Send,
{
    // log::debug!("waiting for message with key {:?}", key);
    let (_, payload): (String, Vec<u8>) = connection.blpop(key, 0).await?;
    // log::debug!("received message: {:?}", payload);
    let msg = Message::from(payload);
    Ok(msg)
}

fn get_redis_list_key(prefix: &str, burst_id: &str, worker_id: u32) -> String {
    format!("{}:{}:worker_{}", prefix, burst_id, worker_id)
}

fn get_broadcast_list_key(prefix: &str, burst_id: &str, group_id: &str) -> String {
    format!("{}:{}:group_{}", prefix, burst_id, group_id)
}
