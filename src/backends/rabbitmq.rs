use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;

use bytes::Bytes;
use deadpool_lapin::{Config, Pool, PoolConfig, Runtime};
use futures::TryStreamExt;
use lapin::{
    message::Delivery,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions,
        QueueBindOptions, QueueDeclareOptions,
    },
    types::{AMQPValue, FieldTable},
    BasicProperties, Consumer, ExchangeKind,
};
use uuid::Uuid;

use crate::{
    impl_chainable_setter, BurstOptions, MessageMetadata, RemoteBroadcastProxy,
    RemoteBroadcastReceiveProxy, RemoteBroadcastSendProxy, RemoteMessage, RemoteReceiveProxy,
    RemoteSendProxy, RemoteSendReceiveFactory, RemoteSendReceiveProxy, Result,
};

#[derive(Clone, Debug)]
pub struct RabbitMQOptions {
    pub rabbitmq_uri: String,
    pub direct_exchange_prefix: String,
    pub broadcast_exchange_prefix: String,
    pub queue_prefix: String,
    pub broadcast_queue_prefix: String,
    pub ack: bool,
    pub durable_exchanges: bool,
    pub durable_queues: bool,
    pub pool_size: Option<usize>,
}

impl RabbitMQOptions {
    pub fn new(rabbitmq_uri: String) -> Self {
        Self {
            rabbitmq_uri,
            ..Default::default()
        }
    }

    impl_chainable_setter!(rabbitmq_uri, String);
    impl_chainable_setter!(direct_exchange_prefix, String);
    impl_chainable_setter!(broadcast_exchange_prefix, String);
    impl_chainable_setter!(queue_prefix, String);
    impl_chainable_setter!(ack, bool);
    impl_chainable_setter!(broadcast_queue_prefix, String);
    impl_chainable_setter!(durable_exchanges, bool);
    impl_chainable_setter!(durable_queues, bool);
    impl_chainable_setter!(pool_size, Option<usize>);

    pub fn build(&self) -> Self {
        self.clone()
    }
}

impl Default for RabbitMQOptions {
    fn default() -> Self {
        Self {
            rabbitmq_uri: "amqp://guest:guest@localhost:5672".into(),
            direct_exchange_prefix: "burst_direct".into(),
            broadcast_exchange_prefix: "burst_topict".into(),
            queue_prefix: "queue".into(),
            broadcast_queue_prefix: "broadcast_queue".into(),
            ack: true,
            durable_exchanges: true,
            durable_queues: true,
            pool_size: None,
        }
    }
}

pub struct RabbitMQMImpl;

#[async_trait]
impl RemoteSendReceiveFactory<RabbitMQOptions> for RabbitMQMImpl {
    async fn create_remote_proxies(
        burst_options: Arc<BurstOptions>,
        rabbitmq_options: RabbitMQOptions,
    ) -> Result<
        HashMap<
            u32,
            (
                Box<dyn RemoteSendReceiveProxy>,
                Box<dyn RemoteBroadcastProxy>,
            ),
        >,
    > {
        let rabbitmq_options = Arc::new(rabbitmq_options);

        let current_group = burst_options
            .group_ranges
            .get(&burst_options.group_id)
            .unwrap();

        // Create pool of connections
        let mut config = Config::default();
        let mut pool_config = PoolConfig::default();
        pool_config.max_size = rabbitmq_options.pool_size.unwrap_or(current_group.len());
        config.url = Some(rabbitmq_options.rabbitmq_uri.to_string());
        config.pool = Some(pool_config);
        let pool = config.create_pool(Some(Runtime::Tokio1)).unwrap();

        init_rabbit(
            pool.clone(),
            burst_options.clone(),
            rabbitmq_options.clone(),
        )
        .await?;

        let mut proxies = HashMap::new();

        futures::future::try_join_all(current_group.iter().map(|worker_id| {
            let r = rabbitmq_options.clone();
            let b = burst_options.clone();
            let p = pool.clone();
            async move {
                let proxy = RabbitMQProxy::new(r.clone(), b.clone(), *worker_id, p.clone())
                    .await
                    .unwrap();
                let broadcast_proxy =
                    RabbitMQRemoteBroadcastProxy::new(r.clone(), b.clone(), p.clone())
                        .await
                        .unwrap();
                Ok::<_, lapin::Error>((*worker_id, proxy, broadcast_proxy))
            }
        }))
        .await?
        .into_iter()
        .for_each(|(worker_id, proxy, broadcast_proxy)| {
            proxies.insert(
                worker_id,
                (
                    Box::new(proxy) as Box<dyn RemoteSendReceiveProxy>,
                    Box::new(broadcast_proxy) as Box<dyn RemoteBroadcastProxy>,
                ),
            );
        });

        return Ok(proxies);
    }
}

// DIRECT PROXIES

pub struct RabbitMQProxy {
    sender: Box<dyn RemoteSendProxy>,
    receiver: Box<dyn RemoteReceiveProxy>,
}

pub struct RabbitMQSendProxy {
    pool: Pool,
    rabbitmq_options: Arc<RabbitMQOptions>,
    burst_options: Arc<BurstOptions>,
}

pub struct RabbitMQReceiveProxy {
    rabbitmq_options: Arc<RabbitMQOptions>,
    consumer: Consumer,
}

impl RemoteSendReceiveProxy for RabbitMQProxy {}

#[async_trait]
impl RemoteSendProxy for RabbitMQProxy {
    async fn remote_send(&self, dest: u32, msg: RemoteMessage) -> Result<()> {
        self.sender.remote_send(dest, msg).await
    }
}

#[async_trait]
impl RemoteReceiveProxy for RabbitMQProxy {
    async fn remote_recv(&self, source: u32) -> Result<RemoteMessage> {
        self.receiver.remote_recv(source).await
    }
}

impl RabbitMQProxy {
    pub async fn new(
        rabbitmq_options: Arc<RabbitMQOptions>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
        pool: Pool,
    ) -> Result<Self> {
        Ok(Self {
            sender: Box::new(
                RabbitMQSendProxy::new(
                    rabbitmq_options.clone(),
                    burst_options.clone(),
                    pool.clone(),
                )
                .await?,
            ),
            receiver: Box::new(
                RabbitMQReceiveProxy::new(worker_id, rabbitmq_options, burst_options, pool).await?,
            ),
        })
    }
}

#[async_trait]
impl RemoteSendProxy for RabbitMQSendProxy {
    async fn remote_send(&self, dest: u32, msg: RemoteMessage) -> Result<()> {
        send_direct(
            &self.pool,
            &msg,
            dest,
            &self.rabbitmq_options,
            &self.burst_options,
        )
        .await
    }
}

impl RabbitMQSendProxy {
    pub async fn new(
        rabbitmq_options: Arc<RabbitMQOptions>,
        burst_options: Arc<BurstOptions>,
        pool: Pool,
    ) -> Result<Self> {
        Ok(Self {
            pool,
            rabbitmq_options,
            burst_options,
        })
    }
}

#[async_trait]
impl RemoteReceiveProxy for RabbitMQReceiveProxy {
    async fn remote_recv(&self, _source: u32) -> Result<RemoteMessage> {
        let delivery = self.consumer.clone().try_next().await?;
        let delivery = delivery.ok_or("No RemoteMessage received")?;
        log::debug!(
            "RabbitMQ Basic consume, routing key: {:?}, exchange: {:?}",
            delivery.routing_key,
            delivery.exchange
        );
        if self.rabbitmq_options.ack {
            delivery.ack(BasicAckOptions::default()).await?;
        }
        Ok(parse_delivery(delivery))
    }
}

impl RabbitMQReceiveProxy {
    pub async fn new(
        worker_id: u32,
        rabbitmq_options: Arc<RabbitMQOptions>,
        burst_options: Arc<BurstOptions>,
        pool: Pool,
    ) -> Result<Self> {
        let connection = pool.get().await?;
        let channel = connection.create_channel().await?;
        let queue_name = get_queue_name(
            &rabbitmq_options.queue_prefix,
            &burst_options.burst_id,
            worker_id,
        );
        let consumer = channel
            .basic_consume(
                &queue_name,
                &get_consumer_tag(),
                BasicConsumeOptions {
                    no_ack: !rabbitmq_options.ack,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        Ok(Self {
            rabbitmq_options,
            consumer,
        })
    }
}

// BROADCAST PROXIES

pub struct RabbitMQRemoteBroadcastProxy {
    broadcast_sender: Box<dyn RemoteBroadcastSendProxy>,
    broadcast_receiver: Box<dyn RemoteBroadcastReceiveProxy>,
}

pub struct RabbitMQRemoteBroadcastSendProxy {
    pool: Pool,
    rabbitmq_options: Arc<RabbitMQOptions>,
    burst_options: Arc<BurstOptions>,
}

pub struct RabbitMQRemoteBroadcastReceiveProxy {
    rabbitmq_options: Arc<RabbitMQOptions>,
    consumer: Consumer,
}

impl RemoteBroadcastProxy for RabbitMQRemoteBroadcastProxy {}

impl RabbitMQRemoteBroadcastProxy {
    pub async fn new(
        rabbitmq_options: Arc<RabbitMQOptions>,
        burst_options: Arc<BurstOptions>,
        pool: Pool,
    ) -> Result<Self> {
        Ok(Self {
            broadcast_sender: Box::new(
                RabbitMQRemoteBroadcastSendProxy::new(
                    rabbitmq_options.clone(),
                    burst_options.clone(),
                    pool.clone(),
                )
                .await?,
            ),
            broadcast_receiver: Box::new(
                RabbitMQRemoteBroadcastReceiveProxy::new(
                    rabbitmq_options.clone(),
                    burst_options.clone(),
                    pool.clone(),
                )
                .await?,
            ),
        })
    }
}

#[async_trait]
impl RemoteBroadcastSendProxy for RabbitMQRemoteBroadcastProxy {
    async fn remote_broadcast_send(&self, msg: RemoteMessage) -> Result<()> {
        self.broadcast_sender.remote_broadcast_send(msg).await
    }
}

#[async_trait]
impl RemoteBroadcastReceiveProxy for RabbitMQRemoteBroadcastProxy {
    async fn remote_broadcast_recv(&self) -> Result<RemoteMessage> {
        self.broadcast_receiver.remote_broadcast_recv().await
    }
}

#[async_trait]
impl RemoteBroadcastSendProxy for RabbitMQRemoteBroadcastSendProxy {
    async fn remote_broadcast_send(&self, msg: RemoteMessage) -> Result<()> {
        send_broadcast(
            &self.pool,
            &msg,
            &self.rabbitmq_options,
            &self.burst_options,
        )
        .await
    }
}

impl RabbitMQRemoteBroadcastSendProxy {
    pub async fn new(
        rabbitmq_options: Arc<RabbitMQOptions>,
        burst_options: Arc<BurstOptions>,
        pool: Pool,
    ) -> Result<Self> {
        Ok(Self {
            pool,
            rabbitmq_options,
            burst_options,
        })
    }
}

#[async_trait]
impl RemoteBroadcastReceiveProxy for RabbitMQRemoteBroadcastReceiveProxy {
    async fn remote_broadcast_recv(&self) -> Result<RemoteMessage> {
        // log::debug!("RabbitMQ Basic consume");
        let delivery = self.consumer.clone().try_next().await?;
        let delivery = delivery.ok_or("No RemoteMessage received")?;
        log::debug!(
            "RabbitMQ Basic consume, routing key: {:?}, exchange: {:?}",
            delivery.routing_key,
            delivery.exchange
        );
        if self.rabbitmq_options.ack {
            delivery.ack(BasicAckOptions::default()).await?;
        }
        Ok(parse_delivery(delivery))
    }
}

impl RabbitMQRemoteBroadcastReceiveProxy {
    pub async fn new(
        rabbitmq_options: Arc<RabbitMQOptions>,
        burst_options: Arc<BurstOptions>,
        pool: Pool,
    ) -> Result<Self> {
        let broadcast_channel = pool.get().await?.create_channel().await?;
        let broadcast_queue = get_broadcast_queue_name(
            &rabbitmq_options.broadcast_queue_prefix,
            &burst_options.burst_id,
            &burst_options.group_id,
        );
        let broadcast_consumer = broadcast_channel
            .basic_consume(
                &broadcast_queue,
                &get_consumer_tag(),
                BasicConsumeOptions {
                    no_ack: !rabbitmq_options.ack,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        Ok(Self {
            rabbitmq_options,
            consumer: broadcast_consumer,
        })
    }
}

// Helper Functions

async fn init_rabbit(
    pool: Pool,
    burst_options: Arc<BurstOptions>,
    rabbitmq_options: Arc<RabbitMQOptions>,
) -> Result<()> {
    let connection = pool.get().await?;
    let channel = connection.create_channel().await?;

    // Declare direct exchange
    let direct_exchange = get_direct_exchange_name(
        &rabbitmq_options.direct_exchange_prefix,
        &burst_options.burst_id,
    );

    let mut options = ExchangeDeclareOptions::default();
    options.durable = rabbitmq_options.durable_exchanges;

    channel
        .exchange_declare(
            &direct_exchange,
            ExchangeKind::Direct,
            options,
            FieldTable::default(),
        )
        .await?;

    // Declare broadcast exchange of type topic
    let mut options = ExchangeDeclareOptions::default();
    options.durable = rabbitmq_options.durable_exchanges;

    channel
        .exchange_declare(
            get_broadcast_exchange_name(
                &rabbitmq_options.broadcast_exchange_prefix,
                &burst_options.burst_id,
            )
            .leak(),
            ExchangeKind::Topic,
            options,
            FieldTable::default(),
        )
        .await?;

    // Declare all queues and bind them to the direct exchange
    let mut options = QueueDeclareOptions::default();
    options.durable = rabbitmq_options.durable_queues;

    let ch = Arc::new(channel.clone());
    let exchange = Arc::new(direct_exchange);
    let boptions = burst_options.clone();
    let roptions = rabbitmq_options.clone();

    futures::future::try_join_all(burst_options.group_ranges.iter().map(
        |(group_id, worker_ids)| {
            let ch = ch.clone();
            let exchange = exchange.clone();
            let boptions = boptions.clone();
            let roptions = roptions.clone();
            async move {
                // Declare group broadcast queue
                let queue_name = get_broadcast_queue_name(
                    &roptions.broadcast_queue_prefix,
                    &boptions.burst_id,
                    group_id,
                );
                let q = ch
                    .queue_declare(queue_name.leak(), options, FieldTable::default())
                    .await?;
                // Bind queue to broadcast exchange
                ch.queue_bind(
                    q.name().as_str(),
                    &get_broadcast_exchange_name(
                        &roptions.broadcast_exchange_prefix,
                        &boptions.burst_id,
                    ),
                    &get_broadcast_subscribe_routing_key(group_id),
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await?;
                // Declare worker queues
                futures::future::try_join_all(worker_ids.iter().map(|id| {
                    let ch = ch.clone();
                    let exchange = exchange.clone();
                    let boptions = boptions.clone();
                    let roptions = roptions.clone();
                    let queue_name =
                        get_queue_name(&roptions.queue_prefix, &boptions.burst_id, *id);
                    async move {
                        let q = ch
                            .queue_declare(&queue_name, options, FieldTable::default())
                            .await?;
                        // Bind queue to direct exchange
                        ch.queue_bind(
                            q.name().as_str(),
                            &exchange,
                            q.name().as_str(),
                            QueueBindOptions::default(),
                            FieldTable::default(),
                        )
                        .await?;
                        Ok::<_, lapin::Error>(())
                    }
                }))
                .await?;
                Ok::<_, lapin::Error>(())
            }
        },
    ))
    .await?;

    Ok(())
}

async fn send_direct(
    pool: &Pool,
    msg: &RemoteMessage,
    dest: u32,
    rabbitmq_options: &RabbitMQOptions,
    burst_options: &BurstOptions,
) -> Result<()> {
    send_rabbit(
        pool,
        msg,
        &get_direct_exchange_name(
            &rabbitmq_options.direct_exchange_prefix,
            &burst_options.burst_id,
        ),
        &get_queue_name(
            &rabbitmq_options.queue_prefix,
            &burst_options.burst_id,
            dest,
        ),
    )
    .await
}

async fn send_broadcast(
    pool: &Pool,
    msg: &RemoteMessage,
    rabbitmq_options: &RabbitMQOptions,
    burst_options: &BurstOptions,
) -> Result<()> {
    let routing_key = burst_options
        .group_ranges
        .keys()
        .filter(|g| *g != &burst_options.group_id)
        .map(|g| g.as_str())
        .collect::<Vec<_>>()
        .join(".");

    log::debug!(
        "GROUP {} => sending broadcast to routing key: {}",
        burst_options.group_id,
        routing_key
    );

    send_rabbit(
        pool,
        msg,
        &get_broadcast_exchange_name(
            &rabbitmq_options.broadcast_exchange_prefix,
            &burst_options.burst_id,
        ),
        &routing_key,
    )
    .await
}

async fn send_rabbit(
    pool: &Pool,
    msg: &RemoteMessage,
    exchange: &str,
    routing_key: &str,
) -> Result<()> {
    let connection = pool.get().await?;
    let channel = connection.create_channel().await?;

    log::debug!(
        "RabbitMQ Basic publish, exchange: {:?}, routing_key: {:?}",
        exchange,
        routing_key
    );

    channel
        .basic_publish(
            exchange,
            routing_key,
            BasicPublishOptions::default(),
            &msg.data,
            BasicProperties::default().with_headers(create_headers(msg)),
        )
        .await?;
    Ok(())
}

fn get_direct_exchange_name(prefix: &str, burst_id: &str) -> String {
    format!("{}_{}", prefix, burst_id)
}

fn get_broadcast_exchange_name(prefix: &str, burst_id: &str) -> String {
    format!("{}_{}", prefix, burst_id)
}

fn get_broadcast_subscribe_routing_key(group_id: &str) -> String {
    format!("#.{}.#", group_id)
}

fn get_queue_name(prefix: &str, burst_id: &str, worker_id: u32) -> String {
    format!("{}_{}_worker_{}", prefix, burst_id, worker_id)
}

fn get_broadcast_queue_name(prefix: &str, burst_id: &str, group_id: &str) -> String {
    format!("{}_{}_group_{}", prefix, burst_id, group_id)
}

fn get_consumer_tag() -> String {
    format!("consumer_{}", Uuid::new_v4())
}

fn create_headers(msg: &RemoteMessage) -> FieldTable {
    let mut fields = FieldTable::default();
    fields.insert("sender_id".into(), AMQPValue::LongUInt(msg.metadata.sender_id));
    fields.insert("chunk_id".into(), AMQPValue::LongUInt(msg.metadata.chunk_id));
    fields.insert("num_chunks".into(), AMQPValue::LongUInt(msg.metadata.num_chunks));
    fields.insert("counter".into(), AMQPValue::LongUInt(msg.metadata.counter));
    fields.insert(
        "collective".into(),
        AMQPValue::LongUInt(msg.metadata.collective as u32),
    );
    fields
}

fn parse_delivery(delivery: Delivery) -> RemoteMessage {
    let data = Bytes::from(delivery.data);
    let map = delivery.properties.headers().as_ref().unwrap().inner();

    let sender_id = map.get("sender_id").unwrap().as_long_uint().unwrap();
    let chunk_id = map.get("chunk_id").unwrap().as_long_uint().unwrap();
    let num_chunks = map.get("num_chunks").unwrap().as_long_uint().unwrap();
    let counter = map.get("counter").unwrap().as_long_uint().unwrap();
    let collective = map
        .get("collective")
        .unwrap()
        .as_long_uint()
        .unwrap()
        .into();

    RemoteMessage {
        metadata: MessageMetadata {
            sender_id,
            chunk_id,
            num_chunks,
            counter,
            collective,
        },
        data,
    }
}
