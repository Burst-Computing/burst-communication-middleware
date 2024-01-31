use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;

use bytes::Bytes;
use futures::StreamExt;
use lapin::{
    message::Delivery,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions,
        QueueBindOptions, QueueDeclareOptions,
    },
    types::{AMQPValue, FieldTable},
    BasicProperties, Channel, Connection, Consumer, ExchangeKind,
};
use uuid::Uuid;

use crate::{
    impl_chainable_setter, BroadcastSendProxy, BurstOptions, CollectiveType, Message, ReceiveProxy,
    Result, SendProxy, SendReceiveFactory, SendReceiveProxy,
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
}

impl RabbitMQOptions {
    pub fn new(rabbitmq_uri: String) -> Self {
        Self {
            rabbitmq_uri,
            ..Default::default()
        }
    }

    impl_chainable_setter! {
        rabbitmq_uri, String
    }

    impl_chainable_setter! {
        direct_exchange_prefix, String
    }

    impl_chainable_setter! {
        broadcast_exchange_prefix, String
    }

    impl_chainable_setter! {
        queue_prefix, String
    }

    impl_chainable_setter! {
        ack, bool
    }

    impl_chainable_setter! {
        broadcast_queue_prefix, String
    }

    impl_chainable_setter! {
        durable_exchanges, bool
    }

    impl_chainable_setter! {
        durable_queues, bool
    }

    pub fn build(&self) -> Self {
        self.clone()
    }
}

impl Default for RabbitMQOptions {
    fn default() -> Self {
        Self {
            rabbitmq_uri: "amqp://guest:guest@localhost:5672".into(),
            direct_exchange_prefix: "burst_direct".into(),
            broadcast_exchange_prefix: "burst_fanout".into(),
            queue_prefix: "queue".into(),
            broadcast_queue_prefix: "broadcast_queue".into(),
            ack: true,
            durable_exchanges: true,
            durable_queues: true,
        }
    }
}

pub struct RabbitMQMImpl;

#[async_trait]
impl SendReceiveFactory<RabbitMQOptions> for RabbitMQMImpl {
    async fn create_proxies(
        burst_options: Arc<BurstOptions>,
        rabbitmq_options: RabbitMQOptions,
        broadcast_proxy: Box<dyn BroadcastSendProxy>,
    ) -> Result<(
        HashMap<u32, Box<dyn SendReceiveProxy>>,
        Box<dyn BroadcastSendProxy>,
    )> {
        let connection = Arc::new(
            Connection::connect(&rabbitmq_options.rabbitmq_uri, Default::default()).await?,
        );
        let rabbitmq_options = Arc::new(rabbitmq_options);
        init_rabbit(
            connection.clone(),
            burst_options.clone(),
            rabbitmq_options.clone(),
            broadcast_proxy,
        )
        .await?;

        let current_group = burst_options
            .group_ranges
            .get(&burst_options.group_id)
            .unwrap();

        let mut hmap = HashMap::new();

        futures::future::try_join_all(current_group.iter().map(|worker_id| {
            let c = connection.clone();
            let r = rabbitmq_options.clone();
            let b = burst_options.clone();
            async move { RabbitMQProxy::new(c.clone(), r.clone(), b.clone(), *worker_id).await }
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
            Box::new(
                RabbitMQBroadcastSendProxy::new(connection, rabbitmq_options, burst_options)
                    .await?,
            ) as Box<dyn BroadcastSendProxy>,
        ))
    }
}

async fn init_rabbit(
    connection: Arc<Connection>,
    burst_options: Arc<BurstOptions>,
    rabbitmq_options: Arc<RabbitMQOptions>,
    broadcast_proxy: Box<dyn BroadcastSendProxy>,
) -> Result<()> {
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

    // Declare broadcast exchanges for each group
    let mut options = ExchangeDeclareOptions::default();
    options.durable = rabbitmq_options.durable_exchanges;

    futures::future::try_join_all(burst_options.group_ranges.keys().map(|id| {
        channel.exchange_declare(
            get_broadcast_exchange_name(
                &rabbitmq_options.broadcast_exchange_prefix,
                &burst_options.burst_id,
                id,
            )
            .leak(),
            ExchangeKind::Fanout,
            options,
            FieldTable::default(),
        )
    }))
    .await?;

    // Declare all queues and bind them to the direct exchange
    let mut options = QueueDeclareOptions::default();
    options.durable = rabbitmq_options.durable_queues;

    let ch = Arc::new(channel.clone());
    let exchange = Arc::new(direct_exchange.clone());
    let boptions = burst_options.clone();
    let roptions = rabbitmq_options.clone();

    futures::future::try_join_all(burst_options.group_ranges.iter().map(
        move |(group_id, worker_ids)| {
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
                // Bind queue to its corresponding broadcast exchange
                ch.queue_bind(
                    q.name().as_str(),
                    &get_broadcast_exchange_name(
                        &roptions.broadcast_exchange_prefix,
                        &boptions.burst_id,
                        group_id,
                    ),
                    q.name().as_str(),
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await?;
                // Declare worker queues
                futures::future::try_join_all(worker_ids.iter().map(move |id| {
                    let ch = ch.clone();
                    let exchange = exchange.clone();
                    let boptions = boptions.clone();
                    let roptions = roptions.clone();
                    async move {
                        let queue_name =
                            get_queue_name(&roptions.queue_prefix, &boptions.burst_id, *id);
                        let q = ch
                            .queue_declare(queue_name.leak(), options, FieldTable::default())
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

    channel.close(200, "Bye").await?;

    // spawn task to receive broadcast messages and send them to the broadcast proxy
    let broadcast_channel = connection.create_channel().await?;
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
    let r = rabbitmq_options.clone();
    tokio::spawn(async move {
        let mut broadcast_consumer = broadcast_consumer;
        while let Some(delivery) = broadcast_consumer.next().await {
            let delivery = delivery.unwrap();
            if r.ack {
                broadcast_channel
                    .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                    .await
                    .unwrap();
            }
            let msg = get_message(delivery);
            broadcast_proxy.broadcast_send(msg).await.unwrap();
        }
    });

    Ok(())
}

pub struct RabbitMQProxy {
    worker_id: u32,
    receiver: Box<dyn ReceiveProxy>,
    sender: Box<dyn SendProxy>,
}

pub struct RabbitMQSendProxy {
    channel: Channel,
    rabbitmq_options: Arc<RabbitMQOptions>,
    burst_options: Arc<BurstOptions>,
}

pub struct RabbitMQReceiveProxy {
    channel: Channel,
    options: Arc<RabbitMQOptions>,
    consumer: Consumer,
}

pub struct RabbitMQBroadcastSendProxy {
    channel: Channel,
    rabbitmq_options: Arc<RabbitMQOptions>,
    burst_options: Arc<BurstOptions>,
}

impl SendReceiveProxy for RabbitMQProxy {}

#[async_trait]
impl SendProxy for RabbitMQProxy {
    async fn send(&self, dest: u32, msg: Message) -> Result<()> {
        self.sender.send(dest, msg).await
    }
}

#[async_trait]
impl ReceiveProxy for RabbitMQProxy {
    async fn recv(&self) -> Result<Message> {
        self.receiver.recv().await
    }
}

impl RabbitMQProxy {
    pub async fn new(
        connection: Arc<Connection>,
        rabbitmq_options: Arc<RabbitMQOptions>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
    ) -> Result<Self> {
        Ok(Self {
            worker_id,
            sender: Box::new(
                RabbitMQSendProxy::new(
                    connection.clone(),
                    rabbitmq_options.clone(),
                    burst_options.clone(),
                )
                .await?,
            ),
            receiver: Box::new(
                RabbitMQReceiveProxy::new(
                    connection,
                    rabbitmq_options.clone(),
                    burst_options.clone(),
                    worker_id,
                )
                .await?,
            ),
        })
    }
}

#[async_trait]
impl SendProxy for RabbitMQSendProxy {
    async fn send(&self, dest: u32, msg: Message) -> Result<()> {
        if msg.collective == CollectiveType::Broadcast {
            Err("Cannot send broadcast message to a single destination".into())
        } else {
            send_direct(
                &self.channel,
                msg,
                dest,
                &self.rabbitmq_options,
                &self.burst_options,
            )
            .await
        }
    }
}

impl RabbitMQSendProxy {
    pub async fn new(
        connection: Arc<Connection>,
        rabbitmq_options: Arc<RabbitMQOptions>,
        burst_options: Arc<BurstOptions>,
    ) -> Result<Self> {
        let channel = connection.create_channel().await?;
        Ok(Self {
            channel,
            rabbitmq_options,
            burst_options,
        })
    }
}

#[async_trait]
impl ReceiveProxy for RabbitMQReceiveProxy {
    async fn recv(&self) -> Result<Message> {
        let delivery = self.consumer.clone().next().await.unwrap()?;
        log::debug!(
            "RabbitMQ Basic consume, routing key: {:?}, exchange: {:?}",
            delivery.routing_key,
            delivery.exchange
        );
        if self.options.ack {
            self.channel
                .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                .await?;
        }
        Ok(get_message(delivery))
    }
}

impl RabbitMQBroadcastSendProxy {
    pub async fn new(
        connection: Arc<Connection>,
        rabbitmq_options: Arc<RabbitMQOptions>,
        burst_options: Arc<BurstOptions>,
    ) -> Result<Self> {
        let channel = connection.create_channel().await?;
        Ok(Self {
            channel,
            rabbitmq_options,
            burst_options,
        })
    }
}

#[async_trait]
impl BroadcastSendProxy for RabbitMQBroadcastSendProxy {
    async fn broadcast_send(&self, msg: Message) -> Result<()> {
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
                            &self.channel,
                            msg.clone(),
                            dest,
                            &self.rabbitmq_options,
                            &self.burst_options,
                        )
                    }),
            )
            .await?;
            Ok(())
        }
    }
}

impl RabbitMQReceiveProxy {
    pub async fn new(
        connection: Arc<Connection>,
        rabbitmq_options: Arc<RabbitMQOptions>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
    ) -> Result<Self> {
        let channel = connection.create_channel().await?;
        let consumer = channel
            .basic_consume(
                &get_queue_name(
                    &rabbitmq_options.queue_prefix,
                    &burst_options.burst_id,
                    worker_id,
                ),
                &get_consumer_tag(),
                BasicConsumeOptions {
                    no_ack: !rabbitmq_options.ack,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;
        Ok(Self {
            channel,
            options: rabbitmq_options,
            consumer,
        })
    }
}

async fn send_direct(
    channel: &Channel,
    msg: Message,
    dest: u32,
    rabbitmq_options: &RabbitMQOptions,
    burst_options: &BurstOptions,
) -> Result<()> {
    send_rabbit(
        channel,
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
    channel: &Channel,
    msg: Message,
    dest: &str,
    rabbitmq_options: &RabbitMQOptions,
    burst_options: &BurstOptions,
) -> Result<()> {
    send_rabbit(
        channel,
        msg,
        &get_broadcast_exchange_name(
            &rabbitmq_options.broadcast_exchange_prefix,
            &burst_options.burst_id,
            dest,
        ),
        "",
    )
    .await
}

async fn send_rabbit(
    channel: &Channel,
    msg: Message,
    exchange: &str,
    routing_key: &str,
) -> Result<()> {
    let mut fields = FieldTable::default();
    fields.insert("sender_id".into(), AMQPValue::LongUInt(msg.sender_id));
    fields.insert("chunk_id".into(), AMQPValue::LongUInt(msg.chunk_id));
    fields.insert("num_chunks".into(), AMQPValue::LongUInt(msg.num_chunks));
    fields.insert("counter".into(), AMQPValue::LongUInt(msg.counter));

    fields.insert(
        "collective".into(),
        AMQPValue::LongUInt(msg.collective as u32),
    );

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
            BasicProperties::default().with_headers(fields),
        )
        .await?;
    Ok(())
}

fn get_direct_exchange_name(prefix: &str, burst_id: &str) -> String {
    format!("{}_{}", prefix, burst_id)
}

fn get_broadcast_exchange_name(prefix: &str, burst_id: &str, group_id: &str) -> String {
    format!("{}_{}_group_{}", prefix, burst_id, group_id)
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

fn get_message(delivery: Delivery) -> Message {
    let data = Bytes::from(delivery.data);
    let sender_id = delivery
        .properties
        .headers()
        .as_ref()
        .unwrap()
        .inner()
        .get("sender_id")
        .unwrap()
        .as_long_uint()
        .unwrap();
    let chunk_id = delivery
        .properties
        .headers()
        .as_ref()
        .unwrap()
        .inner()
        .get("chunk_id")
        .unwrap()
        .as_long_uint()
        .unwrap();
    let num_chunks = delivery
        .properties
        .headers()
        .as_ref()
        .unwrap()
        .inner()
        .get("num_chunks")
        .unwrap()
        .as_long_uint()
        .unwrap();
    let counter = delivery
        .properties
        .headers()
        .as_ref()
        .unwrap()
        .inner()
        .get("counter")
        .unwrap()
        .as_long_uint()
        .unwrap();
    let collective = delivery
        .properties
        .headers()
        .as_ref()
        .unwrap()
        .inner()
        .get("collective")
        .unwrap()
        .as_long_uint()
        .unwrap()
        .into();
    Message {
        sender_id,
        chunk_id,
        num_chunks,
        counter,
        collective,
        data,
    }
}
