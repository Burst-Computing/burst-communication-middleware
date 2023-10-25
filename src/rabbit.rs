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
    impl_chainable_setter, CollectiveType, Message, MiddlewareOptions, ReceiveProxy, Result,
    SendProxy, SendReceiveFactory, SendReceiveProxy,
};

#[derive(Clone, Debug)]
pub struct RabbitMQOptions {
    pub rabbitmq_uri: String,
    pub direct_exchange_prefix: String,
    pub broadcast_exchange_prefix: String,
    pub queue_prefix: String,
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
            ack: true,
            durable_exchanges: true,
            durable_queues: true,
        }
    }
}

pub struct RabbitMQMiddleware {
    middleware_options: MiddlewareOptions,
    rabbitmq_options: RabbitMQOptions,
    connection: Arc<Connection>,
    group_id: u32,
}

#[async_trait]
impl SendReceiveFactory for RabbitMQMiddleware {
    async fn create_remote_proxies(&self) -> Result<HashMap<u32, Box<dyn SendReceiveProxy>>> {
        self.init_rabbit().await?;

        let mut hmap = HashMap::new();

        futures::future::try_join_all(self.middleware_options.local_range.clone().map(
            |worker_id| async move {
                RabbitMQProxy::new(
                    self.connection.clone(),
                    self.rabbitmq_options.clone(),
                    self.middleware_options.clone(),
                    worker_id,
                    self.group_id,
                )
                .await
            },
        ))
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

impl RabbitMQMiddleware {
    pub async fn new(
        middleware_options: MiddlewareOptions,
        rabbitmq_options: RabbitMQOptions,
        group_id: u32,
    ) -> Result<Self> {
        let connection =
            Connection::connect(&rabbitmq_options.rabbitmq_uri, Default::default()).await?;
        Ok(Self {
            middleware_options,
            rabbitmq_options,
            connection: Arc::new(connection),
            group_id,
        })
    }

    async fn init_rabbit(&self) -> Result<()> {
        let channel = self.connection.create_channel().await?;

        // Declare direct exchange
        let mut options = ExchangeDeclareOptions::default();
        options.durable = self.rabbitmq_options.durable_exchanges;

        channel
            .exchange_declare(
                &get_direct_exchange_name(&self.rabbitmq_options, &self.middleware_options),
                ExchangeKind::Direct,
                options,
                FieldTable::default(),
            )
            .await?;

        // Declare broadcast exchanges for each group
        let mut options = ExchangeDeclareOptions::default();
        options.durable = self.rabbitmq_options.durable_exchanges;

        let broadcast_exchange = get_broadcast_exchange_name(
            &self.rabbitmq_options,
            &self.middleware_options,
            self.group_id,
        );

        futures::future::try_join_all(self.middleware_options.broadcast_range.clone().map(|id| {
            channel.exchange_declare(
                &broadcast_exchange,
                ExchangeKind::Fanout,
                options,
                FieldTable::default(),
            )
        }))
        .await?;

        // Declare all queues
        let mut options = QueueDeclareOptions::default();
        options.durable = self.rabbitmq_options.durable_queues;

        let mut local_queues = HashMap::new();

        let queues =
            futures::future::try_join_all(self.middleware_options.global_range.clone().map(|id| {
                let queue_name =
                    get_queue_name(&self.rabbitmq_options, &self.middleware_options, id);
                if self.middleware_options.local_range.contains(&id) {
                    local_queues.insert(id, queue_name.clone());
                }
                channel.queue_declare(queue_name.leak(), options, FieldTable::default())
            }))
            .await?;

        // Bind queues to exchange
        futures::future::try_join_all(queues.iter().map(|queue| {
            channel.queue_bind(
                queue.name().as_str(),
                &self.rabbitmq_options.direct_exchange_prefix,
                queue.name().as_str(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
        }))
        .await?;

        // Bind local queues to this group's broadcast exchange
        futures::future::try_join_all(local_queues.values().map(|queue| {
            channel.queue_bind(
                queue,
                &broadcast_exchange,
                queue,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
        }))
        .await?;

        channel.close(200, "Bye").await?;

        Ok(())
    }
}

pub struct RabbitMQProxy {
    channel: Channel,
    rabbitmq_options: RabbitMQOptions,
    middleware_options: MiddlewareOptions,
    worker_id: u32,
    group_id: u32,
    receiver: Box<dyn ReceiveProxy>,
    sender: Box<dyn SendProxy>,
}

pub struct RabbitMQSendProxy {
    channel: Channel,
    rabbitmq_options: RabbitMQOptions,
    middleware_options: MiddlewareOptions,
    worker_id: u32,
}

pub struct RabbitMQReceiveProxy {
    channel: Channel,
    options: RabbitMQOptions,
    consumer: Consumer,
}

#[async_trait]
impl SendReceiveProxy for RabbitMQProxy {
    async fn broadcast(&self, msg: &Message) -> Result<()> {
        if msg.collective != CollectiveType::Broadcast {
            Err("Cannot send non-broadcast message to broadcast".into())
        } else {
            futures::future::try_join_all(
                self.middleware_options
                    .broadcast_range
                    .clone()
                    .filter(|&dest| dest != self.group_id)
                    .map(|dest| {
                        send_rabbit(
                            &self.channel,
                            msg,
                            dest,
                            true,
                            &self.rabbitmq_options,
                            &self.middleware_options,
                            Some(self.group_id),
                        )
                    }),
            )
            .await?;
            Ok(())
        }
    }
}

#[async_trait]
impl SendProxy for RabbitMQProxy {
    async fn send(&self, dest: u32, msg: &Message) -> Result<()> {
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
        rabbitmq_options: RabbitMQOptions,
        middleware_options: MiddlewareOptions,
        worker_id: u32,
        group_id: u32,
    ) -> Result<Self> {
        let channel = connection.create_channel().await?;
        let ropt = rabbitmq_options.clone();
        let mopt = middleware_options.clone();

        Ok(Self {
            channel,
            rabbitmq_options: ropt,
            middleware_options: mopt,
            worker_id,
            group_id,
            sender: Box::new(
                RabbitMQSendProxy::new(
                    connection.clone(),
                    rabbitmq_options.clone(),
                    middleware_options.clone(),
                    worker_id,
                )
                .await?,
            ),
            receiver: Box::new(
                RabbitMQReceiveProxy::new(
                    connection,
                    rabbitmq_options.clone(),
                    middleware_options.clone(),
                    worker_id,
                )
                .await?,
            ),
        })
    }
}

#[async_trait]
impl SendProxy for RabbitMQSendProxy {
    async fn send(&self, dest: u32, msg: &Message) -> Result<()> {
        if msg.collective == CollectiveType::Broadcast {
            Err("Cannot send broadcast message to a single destination".into())
        } else {
            send_rabbit(
                &self.channel,
                msg,
                dest,
                false,
                &self.rabbitmq_options,
                &self.middleware_options,
                None,
            )
            .await?;
            Ok(())
        }
    }
}

impl RabbitMQSendProxy {
    pub async fn new(
        connection: Arc<Connection>,
        rabbitmq_options: RabbitMQOptions,
        middleware_options: MiddlewareOptions,
        worker_id: u32,
    ) -> Result<Self> {
        let channel = connection.create_channel().await?;
        Ok(Self {
            channel,
            rabbitmq_options,
            middleware_options,
            worker_id,
        })
    }
}

#[async_trait]
impl ReceiveProxy for RabbitMQReceiveProxy {
    async fn recv(&self) -> Result<Message> {
        let delivery = self.consumer.clone().next().await.unwrap()?;
        if self.options.ack {
            self.channel
                .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                .await?;
        }
        Ok(get_message(delivery))
    }
}

impl RabbitMQReceiveProxy {
    pub async fn new(
        connection: Arc<Connection>,
        rabbitmq_options: RabbitMQOptions,
        middleware_options: MiddlewareOptions,
        worker_id: u32,
    ) -> Result<Self> {
        let channel = connection.create_channel().await?;
        let consumer = channel
            .basic_consume(
                &get_queue_name(&rabbitmq_options, &middleware_options, worker_id),
                &get_consumer_tag(&rabbitmq_options, &middleware_options, worker_id),
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

async fn send_rabbit(
    channel: &Channel,
    msg: &Message,
    dest: u32,
    broadcast: bool,
    rabbitmq_options: &RabbitMQOptions,
    middleware_options: &MiddlewareOptions,
    group_id: Option<u32>,
) -> Result<()> {
    let mut fields = FieldTable::default();
    fields.insert("sender_id".into(), AMQPValue::LongUInt(msg.sender_id));
    fields.insert("chunk_id".into(), AMQPValue::LongUInt(msg.chunk_id));
    fields.insert("last_chunk".into(), AMQPValue::Boolean(msg.last_chunk));
    if let Some(counter) = msg.counter {
        fields.insert("counter".into(), AMQPValue::LongUInt(counter));
    }
    fields.insert(
        "collective".into(),
        AMQPValue::LongUInt(msg.collective as u32),
    );

    let (exchange, routing_key) = if broadcast {
        (
            get_broadcast_exchange_name(rabbitmq_options, middleware_options, group_id.unwrap()),
            "".into(),
        )
    } else {
        (
            get_direct_exchange_name(rabbitmq_options, middleware_options),
            get_queue_name(rabbitmq_options, middleware_options, dest),
        )
    };

    channel
        .basic_publish(
            &exchange,
            &routing_key,
            BasicPublishOptions::default(),
            &msg.data,
            BasicProperties::default().with_headers(fields),
        )
        .await?;
    Ok(())
}

fn get_direct_exchange_name(
    rabbitmq_options: &RabbitMQOptions,
    middleware_options: &MiddlewareOptions,
) -> String {
    format!(
        "{}_{}",
        rabbitmq_options.direct_exchange_prefix, middleware_options.burst_id
    )
}

fn get_broadcast_exchange_name(
    rabbitmq_options: &RabbitMQOptions,
    middleware_options: &MiddlewareOptions,
    group_id: u32,
) -> String {
    format!(
        "{}_{}_group_{}",
        rabbitmq_options.broadcast_exchange_prefix, middleware_options.burst_id, group_id
    )
}

fn get_queue_name(
    rabbitmq_options: &RabbitMQOptions,
    middleware_options: &MiddlewareOptions,
    worker_id: u32,
) -> String {
    format!(
        "{}_{}_worker_{}",
        rabbitmq_options.queue_prefix, middleware_options.burst_id, worker_id
    )
}

fn get_consumer_tag(
    _rabbitmq_options: &RabbitMQOptions,
    _middleware_options: &MiddlewareOptions,
    _worker_id: u32,
) -> String {
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
    let last_chunk = delivery
        .properties
        .headers()
        .as_ref()
        .unwrap()
        .inner()
        .get("last_chunk")
        .unwrap()
        .as_bool()
        .unwrap();
    let counter = delivery
        .properties
        .headers()
        .as_ref()
        .unwrap()
        .inner()
        .get("counter");
    let counter = match counter {
        Some(counter) => Some(counter.as_long_uint().unwrap()),
        None => None,
    };
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
        last_chunk,
        counter,
        collective,
        data,
    }
}
