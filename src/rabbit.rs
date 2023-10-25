use std::{collections::HashMap, ops::Range, sync::Arc};

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

use crate::{
    impl_chainable_setter, types::Message, types::Result, CollectiveType, MiddlewareOptions,
    ReceiveProxy, SendProxy, SendReceiveFactory, SendReceiveProxy,
};

#[derive(Clone, Debug)]
pub struct RabbitMQOptions {
    pub rabbitmq_uri: String,
    pub direct_exchange: String,
    pub broadcast_exchange_prefix: String,
    pub queue_prefix: String,
    pub ack: bool,
}

impl RabbitMQOptions {
    pub fn new(rabbitmq_uri: String) -> Self {
        Self {
            rabbitmq_uri,
            ..Default::default()
        }
    }

    impl_chainable_setter! {
        direct_exchange, String
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

    pub fn build(&self) -> Self {
        self.clone()
    }
}

impl Default for RabbitMQOptions {
    fn default() -> Self {
        Self {
            rabbitmq_uri: "amqp://guest:guest@localhost:5672".into(),
            direct_exchange: "burst".into(),
            broadcast_exchange_prefix: "burst_broadcast".into(),
            queue_prefix: "queue".into(),
            ack: false,
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
                    self.middleware_options.broadcast_range.clone(),
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
        options.durable = true;

        channel
            .exchange_declare(
                &self.rabbitmq_options.direct_exchange,
                ExchangeKind::Direct,
                options,
                FieldTable::default(),
            )
            .await?;

        // Declare broadcast exchanges for each group
        let mut options = ExchangeDeclareOptions::default();
        options.durable = true;

        futures::future::try_join_all(self.middleware_options.broadcast_range.clone().map(|id| {
            channel.exchange_declare(
                format!(
                    "{}_{}",
                    &self.rabbitmq_options.broadcast_exchange_prefix, id
                )
                .leak(),
                ExchangeKind::Fanout,
                options,
                FieldTable::default(),
            )
        }))
        .await?;

        // Declare all queues
        let mut options = QueueDeclareOptions::default();
        options.durable = true;

        let mut local_queues = HashMap::new();

        let queues =
            futures::future::try_join_all(self.middleware_options.global_range.clone().map(|id| {
                if self.middleware_options.local_range.contains(&id) {
                    local_queues.insert(
                        id,
                        format!("{}_{}", &self.rabbitmq_options.queue_prefix, id),
                    );
                }
                channel.queue_declare(
                    format!("{}_{}", &self.rabbitmq_options.queue_prefix, id).leak(),
                    options,
                    FieldTable::default(),
                )
            }))
            .await?;

        // Bind queues to exchange
        futures::future::try_join_all(queues.iter().map(|queue| {
            channel.queue_bind(
                queue.name().as_str(),
                &self.rabbitmq_options.direct_exchange,
                queue.name().as_str(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
        }))
        .await?;

        // Bind local queues to this group's broadcast exchange
        let exchange_name = format!(
            "{}_{}",
            &self.rabbitmq_options.broadcast_exchange_prefix, self.group_id
        );
        futures::future::try_join_all(local_queues.values().map(|queue| {
            channel.queue_bind(
                queue,
                &exchange_name,
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
    options: RabbitMQOptions,
    broadcast_range: Range<u32>,
    worker_id: u32,
    group_id: u32,
    receiver: Box<dyn ReceiveProxy>,
    sender: Box<dyn SendProxy>,
}

pub struct RabbitMQSendProxy {
    channel: Channel,
    options: RabbitMQOptions,
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
                self.broadcast_range
                    .clone()
                    .filter(|&dest| dest != self.group_id)
                    .map(|dest| {
                        send_rabbit(
                            self.worker_id,
                            msg.chunk_id,
                            msg.last_chunk,
                            msg.counter,
                            msg.collective,
                            &msg.data,
                            &self.channel,
                            format!("{}_{}", self.options.broadcast_exchange_prefix, dest).leak(),
                            "",
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
        options: RabbitMQOptions,
        broadcast_range: Range<u32>,
        worker_id: u32,
        group_id: u32,
    ) -> Result<Self> {
        let channel = connection.create_channel().await?;
        let opt = options.clone();

        Ok(Self {
            channel,
            options: opt,
            broadcast_range,
            worker_id,
            group_id,
            sender: Box::new(
                RabbitMQSendProxy::new(connection.clone(), options.clone(), worker_id).await?,
            ),
            receiver: Box::new(
                RabbitMQReceiveProxy::new(connection, options.clone(), worker_id).await?,
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
                self.worker_id,
                msg.chunk_id,
                msg.last_chunk,
                msg.counter,
                msg.collective,
                &msg.data,
                &self.channel,
                &self.options.direct_exchange,
                &format!("{}_{}", self.options.queue_prefix, dest),
            )
            .await
        }
    }
}

impl RabbitMQSendProxy {
    pub async fn new(
        connection: Arc<Connection>,
        options: RabbitMQOptions,
        worker_id: u32,
    ) -> Result<Self> {
        let channel = connection.create_channel().await?;
        Ok(Self {
            channel,
            options,
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
        options: RabbitMQOptions,
        worker_id: u32,
    ) -> Result<Self> {
        let channel = connection.create_channel().await?;
        let consumer = channel
            .basic_consume(
                &format!("{}_{}", options.queue_prefix, worker_id),
                &format!("{}_{} consumer", options.queue_prefix, worker_id),
                BasicConsumeOptions {
                    no_ack: !options.ack,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;
        Ok(Self {
            channel,
            options,
            consumer,
        })
    }
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

async fn send_rabbit(
    sender_id: u32,
    chunk_id: u32,
    last_chunk: bool,
    counter: Option<u32>,
    collective: CollectiveType,
    data: &[u8],
    channel: &Channel,
    exchange: &str,
    routing_key: &str,
) -> Result<()> {
    let mut fields = FieldTable::default();
    fields.insert("sender_id".into(), AMQPValue::LongUInt(sender_id));
    fields.insert("chunk_id".into(), AMQPValue::LongUInt(chunk_id));
    fields.insert("last_chunk".into(), AMQPValue::Boolean(last_chunk));
    if let Some(counter) = counter {
        fields.insert("counter".into(), AMQPValue::LongUInt(counter));
    }
    fields.insert("collective".into(), AMQPValue::LongUInt(collective as u32));
    channel
        .basic_publish(
            exchange,
            routing_key,
            BasicPublishOptions::default(),
            data,
            BasicProperties::default().with_headers(fields),
        )
        .await?;
    Ok(())
}
