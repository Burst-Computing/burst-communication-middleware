use crate::types::Message;
use bytes::Bytes;
use futures::{FutureExt, StreamExt, TryFutureExt};
use lapin::{
    message::Delivery,
    options::{
        //BasicAckOptions,
        BasicConsumeOptions,
        BasicPublishOptions,
        ExchangeDeclareOptions,
        QueueBindOptions,
        QueueDeclareOptions,
    },
    protocol::exchange,
    types::{AMQPValue, FieldTable},
    BasicProperties, Channel, Connection, ConnectionProperties, Consumer, ExchangeKind,
};
use log;
use std::{
    collections::{HashMap, HashSet},
    ops::Range,
    sync::Arc,
};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct MiddlewareArguments {
    rabbitmq_uri: String,
    burst_id: String,
    burst_size: u32,
    group_id: u32,
    groups: u32,
    group_range: Range<u32>,
    durable_queues: bool,
    broadcast_channel_size: u32,
}

impl MiddlewareArguments {
    pub fn new(
        burst_id: String,
        burst_size: u32,
        groups: u32,
        group_id: u32,
        group_range: Range<u32>,
        rabbitmq_uri: String,
        durable_queues: bool,
        broadcast_channel_size: u32,
    ) -> Self {
        Self {
            burst_id,
            burst_size,
            groups,
            group_id,
            group_range,
            rabbitmq_uri,
            durable_queues,
            broadcast_channel_size,
        }
    }
}

pub struct BurstMiddleware {
    pub worker_id: u32,
    pub burst_id: String,
    pub burst_size: u32,
    pub group_id: u32,
    pub groups: u32,
    pub group_set: HashSet<u32>,
    channel: UnboundedReceiver<Message>,
    group_channels: Arc<HashMap<u32, UnboundedSender<Message>>>,
    broadcast_channel_tx: tokio::sync::broadcast::Sender<Message>,
    broadcast_channel_rx: tokio::sync::broadcast::Receiver<Message>,
    rabbitmq_channel: Channel,
    rabbitmq_consumer: Consumer,
}

pub async fn create_group_handlers(args: MiddlewareArguments) -> Result<Vec<BurstMiddleware>> {
    let group_set = args.group_range.collect::<HashSet<_>>();
    let group_size = group_set.len();

    let rabbitmq_conn =
        Connection::connect(&args.rabbitmq_uri, ConnectionProperties::default()).await?;
    let channel = rabbitmq_conn.create_channel().await?; // TODO: set publisher_confirms to true

    // Declare exchanges, one direct for worker queues and one fanout for each group
    let mut options = ExchangeDeclareOptions::default();
    options.durable = args.durable_queues;
    channel
        .exchange_declare(
            &format!("burst_{}_direct", args.burst_id).as_str(),
            ExchangeKind::Direct,
            options,
            FieldTable::default(),
        )
        .await?;
    futures::future::try_join_all((0..args.groups).map(|group_id| {
        let mut options = ExchangeDeclareOptions::default();
        options.durable = args.durable_queues;
        channel.exchange_declare(
            format!("burst_{}_fanout-group-{}", args.burst_id, group_id).leak(),
            ExchangeKind::Fanout,
            options,
            FieldTable::default(),
        )
    }))
    .await?;

    // Declare all worker queues and bind them to direct exchange
    let mut options = QueueDeclareOptions::default();
    options.durable = args.durable_queues;

    futures::future::try_join_all((0..args.burst_size).map(|worker_id| {
        channel.queue_declare(
            format!("burst_{}_worker-{}", args.burst_id.clone(), worker_id).leak(),
            options,
            FieldTable::default(),
        )
    }))
    .await?;

    futures::future::try_join_all((0..args.burst_size).map(|queue_name| {
        channel.queue_bind(
            format!("burst_{}_worker-{}", args.burst_id, queue_name).leak(),
            format!("burst_{}_direct", args.burst_id).leak(),
            format!("burst_{}_worker-{}", args.burst_id, queue_name).leak(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
    }))
    .await?;

    // Bind group queues to this group's exchange
    futures::future::try_join_all(group_set.iter().map(|worker_id| {
        channel.queue_bind(
            format!("burst_{}_worker-{}", args.burst_id, worker_id).leak(),
            format!("burst_{}_fanout-group-{}", args.burst_id, args.group_id).leak(),
            format!("burst_{}_worker-{}", args.burst_id, worker_id).leak(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
    }))
    .await?;

    channel.close(200, "Bye").await?;

    // Create local channels
    let mut tx_chans = HashMap::with_capacity(group_size);
    let mut rx_chans = Vec::with_capacity(group_size);
    for id in group_set.iter() {
        let (tx, rx) = mpsc::unbounded_channel::<Message>();
        tx_chans.insert(*id, tx);
        rx_chans.push((*id, rx));
    }

    let tx_chans = Arc::new(tx_chans);

    // create broadcast channel for this group
    let (broadcast_channel, _) =
        tokio::sync::broadcast::channel::<Message>(args.broadcast_channel_size as usize);

    // Initialize worker handlers
    let mut handlers = Vec::with_capacity(group_size);
    for (id, rx) in rx_chans {
        let rabbitmq_channel = rabbitmq_conn.create_channel().await?;
        let mut options = BasicConsumeOptions::default();
        options.no_ack = true;
        let consumer = rabbitmq_channel
            .basic_consume(
                &format!("burst_{}_worker-{}", args.burst_id, id),
                &format!("{}_{} consumer", args.burst_id, id),
                options,
                FieldTable::default(),
            )
            .await?;
        let burst_worker_handler = BurstMiddleware {
            burst_id: args.burst_id.clone(),
            burst_size: args.burst_size,
            worker_id: id,
            channel: rx,
            group_channels: tx_chans.clone(),
            broadcast_channel_tx: broadcast_channel.clone(),
            broadcast_channel_rx: broadcast_channel.subscribe(),
            groups: args.groups,
            group_id: args.group_id,
            group_set: group_set.clone(),
            rabbitmq_channel: rabbitmq_channel,
            rabbitmq_consumer: consumer,
        };
        handlers.push(burst_worker_handler);
    }
    Ok(handlers)
}

impl BurstMiddleware {
    pub async fn send(&self, target: u32, data: Bytes) -> Result<()> {
        let chunk_id = 0;
        let last_chunk = true;

        if target >= self.burst_size {
            return Err(format!("worker with id {} does not exist", target).into());
        }

        match self.group_channels.get(&target) {
            Some(tx) => {
                log::debug!(
                    "[worker {}] Sending message with size {} to worker {} through local channel",
                    self.worker_id,
                    data.len(),
                    target
                );
                // send to local channel
                tx.send(Message {
                    sender_id: self.worker_id,
                    data,
                    chunk_id,
                    last_chunk,
                })?;
            }
            None => {
                // send message trough rabbitmq
                log::debug!(
                    "[worker {}] Sending message with size {} to worker {} through rabbitmq",
                    self.worker_id,
                    data.len(),
                    target
                );
                let mut fields = FieldTable::default();
                fields.insert("sender_id".into(), AMQPValue::LongUInt(self.worker_id));
                fields.insert("chunk_id".into(), AMQPValue::LongUInt(chunk_id));
                fields.insert("last_chunk".into(), AMQPValue::Boolean(last_chunk));

                self.rabbitmq_channel
                    .basic_publish(
                        &format!("burst_{}_fanout-group-{}", self.burst_id, target),
                        &format!("burst_{}_worker-{}", self.burst_id, target),
                        BasicPublishOptions::default(),
                        &data,
                        BasicProperties::default().with_headers(fields),
                    )
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn recv(&mut self) -> Result<Message> {
        // receive blocking
        let handle = async {
            let msg = self.channel.recv().await.unwrap();
            log::debug!(
                "[worker {}] Received message with size {} from worker {} through local channel",
                self.worker_id,
                msg.data.len(),
                msg.sender_id
            );
            Ok::<Message, Error>(msg)
        };

        // Receive from rabbitmq
        let fut = async {
            // receive blocking
            let delivery = self.rabbitmq_consumer.next().await.unwrap()?;
            //delivery.ack(BasicAckOptions::default()).await?;
            let msg = Self::get_rabbitmq_message(delivery);
            log::debug!(
                "[worker {}] Received message with size {} from worker {} through rabbitmq",
                self.worker_id,
                msg.data.len(),
                msg.sender_id
            );
            Ok::<Message, Error>(msg)
        };

        tokio::select! {
            msg = fut => Ok(msg?),
            msg = handle => Ok(msg?),
        }
    }

    pub async fn broadcast(&mut self, data: Option<Bytes>) -> Result<Message> {
        if let Some(data) = data {
            log::debug!(
                "[worker {}] Broadcasting message with size {}",
                self.worker_id,
                data.len()
            );
            let chunk_id = 0;
            let last_chunk = true;
            let msg = Message {
                sender_id: self.worker_id,
                data: data.clone(),
                chunk_id,
                last_chunk,
            };

            self.broadcast_channel_tx.send(msg)?;

            futures::future::try_join_all(
                (0..self.groups)
                    .filter(|target_group| *target_group != self.group_id)
                    .map(|target_group| {
                        let mut fields = FieldTable::default();
                        fields.insert("sender_id".into(), AMQPValue::LongUInt(self.worker_id));
                        fields.insert("chunk_id".into(), AMQPValue::LongUInt(chunk_id));
                        fields.insert("last_chunk".into(), AMQPValue::Boolean(last_chunk));

                        let exchange =
                            format!("burst_{}_fanout-group-{}", self.burst_id, target_group);
                        log::debug!(
                            "[worker {}] Sending broadcast message with size {} to exchange {}",
                            self.worker_id,
                            data.len(),
                            exchange
                        );

                        self.rabbitmq_channel.basic_publish(
                            exchange.leak(),
                            "",
                            BasicPublishOptions::default(),
                            &data,
                            BasicProperties::default().with_headers(fields),
                        )
                    }),
            )
            .await?;

            // Consume self broadcast message to avoid receiving it in the next receive
            let msg = self.broadcast_channel_rx.recv().await?;
            Ok::<Message, Error>(msg)
        } else {
            log::debug!("[worker {}] Receiving broadcast message", self.worker_id);
            // Receive from local channel
            let local = async {
                // receive blocking
                let msg = self.broadcast_channel_rx.recv().await?;
                log::debug!(
                    "[worker {}] Received broadcast message with size {} from worker {} through local channel",
                    self.worker_id,
                    msg.data.len(),
                    msg.sender_id
                );
                Ok::<Message, Error>(msg)
            };

            // Receive from rabbitmq
            let fut = async {
                // receive blocking
                let delivery = self.rabbitmq_consumer.next().await.unwrap()?;
                log::debug!(
                    "[worker {}] Received broadcast message with size {} from worker {} through rabbitmq",
                    self.worker_id,
                    delivery.data.len(),
                    delivery.properties.headers().as_ref().unwrap().inner().get("sender_id").unwrap().as_long_uint().unwrap()
                );
                //delivery.ack(BasicAckOptions::default()).await?;
                Ok::<Message, Error>(Self::get_rabbitmq_message(delivery))
            };

            tokio::select! {
                msg = local => Ok(msg?),
                msg = fut => Ok(msg?),
            }
        }
    }

    pub async fn scatter() {}

    pub async fn gather() {}

    fn get_rabbitmq_message(delivery: Delivery) -> Message {
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
        Message {
            sender_id,
            data,
            chunk_id,
            last_chunk,
        }
    }
}
