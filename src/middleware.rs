use std::{collections::HashMap, ops::Range, sync::Arc};

use bytes::Bytes;
use tokio::sync::{
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    Mutex,
};

use tokio::sync::broadcast::{Receiver, Sender};

use futures::{FutureExt, StreamExt};
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
    types::{AMQPValue, FieldTable},
    BasicProperties, Channel, Connection, ConnectionProperties, Consumer, ExchangeKind,
};

use crate::impl_chainable_setter;
use crate::types::Message;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

const DEFAULT_BROADCAST_CHANNEL_SIZE: usize = 1024 * 1024;

#[derive(Debug, Clone)]
pub struct MiddlewareArguments {
    rabbitmq_uri: String,
    direct_exchage: String,
    broadcast_exchage_prefix: String,
    queue_prefix: String,
    global_range: Range<u32>,
    local_range: Range<u32>,
    broadcast_range: Range<u32>,
    broadcast_channel_size: usize,
}

impl MiddlewareArguments {
    pub fn new(
        rabbitmq_uri: String,
        global_range: Range<u32>,
        local_range: Range<u32>,
        broadcast_range: Range<u32>,
    ) -> Self {
        Self {
            rabbitmq_uri,
            direct_exchage: "exchange".to_string(),
            broadcast_exchage_prefix: "broadcast".to_string(),
            queue_prefix: "queue".to_string(),
            global_range,
            local_range,
            broadcast_range,
            broadcast_channel_size: DEFAULT_BROADCAST_CHANNEL_SIZE,
        }
    }

    impl_chainable_setter! {
        direct_exchage, String
    }

    impl_chainable_setter! {
        broadcast_exchage_prefix, String
    }

    impl_chainable_setter! {
        queue_prefix, String
    }

    impl_chainable_setter! {
        broadcast_channel_size, usize
    }

    pub fn build(&mut self) -> Self {
        self.clone()
    }
}

#[derive(Clone)]
pub struct Middleware {
    options: MiddlewareArguments,
    rabbitmq_conn: Arc<Connection>,
    rabbitmq_channel: Option<Channel>,

    local_queues: HashMap<u32, String>,

    local_channel_tx: HashMap<u32, UnboundedSender<Message>>,
    local_channel_rx: HashMap<u32, Arc<Mutex<UnboundedReceiver<Message>>>>,

    broadcast_channel_tx: Sender<Message>,
    broadcast_channel_rx: HashMap<u32, Arc<Mutex<Receiver<Message>>>>,

    consumer: Option<Consumer>,

    worker_id: Option<u32>,
    group_id: u32,
}

impl Middleware {
    pub async fn init_global(args: MiddlewareArguments, group_id: u32) -> Result<Self> {
        let rabbitmq_conn =
            Connection::connect(&args.rabbitmq_uri, ConnectionProperties::default()).await?;

        // TODO: set publisher_confirms to true
        let channel = rabbitmq_conn.create_channel().await?;

        // Declare direct exchange
        let mut options = ExchangeDeclareOptions::default();
        options.durable = true;

        channel
            .exchange_declare(
                &args.direct_exchage,
                ExchangeKind::Direct,
                options,
                FieldTable::default(),
            )
            .await?;

        // Declare broadcast exchanges for each group
        let mut options = ExchangeDeclareOptions::default();
        options.durable = true;

        futures::future::try_join_all(args.broadcast_range.clone().map(|id| {
            channel.exchange_declare(
                format!("{}_{}", args.broadcast_exchage_prefix, id).leak(),
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

        let queues = futures::future::try_join_all(args.global_range.clone().map(|id| {
            if args.local_range.contains(&id) {
                local_queues.insert(id, format!("{}_{}", args.queue_prefix, id));
            }
            channel.queue_declare(
                format!("{}_{}", args.queue_prefix, id).leak(),
                options,
                FieldTable::default(),
            )
        }))
        .await?;

        // Bind queues to exchange
        futures::future::try_join_all(queues.iter().map(|queue| {
            channel.queue_bind(
                queue.name().as_str(),
                &args.direct_exchage,
                queue.name().as_str(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
        }))
        .await?;

        // Bind local queues to this group's broadcast exchange
        let exchange_name = format!("{}_{}", args.broadcast_exchage_prefix, group_id);
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

        // create local channels
        let mut local_channel_tx = HashMap::new();
        let mut local_channel_rx = HashMap::new();

        for id in args.local_range.clone() {
            let (tx, rx) = mpsc::unbounded_channel::<Message>();
            local_channel_tx.insert(id, tx);
            local_channel_rx.insert(id, Arc::new(Mutex::new(rx)));
        }

        // create broadcast channel for this group
        let (tx, _) = tokio::sync::broadcast::channel::<Message>(args.broadcast_channel_size);
        let mut broadcast_channel_rx = HashMap::new();

        // subscribe to all broadcast channels for each thread
        args.local_range.clone().for_each(|id| {
            broadcast_channel_rx.insert(id, Arc::new(Mutex::new(tx.subscribe())));
        });

        Ok(Self {
            options: args,
            rabbitmq_conn: Arc::new(rabbitmq_conn),
            rabbitmq_channel: None,
            local_queues,
            local_channel_tx,
            local_channel_rx,
            broadcast_channel_tx: tx,
            broadcast_channel_rx,
            consumer: None,
            worker_id: None,
            group_id,
        })
    }

    pub async fn init_local(&mut self, id: u32) -> Result<()> {
        self.rabbitmq_channel = Some(self.rabbitmq_conn.create_channel().await?);
        self.worker_id = Some(id);
        let mut options = BasicConsumeOptions::default();
        options.no_ack = true;
        self.consumer = Some(
            self.rabbitmq_channel
                .as_ref()
                .unwrap()
                .basic_consume(
                    &self.local_queues.get(&self.worker_id.unwrap()).unwrap(),
                    &format!(
                        "{}_{} consumer",
                        self.options.queue_prefix,
                        self.worker_id.unwrap()
                    ),
                    options,
                    FieldTable::default(),
                )
                .await?,
        );
        Ok(())
    }

    pub async fn send(&self, dest: u32, data: Bytes) -> Result<()> {
        let chunk_id = 0;
        let last_chunk = true;

        if self.rabbitmq_channel.is_none() | self.worker_id.is_none() {
            return Err("init_local() must be called before send()".into());
        }

        if !self.options.global_range.contains(&dest) {
            return Err("worker with id {} does not exist".into());
        }
        if self.options.local_range.contains(&dest) {
            if let Some(tx) = self.local_channel_tx.get(&dest) {
                tx.send(Message {
                    sender_id: self.worker_id.unwrap(),
                    data,
                    chunk_id,
                    last_chunk,
                })?;
            } else {
                return Err("worker with id {} has no channel registered".into());
            }
        } else {
            let mut fields = FieldTable::default();
            fields.insert(
                "sender_id".into(),
                AMQPValue::LongUInt(self.worker_id.unwrap()),
            );
            fields.insert("chunk_id".into(), AMQPValue::LongUInt(chunk_id));
            fields.insert("last_chunk".into(), AMQPValue::Boolean(last_chunk));

            self.rabbitmq_channel
                .as_ref()
                .unwrap()
                .basic_publish(
                    &self.options.direct_exchage,
                    &format!("{}_{}", self.options.queue_prefix, dest),
                    BasicPublishOptions::default(),
                    &data,
                    BasicProperties::default().with_headers(fields),
                )
                .await?;
        }
        Ok(())
    }

    pub async fn broadcast(&self, data: Option<Bytes>) -> Result<Option<Message>> {
        if self.rabbitmq_channel.is_none() | self.worker_id.is_none() {
            return Err("init_local() must be called before broadcast()".into());
        }

        // If there is some data, broadcast it
        if let Some(data) = data {
            let chunk_id = 0;
            let last_chunk = true;

            self.broadcast_channel_tx.send(Message {
                sender_id: self.worker_id.unwrap(),
                data: data.clone(),
                chunk_id,
                last_chunk,
            })?;

            futures::future::try_join_all(
                self.options
                    .broadcast_range
                    .clone()
                    .filter(|dest| if *dest == self.group_id { false } else { true })
                    .map(|dest| {
                        let mut fields = FieldTable::default();
                        fields.insert(
                            "sender_id".into(),
                            AMQPValue::LongUInt(self.worker_id.unwrap()),
                        );
                        fields.insert("chunk_id".into(), AMQPValue::LongUInt(chunk_id));
                        fields.insert("last_chunk".into(), AMQPValue::Boolean(last_chunk));

                        self.rabbitmq_channel.as_ref().unwrap().basic_publish(
                            format!("{}_{}", self.options.broadcast_exchage_prefix, dest).leak(),
                            "",
                            BasicPublishOptions::default(),
                            &data,
                            BasicProperties::default().with_headers(fields),
                        )
                    }),
            )
            .await?;

            // Consume self broadcast message to avoid receiving it in the next receive
            self.broadcast_channel_rx
                .get(&self.worker_id.unwrap())
                .unwrap()
                .lock()
                .await
                .recv()
                .await?;
            Ok(None)
        // Otherwise, wait for broadcast message
        } else {
            // Receive from local channel
            let local = async {
                // receive blocking
                let msg = self
                    .broadcast_channel_rx
                    .get(&self.worker_id.unwrap())
                    .unwrap()
                    .lock()
                    .await
                    .recv()
                    .await?;
                Ok::<Message, Error>(msg)
            };

            // Receive from rabbitmq
            let fut = async {
                // receive blocking
                let delivery = self.consumer.clone().unwrap().next().await.unwrap()?;
                //delivery.ack(BasicAckOptions::default()).await?;
                Ok::<Message, Error>(Self::get_message(delivery))
            };

            tokio::select! {
                msg = local => Ok::<Option<Message>, Error>(Some(msg?)),
                msg = fut => Ok::<Option<Message>, Error>(Some(msg?)),
            }
        }
    }

    pub async fn try_rcv(&self) -> Result<Option<Message>> {
        if self.rabbitmq_channel.is_none() || self.worker_id.is_none() || self.consumer.is_none() {
            return Err("init_local() must be called before receive()".into());
        }

        // Receive from local channel
        if let Some(rx) = self.local_channel_rx.get(&self.worker_id.unwrap()) {
            // try receive without blocking
            if let Ok(msg) = rx.lock().await.try_recv() {
                return Ok(Some(msg));
            }
        }

        // Receive from rabbitmq
        if let Some(delivery) = self
            .consumer
            .clone()
            .unwrap()
            .next()
            // try receive without blocking
            .now_or_never()
            .flatten()
        {
            let delivery = delivery?;
            //delivery.ack(BasicAckOptions::default()).await?;
            return Ok(Some(Self::get_message(delivery)));
        }

        Ok(None)
    }

    pub async fn recv(&self) -> Result<Message> {
        if self.rabbitmq_channel.is_none()
            || self.worker_id.is_none()
            || self.consumer.is_none()
            || !self.local_channel_rx.contains_key(&self.worker_id.unwrap())
        {
            return Err("init_local() must be called before receive()".into());
        }

        let rx = self.local_channel_rx.get(&self.worker_id.unwrap()).unwrap();

        // Receive from local channel
        let local = async {
            // receive blocking
            let msg = rx.lock().await.recv().await.unwrap();
            Ok::<Message, Error>(msg)
        };

        // Receive from rabbitmq
        let fut = async {
            // receive blocking
            let delivery = self.consumer.clone().unwrap().next().await.unwrap()?;
            //delivery.ack(BasicAckOptions::default()).await?;
            Ok::<Message, Error>(Self::get_message(delivery))
        };

        tokio::select! {
            msg = local => Ok(msg?),
            msg = fut => Ok(msg?),
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
        Message {
            sender_id,
            data,
            chunk_id,
            last_chunk,
        }
    }
}
