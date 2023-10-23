use std::{collections::HashMap, ops::Range, sync::Arc};

use bytes::Bytes;
use tokio::sync::{
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    Mutex, RwLock,
};

use tokio::sync::broadcast::{Receiver, Sender};

use futures::StreamExt;
use lapin::{
    message::Delivery,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions,
        QueueBindOptions, QueueDeclareOptions,
    },
    types::{AMQPValue, FieldTable},
    BasicProperties, Channel, Connection, ConnectionProperties, Consumer, ExchangeKind,
};
use log::error;

use crate::types::Message;
use crate::{impl_chainable_setter, CollectiveType};

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

const DEFAULT_BROADCAST_CHANNEL_SIZE: usize = 1024 * 1024;

const ROOT_ID: u32 = 0;

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

    counters: HashMap<CollectiveType, u32>,

    messages_buff:
        Option<Arc<RwLock<HashMap<CollectiveType, RwLock<HashMap<u32, RwLock<Vec<Message>>>>>>>>,
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

        // Create counters

        let mut counters = HashMap::new();
        for collective in &[
            CollectiveType::Broadcast,
            CollectiveType::Gather,
            CollectiveType::Scatter,
        ] {
            counters.insert(*collective, 0);
        }

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
            counters,
            messages_buff: None,
        })
    }

    pub async fn init_local(&mut self, id: u32) -> Result<()> {
        self.rabbitmq_channel = Some(self.rabbitmq_conn.create_channel().await?);
        self.worker_id = Some(id);
        self.consumer = Some(
            self.rabbitmq_channel
                .as_ref()
                .unwrap()
                .basic_consume(
                    &self.local_queues.get(&self.worker_id.unwrap()).unwrap(),
                    &format!(
                        "{}_{} consumer",
                        self.options.queue_prefix,
                        self.worker_id.unwrap(),
                    ),
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await?,
        );
        self.messages_buff = Some(Arc::new(RwLock::new(HashMap::new())));
        Ok(())
    }

    pub async fn send(&self, dest: u32, data: Bytes) -> Result<()> {
        self.send_collective(dest, data, CollectiveType::None, None)
            .await
    }

    pub async fn recv(&mut self) -> Result<Message> {
        if self.rabbitmq_channel.is_none()
            || self.worker_id.is_none()
            || self.consumer.is_none()
            || !self.local_channel_rx.contains_key(&self.worker_id.unwrap())
        {
            return Err("init_local() must be called before receive()".into());
        }

        // If there is a message in the buffer, return it
        if let Some(msg) = self.get_message_collective(&CollectiveType::None).await {
            return Ok(msg);
        }

        self.receive_message(false, |msg| msg.collective == CollectiveType::None)
            .await
    }

    pub async fn broadcast(&mut self, data: Option<Bytes>) -> Result<Option<Message>> {
        if self.rabbitmq_channel.is_none() | self.worker_id.is_none() {
            return Err("init_local() must be called before broadcast()".into());
        }

        let counter = *self.counters.get(&CollectiveType::Broadcast).unwrap();

        let mut r = None;

        // If there is some data, broadcast it
        if let Some(data) = data {
            let chunk_id = 0;
            let last_chunk = true;

            self.broadcast_channel_tx.send(Message {
                sender_id: self.worker_id.unwrap(),
                chunk_id,
                last_chunk,
                counter: Some(counter),
                collective: CollectiveType::Broadcast,
                data: data.clone(),
            })?;

            match futures::future::try_join_all(
                self.options
                    .broadcast_range
                    .clone()
                    .filter(|dest| if *dest == self.group_id { false } else { true })
                    .map(|dest| {
                        self.send_rabbit(
                            chunk_id,
                            last_chunk,
                            Some(counter),
                            CollectiveType::Broadcast,
                            &data,
                            format!("{}_{}", self.options.broadcast_exchage_prefix, dest).leak(),
                            "",
                        )
                    }),
            )
            .await
            {
                Ok(_) => {}
                Err(e) => {
                    error!("Error: {}", e);
                }
            }

            // Consume self broadcast message to avoid receiving it in the next receive
            self.receive_multiple_messages(1, 0, true, |msg| {
                msg.collective == CollectiveType::Broadcast && msg.counter.unwrap() == counter
            })
            .await?;
        // Otherwise, wait for broadcast message
        } else {
            // If there is a message in the buffer, return it
            if let Some(msg) = self
                .get_message_collective(&CollectiveType::Broadcast)
                .await
            {
                return Ok(Some(msg));
            }

            // Else, wait for a broadcast message
            let msg = self
                .receive_message(true, |msg| {
                    msg.collective == CollectiveType::Broadcast
                        && msg.counter.unwrap()
                            == *self.counters.get(&CollectiveType::Broadcast).unwrap()
                })
                .await?;

            r = Some(msg);
        }

        // Increment broadcast counter
        self.counters
            .entry(CollectiveType::Broadcast)
            .and_modify(|c| *c += 1);

        Ok(r)
    }

    pub async fn gather(&mut self, data: Bytes) -> Result<Option<Vec<Message>>> {
        if self.rabbitmq_channel.is_none() | self.worker_id.is_none() {
            return Err("init_local() must be called before gather()".into());
        }

        let counter = *self.counters.get(&CollectiveType::Gather).unwrap();

        let mut r = None;

        if self.worker_id.unwrap() == ROOT_ID {
            let mut gathered = Vec::new();
            gathered.push(Message {
                sender_id: self.worker_id.unwrap(),
                chunk_id: 0,
                last_chunk: true,
                counter: Some(*self.counters.get(&CollectiveType::Gather).unwrap()),
                collective: CollectiveType::Gather,
                data,
            });

            // If there are messages in the buffer get them all
            let messages = self
                .get_all_messages_collective(&CollectiveType::Gather)
                .await;

            let local_remaining = self.options.local_range.len()
                - messages
                    .iter()
                    .filter(|x| self.options.local_range.contains(&x.sender_id))
                    .count()
                - 1;

            let remote_remaining = self.options.global_range.len()
                - messages
                    .iter()
                    .filter(|x| {
                        self.options.global_range.contains(&x.sender_id)
                            && !self.options.local_range.contains(&x.sender_id)
                    })
                    .count()
                - self.options.local_range.len();

            let msgs = self
                .receive_multiple_messages(local_remaining, remote_remaining, false, |msg| {
                    msg.collective == CollectiveType::Gather && msg.counter.unwrap() == counter
                })
                .await?;

            gathered.extend(messages);
            gathered.extend(msgs);

            // Sort by sender_id
            gathered.sort_by_key(|msg| msg.sender_id);

            r = Some(gathered);
        } else {
            self.send_collective(ROOT_ID, data, CollectiveType::Gather, Some(counter))
                .await?;
        }

        // Increment gather counter
        self.counters
            .entry(CollectiveType::Gather)
            .and_modify(|c| *c += 1);

        Ok(r)
    }

    async fn send_collective(
        &self,
        dest: u32,
        data: Bytes,
        collective: CollectiveType,
        counter: Option<u32>,
    ) -> Result<()> {
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
                    chunk_id,
                    last_chunk,
                    counter,
                    collective,
                    data,
                })?;
            } else {
                return Err("worker with id {} has no channel registered".into());
            }
        } else {
            self.send_rabbit(
                chunk_id,
                last_chunk,
                counter,
                collective,
                &data,
                &self.options.direct_exchage,
                &format!("{}_{}", self.options.queue_prefix, dest),
            )
            .await?;
        }
        Ok(())
    }

    async fn send_rabbit(
        &self,
        chunk_id: u32,
        last_chunk: bool,
        counter: Option<u32>,
        collective: CollectiveType,
        data: &[u8],
        exchange: &str,
        routing_key: &str,
    ) -> Result<()> {
        let mut fields = FieldTable::default();
        fields.insert(
            "sender_id".into(),
            AMQPValue::LongUInt(self.worker_id.unwrap()),
        );
        fields.insert("chunk_id".into(), AMQPValue::LongUInt(chunk_id));
        fields.insert("last_chunk".into(), AMQPValue::Boolean(last_chunk));
        if let Some(counter) = counter {
            fields.insert("counter".into(), AMQPValue::LongUInt(counter));
        }
        fields.insert("collective".into(), AMQPValue::LongUInt(collective as u32));
        self.rabbitmq_channel
            .as_ref()
            .unwrap()
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

    async fn get_message_collective(&mut self, collective: &CollectiveType) -> Option<Message> {
        if let Some(msg) = self
            .messages_buff
            .as_ref()
            .unwrap()
            .read()
            .await
            .get(collective)
        {
            if let Some(msgs) = msg.read().await.get(self.counters.get(collective).unwrap()) {
                if let Some(msg) = msgs.write().await.pop() {
                    return Some(msg);
                }
            }
        }
        None
    }

    async fn get_all_messages_collective(&mut self, collective: &CollectiveType) -> Vec<Message> {
        let mut r = Vec::new();
        if let Some(msg) = self
            .messages_buff
            .as_ref()
            .unwrap()
            .read()
            .await
            .get(collective)
        {
            if let Some(msgs) = msg.read().await.get(self.counters.get(collective).unwrap()) {
                for msg in msgs.write().await.drain(..) {
                    r.push(msg);
                }
            }
        }
        r
    }

    async fn receive_message<P>(&self, broadcast: bool, filter: P) -> Result<Message>
    where
        P: Fn(&Message) -> bool,
    {
        // Receive from local channel
        let local = async {
            loop {
                // receive blocking
                let msg = match broadcast {
                    true => self.receive_broadcast().await?,
                    false => self.receive_local().await,
                };
                if filter(&msg) {
                    return Ok::<Message, Error>(msg);
                }
                self.save_message(msg).await;
            }
        };

        // Receive from rabbitmq
        let fut = async {
            loop {
                // receive blocking
                let msg = self.receive_rabbit().await?;
                if filter(&msg) {
                    return Ok::<Message, Error>(msg);
                }
                self.save_message(msg).await;
            }
        };

        tokio::select! {
            msg = local => Ok::<Message, Error>(msg?),
            msg = fut => Ok::<Message, Error>(msg?),
        }
    }

    async fn receive_multiple_messages<P>(
        &self,
        local: usize,
        remote: usize,
        broadcast: bool,
        filter: P,
    ) -> Result<Vec<Message>>
    where
        P: Fn(&Message) -> bool,
    {
        let mut local_msgs = Vec::new();

        // Receive from local channel
        let local = async {
            while local_msgs.len() < local {
                // receive blocking
                let msg = match broadcast {
                    true => self.receive_broadcast().await?,
                    false => self.receive_local().await,
                };
                if filter(&msg) {
                    local_msgs.push(msg);
                } else {
                    self.save_message(msg).await;
                }
            }
            Ok::<Vec<Message>, Error>(local_msgs)
        };

        let mut remote_msgs = Vec::new();

        // Receive from rabbitmq
        let fut = async {
            while remote_msgs.len() < remote {
                // receive blocking
                let msg = self.receive_rabbit().await?;
                if filter(&msg) {
                    remote_msgs.push(msg);
                } else {
                    self.save_message(msg).await;
                }
            }
            Ok::<Vec<Message>, Error>(remote_msgs)
        };

        let (mut local, remote) = tokio::try_join!(local, fut)?;
        local.extend(remote);
        Ok(local)
    }

    async fn receive_rabbit(&self) -> Result<Message> {
        let delivery = self.consumer.clone().unwrap().next().await.unwrap()?;
        delivery.ack(BasicAckOptions::default()).await?;
        Ok(Self::get_message(delivery))
    }

    async fn receive_local(&self) -> Message {
        self.local_channel_rx
            .get(&self.worker_id.unwrap())
            .unwrap()
            .lock()
            .await
            .recv()
            .await
            .unwrap()
    }

    async fn receive_broadcast(&self) -> Result<Message> {
        Ok(self
            .broadcast_channel_rx
            .get(&self.worker_id.unwrap())
            .unwrap()
            .lock()
            .await
            .recv()
            .await?)
    }

    async fn save_message(&self, msg: Message) {
        let messages_buff = self.messages_buff.as_ref().unwrap().read().await;

        match messages_buff.get(&msg.collective) {
            Some(msgs_by_counter_lock) => {
                let msgs_by_counter = msgs_by_counter_lock.read().await;

                match msgs_by_counter.get(&msg.counter.unwrap()) {
                    Some(msgs) => {
                        msgs.write().await.push(msg);
                    }
                    None => {
                        // Release the lock early
                        drop(msgs_by_counter);
                        let mut msgs = msgs_by_counter_lock.write().await;
                        msgs.insert(msg.counter.unwrap(), RwLock::new(vec![msg]));
                    }
                }
            }
            None => {
                // Release the lock early
                drop(messages_buff);
                let mut messages_buff = self.messages_buff.as_ref().unwrap().write().await;

                messages_buff.insert(msg.collective, RwLock::new(HashMap::new()));
                messages_buff
                    .get(&msg.collective)
                    .unwrap()
                    .write()
                    .await
                    .insert(msg.counter.unwrap(), RwLock::new(vec![msg]));
            }
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
}
