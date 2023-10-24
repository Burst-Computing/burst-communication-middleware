use std::{collections::HashMap, ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use log::info;
use tokio::sync::{
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    Mutex, RwLock,
};

use tokio::sync::broadcast::{Receiver, Sender};

use crate::types::{Error, Message, Result};
use crate::{impl_chainable_setter, CollectiveType};

const DEFAULT_BROADCAST_CHANNEL_SIZE: usize = 1024 * 1024;

const ROOT_ID: u32 = 0;

#[async_trait]
pub trait SendReceiveProxy: SendProxy + ReceiveProxy + Send + Sync {
    async fn broadcast(&self, msg: &Message) -> Result<()>;
}

#[async_trait]
pub trait SendProxy: Send + Sync {
    async fn send(&self, dest: u32, msg: &Message) -> Result<()>;
}

#[async_trait]
pub trait ReceiveProxy: Send + Sync {
    async fn recv(&self) -> Result<Message>;
}

#[async_trait]
pub trait MiddlewareProxy: Send + Sync {
    async fn send(&self, dest: u32, data: Bytes) -> Result<()>;
    async fn recv(&mut self) -> Result<Message>;
    async fn broadcast(&mut self, data: Option<Bytes>) -> Result<Option<Message>>;
    async fn gather(&mut self, data: Bytes) -> Result<Option<Vec<Message>>>;
}

#[async_trait]
pub trait SendReceiveFactory: Send + Sync {
    async fn create_remote_proxies(&self) -> Result<HashMap<u32, Box<dyn SendReceiveProxy>>>;
}

#[async_trait]
pub trait Middleware {
    async fn create_proxies(
        &mut self,
        implementation: impl SendReceiveFactory,
    ) -> Result<HashMap<u32, Box<dyn MiddlewareProxy>>>;
}

#[derive(Clone, Debug)]
pub struct MiddlewareOptions {
    pub global_range: Range<u32>,
    pub local_range: Range<u32>,
    pub broadcast_range: Range<u32>,
    pub broadcast_channel_size: usize,
}

impl MiddlewareOptions {
    pub fn new(
        global_range: Range<u32>,
        local_range: Range<u32>,
        broadcast_range: Range<u32>,
    ) -> Self {
        Self {
            global_range,
            local_range,
            broadcast_range,
            broadcast_channel_size: DEFAULT_BROADCAST_CHANNEL_SIZE,
        }
    }

    impl_chainable_setter! {
        broadcast_channel_size, usize
    }

    pub fn build(&self) -> Self {
        self.clone()
    }
}

pub struct BurstMiddleware {
    options: MiddlewareOptions,

    local_channel_tx: HashMap<u32, UnboundedSender<Message>>,
    local_channel_rx: HashMap<u32, UnboundedReceiver<Message>>,

    broadcast_channel_tx: Sender<Message>,
    broadcast_channel_rx: HashMap<u32, Receiver<Message>>,
}

impl BurstMiddleware {
    pub fn new(options: MiddlewareOptions) -> BurstMiddleware {
        // create local channels
        let mut local_channel_tx = HashMap::new();
        let mut local_channel_rx = HashMap::new();

        for id in options.local_range.clone() {
            let (tx, rx) = mpsc::unbounded_channel::<Message>();
            local_channel_tx.insert(id, tx);
            local_channel_rx.insert(id, rx);
        }

        // create broadcast channel for this group
        let (tx, _) = tokio::sync::broadcast::channel::<Message>(options.broadcast_channel_size);
        let mut broadcast_channel_rx = HashMap::new();

        // subscribe to all broadcast channels for each thread
        options.local_range.clone().for_each(|id| {
            broadcast_channel_rx.insert(id, tx.subscribe());
        });

        Self {
            options,
            local_channel_tx,
            local_channel_rx,
            broadcast_channel_tx: tx,
            broadcast_channel_rx,
        }
    }
}

#[async_trait]
impl Middleware for BurstMiddleware {
    async fn create_proxies(
        &mut self,
        implementation: impl SendReceiveFactory,
    ) -> Result<HashMap<u32, Box<dyn MiddlewareProxy>>> {
        let mut proxies = HashMap::new();

        let remote_proxies = implementation.create_remote_proxies().await?;
        let local_channel_tx = Arc::new(self.local_channel_tx.clone());

        for (id, proxy) in remote_proxies {
            let proxy = Box::new(BurstMiddlewareProxy::new(
                self.options.clone(),
                id,
                local_channel_tx.clone(),
                self.local_channel_rx.remove(&id).unwrap(),
                self.broadcast_channel_tx.clone(),
                self.broadcast_channel_rx.remove(&id).unwrap(),
                proxy,
            ));
            proxies.insert(id, proxy as Box<dyn MiddlewareProxy>);
        }

        Ok(proxies)
    }
}

struct BurstMiddlewareProxy {
    options: MiddlewareOptions,

    worker_id: u32,

    local_channel_tx: Arc<HashMap<u32, UnboundedSender<Message>>>,
    local_channel_rx: Mutex<UnboundedReceiver<Message>>,

    broadcast_channel_tx: Sender<Message>,
    broadcast_channel_rx: Mutex<Receiver<Message>>,

    remote_send_receive: Box<dyn SendReceiveProxy>,

    counters: HashMap<CollectiveType, u32>,

    messages_buff: Arc<RwLock<HashMap<CollectiveType, RwLock<HashMap<u32, RwLock<Vec<Message>>>>>>>,
}

#[async_trait]
impl MiddlewareProxy for BurstMiddlewareProxy {
    async fn send(&self, dest: u32, data: Bytes) -> Result<()> {
        self.send_collective(dest, data, CollectiveType::None, None)
            .await
    }
    async fn recv(&mut self) -> Result<Message> {
        // If there is a message in the buffer, return it
        if let Some(msg) = self.get_message_collective(&CollectiveType::None).await {
            return Ok(msg);
        }

        self.receive_message(false, |msg| msg.collective == CollectiveType::None)
            .await
    }

    async fn broadcast(&mut self, data: Option<Bytes>) -> Result<Option<Message>> {
        let counter = *self.counters.get(&CollectiveType::Broadcast).unwrap();

        let mut r = None;

        // If there is some data, broadcast it
        if let Some(data) = data {
            let chunk_id = 0;
            let last_chunk = true;

            let msg = Message {
                sender_id: self.worker_id,
                chunk_id,
                last_chunk,
                counter: Some(counter),
                collective: CollectiveType::Broadcast,
                data: data.clone(),
            };

            self.broadcast_channel_tx.send(msg.clone())?;

            self.remote_send_receive.as_ref().broadcast(&msg).await?;

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

            let counter = *self.counters.get(&CollectiveType::Broadcast).unwrap();

            // Else, wait for a broadcast message
            let msg = self
                .receive_message(true, |msg| {
                    msg.collective == CollectiveType::Broadcast && msg.counter.unwrap() == counter
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

    async fn gather(&mut self, data: Bytes) -> Result<Option<Vec<Message>>> {
        let counter = *self.counters.get(&CollectiveType::Gather).unwrap();

        let mut r = None;

        if self.worker_id == ROOT_ID {
            let mut gathered = Vec::new();
            gathered.push(Message {
                sender_id: self.worker_id,
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

            info!(
                "local_remaining: {}, remote_remaining: {}, got from buffer: {:?}",
                local_remaining, remote_remaining, messages
            );

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
}

impl BurstMiddlewareProxy {
    pub fn new(
        options: MiddlewareOptions,
        worker_id: u32,
        local_channel_tx: Arc<HashMap<u32, UnboundedSender<Message>>>,
        local_channel_rx: UnboundedReceiver<Message>,
        broadcast_channel_tx: Sender<Message>,
        broadcast_channel_rx: Receiver<Message>,
        remote_send_receive: Box<dyn SendReceiveProxy>,
    ) -> Self {
        // Create counters
        let mut counters = HashMap::new();
        for collective in &[
            CollectiveType::Broadcast,
            CollectiveType::Gather,
            CollectiveType::Scatter,
        ] {
            counters.insert(*collective, 0);
        }

        Self {
            options,
            worker_id,
            local_channel_tx,
            local_channel_rx: Mutex::new(local_channel_rx),
            broadcast_channel_tx,
            broadcast_channel_rx: Mutex::new(broadcast_channel_rx),
            remote_send_receive,
            counters,
            messages_buff: Arc::new(RwLock::new(HashMap::new())),
        }
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

        if !self.options.global_range.contains(&dest) {
            return Err("worker with id {} does not exist".into());
        }

        let msg = Message {
            sender_id: self.worker_id,
            chunk_id,
            last_chunk,
            counter,
            collective,
            data,
        };

        if self.options.local_range.contains(&dest) {
            if let Some(tx) = self.local_channel_tx.get(&dest) {
                tx.send(msg)?;
            } else {
                return Err("worker with id {} has no channel registered".into());
            }
        } else {
            self.remote_send_receive.as_ref().send(dest, &msg).await?;
        }
        Ok(())
    }

    async fn get_message_collective(&mut self, collective: &CollectiveType) -> Option<Message> {
        if let Some(msg) = self.messages_buff.as_ref().read().await.get(collective) {
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
        if let Some(msg) = self.messages_buff.as_ref().read().await.get(collective) {
            if let Some(msgs) = msg.read().await.get(self.counters.get(collective).unwrap()) {
                for msg in msgs.write().await.drain(..) {
                    r.push(msg);
                }
            }
        }
        r
    }

    async fn receive_message<P>(&mut self, broadcast: bool, filter: P) -> Result<Message>
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
                let msg = self.receive_remote().await?;
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
                let msg = self.receive_remote().await?;
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

    async fn receive_remote(&self) -> Result<Message> {
        self.remote_send_receive.recv().await
    }

    async fn receive_local(&self) -> Message {
        self.local_channel_rx.lock().await.recv().await.unwrap()
    }

    async fn receive_broadcast(&self) -> Result<Message> {
        Ok(self.broadcast_channel_rx.lock().await.recv().await?)
    }

    async fn save_message(&self, msg: Message) {
        let messages_buff = self.messages_buff.as_ref().read().await;

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
                let mut messages_buff = self.messages_buff.as_ref().write().await;

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
}
