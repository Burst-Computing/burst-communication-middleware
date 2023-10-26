use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    Mutex, RwLock,
};

use tokio::sync::broadcast::{Receiver, Sender};

use crate::{impl_chainable_setter, CollectiveType, Error, Message, Result};

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
pub trait SendReceiveFactory: Send + Sync {
    async fn create_remote_proxies(&self) -> Result<HashMap<u32, Box<dyn SendReceiveProxy>>>;
}

#[derive(Clone, Debug)]
pub struct BurstOptions {
    pub burst_id: String,
    pub burst_size: u32,
    pub group_ranges: HashMap<String, HashSet<u32>>,
    pub group_id: String,
    pub broadcast_channel_size: usize,
}

pub struct BurstInfo<'a> {
    pub burst_id: &'a str,
    pub burst_size: u32,
    pub group_ranges: &'a HashMap<String, HashSet<u32>>,
    pub worker_id: u32,
    pub group_id: &'a str,
}

impl BurstOptions {
    pub fn new(
        burst_id: String,
        burst_size: u32,
        group_ranges: HashMap<String, HashSet<u32>>,
        group_id: String,
    ) -> Self {
        Self {
            burst_id,
            burst_size,
            group_ranges,
            group_id,
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
    options: BurstOptions,

    worker_id: u32,
    group: HashSet<u32>,

    local_channel_tx: Arc<HashMap<u32, UnboundedSender<Message>>>,
    local_channel_rx: Mutex<UnboundedReceiver<Message>>,

    broadcast_channel_tx: Sender<Message>,
    broadcast_channel_rx: Mutex<Receiver<Message>>,

    remote_send_receive: Box<dyn SendReceiveProxy>,

    counters: HashMap<CollectiveType, AtomicU32>,

    messages_buff: RwLock<HashMap<CollectiveType, RwLock<HashMap<u32, RwLock<Vec<Message>>>>>>,
}

impl BurstMiddleware {
    pub async fn create_proxies(
        options: BurstOptions,
        implementation: impl SendReceiveFactory,
    ) -> Result<HashMap<u32, BurstMiddleware>> {
        let current_group = options.group_ranges.get(&options.group_id).unwrap();

        // create local channels
        let mut local_channel_tx = HashMap::new();
        let mut local_channel_rx = HashMap::new();

        for id in current_group {
            let (tx, rx) = mpsc::unbounded_channel::<Message>();
            local_channel_tx.insert(*id, tx);
            local_channel_rx.insert(*id, rx);
        }

        // create broadcast channel for this group
        let (tx, _) = tokio::sync::broadcast::channel::<Message>(options.broadcast_channel_size);
        let mut broadcast_channel_rx = HashMap::new();

        // subscribe to all broadcast channels for each thread
        current_group.iter().for_each(|id| {
            broadcast_channel_rx.insert(*id, tx.subscribe());
        });

        let mut proxies = HashMap::new();
        let remote_proxies = implementation.create_remote_proxies().await?;

        let local_channel_tx = Arc::new(local_channel_tx);

        for (id, proxy) in remote_proxies {
            let proxy = BurstMiddleware::new(
                options.clone(),
                proxy,
                local_channel_tx.clone(),
                local_channel_rx.remove(&id).unwrap(),
                tx.clone(),
                broadcast_channel_rx.remove(&id).unwrap(),
                id,
                current_group.clone(),
            );
            proxies.insert(id, proxy);
        }

        Ok(proxies)
    }

    pub fn new(
        options: BurstOptions,
        remote_proxy: Box<dyn SendReceiveProxy>,
        local_channel_tx: Arc<HashMap<u32, UnboundedSender<Message>>>,
        local_channel_rx: UnboundedReceiver<Message>,
        broadcast_channel_tx: Sender<Message>,
        broadcast_channel_rx: Receiver<Message>,
        worker_id: u32,
        group: HashSet<u32>,
    ) -> BurstMiddleware {
        // create counters
        let mut counters = HashMap::new();
        for collective in &[
            CollectiveType::Broadcast,
            CollectiveType::Gather,
            CollectiveType::Scatter,
        ] {
            counters.insert(*collective, AtomicU32::new(0));
        }

        Self {
            options,
            worker_id,
            group,
            local_channel_tx,
            local_channel_rx: Mutex::new(local_channel_rx),
            broadcast_channel_tx,
            broadcast_channel_rx: Mutex::new(broadcast_channel_rx),
            remote_send_receive: remote_proxy,
            counters,
            messages_buff: RwLock::new(HashMap::new()),
        }
    }

    pub async fn send(&self, dest: u32, data: Bytes) -> Result<()> {
        self.send_collective(dest, data, CollectiveType::None, None)
            .await
    }

    pub async fn recv(&self) -> Result<Message> {
        // If there is a message in the buffer, return it
        if let Some(msg) = self.get_message_collective(&CollectiveType::None).await {
            return Ok(msg);
        }

        self.receive_message(false, |msg| msg.collective == CollectiveType::None)
            .await
    }

    pub async fn broadcast(&self, data: Option<Bytes>) -> Result<Message> {
        let counter = self.get_counter(&CollectiveType::Broadcast).await;

        let m;

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
            let msgs = self
                .receive_multiple_messages(1, 0, true, |msg| {
                    msg.collective == CollectiveType::Broadcast && msg.counter.unwrap() == counter
                })
                .await?;
            m = msgs.into_iter().next().unwrap();
        // Otherwise, wait for broadcast message
        } else {
            // If there is a message in the buffer, return it
            if let Some(msg) = self
                .get_message_collective(&CollectiveType::Broadcast)
                .await
            {
                return Ok(msg);
            }

            // Else, wait for a broadcast message
            m = self
                .receive_message(true, |msg| {
                    msg.collective == CollectiveType::Broadcast && msg.counter.unwrap() == counter
                })
                .await?;
        }

        // Increment broadcast counter
        self.increment_counter(&CollectiveType::Broadcast).await;

        Ok(m)
    }

    pub async fn gather(&self, data: Bytes) -> Result<Option<Vec<Message>>> {
        let counter = self.get_counter(&CollectiveType::Gather).await;

        let mut r = None;

        if self.worker_id == ROOT_ID {
            let mut gathered = Vec::new();
            gathered.push(Message {
                sender_id: self.worker_id,
                chunk_id: 0,
                last_chunk: true,
                counter: Some(counter),
                collective: CollectiveType::Gather,
                data,
            });

            // If there are messages in the buffer get them all
            let messages = self
                .get_all_messages_collective(&CollectiveType::Gather)
                .await;

            let local_remaining = self.options.group_ranges.len()
                - messages
                    .iter()
                    .filter(|x| self.group.contains(&x.sender_id))
                    .count()
                - 1;

            let remote_remaining = self.options.burst_size as usize
                - messages
                    .iter()
                    .filter(|x| !self.group.contains(&x.sender_id))
                    .count()
                - self.options.group_ranges.len();

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
        self.increment_counter(&CollectiveType::Gather).await;

        Ok(r)
    }

    pub fn info(&self) -> BurstInfo {
        BurstInfo {
            burst_id: &self.options.burst_id,
            burst_size: self.options.burst_size,
            group_ranges: &self.options.group_ranges,
            worker_id: self.worker_id,
            group_id: &self.options.group_id,
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

        if dest >= self.options.burst_size {
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

        if self.group.contains(&dest) {
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

    async fn get_message_collective(&self, collective: &CollectiveType) -> Option<Message> {
        if let Some(msg) = self.messages_buff.read().await.get(collective) {
            if let Some(msgs) = msg.read().await.get(&self.get_counter(collective).await) {
                if let Some(msg) = msgs.write().await.pop() {
                    return Some(msg);
                }
            }
        }
        None
    }

    async fn get_all_messages_collective(&self, collective: &CollectiveType) -> Vec<Message> {
        let mut r = Vec::new();
        if let Some(msg) = self.messages_buff.read().await.get(collective) {
            if let Some(msgs) = msg.read().await.get(&self.get_counter(collective).await) {
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
        let messages_buff = self.messages_buff.read().await;

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
                let mut messages_buff = self.messages_buff.write().await;

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

    async fn get_counter(&self, collective: &CollectiveType) -> u32 {
        self.counters
            .get(collective)
            .unwrap()
            .load(Ordering::Relaxed)
    }

    async fn increment_counter(&self, collective: &CollectiveType) {
        self.counters
            .get(collective)
            .unwrap()
            .fetch_add(1, Ordering::Relaxed);
    }
}
