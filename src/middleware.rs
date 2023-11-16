use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::Future;
use log::debug;
use tokio::sync::RwLock;

use crate::{CollectiveType, Error, Message, Result};

const ROOT_ID: u32 = 0;

#[async_trait]
pub trait Proxy: SendReceiveProxy + BroadcastSendProxy + Send + Sync {}

#[async_trait]
pub trait SendReceiveProxy: SendProxy + ReceiveProxy + Send + Sync {}

#[async_trait]
pub trait SendProxy: Send + Sync {
    async fn send(&self, dest: u32, msg: &Message) -> Result<()>;
}

#[async_trait]
pub trait ReceiveProxy: Send + Sync {
    async fn recv(&self) -> Result<Message>;
}

#[async_trait]
pub trait BroadcastSendProxy: Send + Sync {
    async fn broadcast_send(&self, msg: &Message) -> Result<()>;
}

#[async_trait]
pub trait SendReceiveFactory<T>: Send + Sync {
    async fn create_proxies(
        burst_options: Arc<BurstOptions>,
        options: T,
        broadcast_proxy: Box<dyn BroadcastSendProxy>,
    ) -> Result<HashMap<u32, Box<dyn Proxy>>>;
}

#[async_trait]
pub trait SendReceiveLocalFactory<T>: Send + Sync {
    async fn create_proxies(
        burst_options: Arc<BurstOptions>,
        options: T,
    ) -> Result<(HashMap<u32, Box<dyn Proxy>>, Box<dyn BroadcastSendProxy>)>;
}

#[derive(Clone, Debug)]
pub struct BurstOptions {
    pub burst_id: String,
    pub burst_size: u32,
    pub group_ranges: HashMap<String, HashSet<u32>>,
    pub group_id: String,
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
        }
    }
}

#[derive(Clone)]
pub struct BurstMiddleware {
    options: Arc<BurstOptions>,

    worker_id: u32,
    group: HashSet<u32>,

    local_send_receive: Arc<dyn Proxy>,
    remote_send_receive: Arc<dyn Proxy>,

    counters: Arc<HashMap<CollectiveType, AtomicU32>>,

    messages_buff: Arc<RwLock<HashMap<CollectiveType, RwLock<HashMap<u32, RwLock<Vec<Message>>>>>>>,
}

impl BurstMiddleware {
    pub async fn create_proxies<LocalImpl, RemoteImpl, LocalOptions, RemoteOptions>(
        options: BurstOptions,
        local_impl_options: LocalOptions,
        remote_impl_options: RemoteOptions,
    ) -> Result<HashMap<u32, Self>>
    where
        LocalImpl: SendReceiveLocalFactory<LocalOptions>,
        RemoteImpl: SendReceiveFactory<RemoteOptions>,
        LocalOptions: Send + Sync,
        RemoteOptions: Send + Sync,
    {
        let options = Arc::new(options);
        let current_group = options.group_ranges.get(&options.group_id).unwrap();

        let mut proxies = HashMap::new();
        let (local_proxies, broadcast_proxy) =
            LocalImpl::create_proxies(options.clone(), local_impl_options).await?;
        let mut remote_proxies =
            RemoteImpl::create_proxies(options.clone(), remote_impl_options, broadcast_proxy)
                .await?;

        for (id, local_proxy) in local_proxies {
            let proxy = BurstMiddleware::new(
                options.clone(),
                local_proxy,
                remote_proxies.remove(&id).unwrap(),
                id,
                current_group.clone(),
            );
            proxies.insert(id, proxy);
        }

        // Create a thread for this group to receive broadcast messages

        Ok(proxies)
    }

    pub fn new(
        options: Arc<BurstOptions>,
        local_proxy: Box<dyn Proxy>,
        remote_proxy: Box<dyn Proxy>,
        worker_id: u32,
        group: HashSet<u32>,
    ) -> Self {
        // create counters
        let mut counters = HashMap::new();
        for collective in &[
            CollectiveType::Broadcast,
            CollectiveType::Gather,
            CollectiveType::Scatter,
            CollectiveType::AllToAll,
        ] {
            counters.insert(*collective, AtomicU32::new(0));
        }

        Self {
            options,
            worker_id,
            group,
            remote_send_receive: Arc::from(remote_proxy),
            local_send_receive: Arc::from(local_proxy),
            counters: Arc::new(counters),
            messages_buff: Arc::new(RwLock::new(HashMap::new())),
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

        self.receive_message(|msg| msg.collective == CollectiveType::None)
            .await
    }

    pub async fn broadcast(&self, data: Option<Bytes>) -> Result<Message> {
        let counter = self.get_counter(&CollectiveType::Broadcast).await;

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

            match tokio::join!(
                self.local_send_receive.broadcast_send(&msg),
                self.remote_send_receive.broadcast_send(&msg)
            ) {
                (Ok(()), Ok(())) => {}
                (Err(e), _) | (_, Err(e)) => return Err(e),
            }
        } else {
            // If there is a message in the buffer, return it
            if let Some(msg) = self
                .get_message_collective(&CollectiveType::Broadcast)
                .await
            {
                // Increment broadcast counter
                self.increment_counter(&CollectiveType::Broadcast).await;
                return Ok(msg);
            }
        }

        // Broadcast messages will only be received via the local channel
        // Remote broadcast messages will be sent to the local channel
        // by a separate thread
        let m = self
            .receive_message_local(|msg| {
                msg.collective == CollectiveType::Broadcast && msg.counter.unwrap() == counter
            })
            .await?;

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

            let local_remaining = self.group.len()
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
                - self.group.len();

            let msgs = self
                .receive_multiple_messages(local_remaining, remote_remaining, |msg| {
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

    pub async fn scatter(&self, data: Option<Vec<Bytes>>) -> Result<Option<Message>> {
        let counter = self.get_counter(&CollectiveType::Scatter).await;
        let mut r = None;

        if self.worker_id == ROOT_ID {
            if data.is_none() {
                return Err("Root worker must send data".into());
            }
            let data = data.unwrap();
            if data.len() != (self.options.burst_size - 1) as usize {
                return Err("Data size must be equal to burst size - 1".into());
            }

            futures::future::try_join_all(data.into_iter().enumerate().map(|(i, data)| {
                self.send_collective(i as u32 + 1, data, CollectiveType::Scatter, Some(counter))
            }))
            .await?;
        } else {
            // If there is a message in the buffer, return it
            r = if let Some(msg) = self.get_message_collective(&CollectiveType::Scatter).await {
                Some(msg)
            } else {
                Some(
                    self.receive_message(|msg| {
                        msg.collective == CollectiveType::Scatter && msg.counter.unwrap() == counter
                    })
                    .await?,
                )
            };
        }
        // Increment scatter counter
        self.increment_counter(&CollectiveType::Scatter).await;
        Ok(r)
    }

    pub async fn all_to_all(&self, data: Vec<Bytes>) -> Result<Vec<Message>> {
        let counter = self.get_counter(&CollectiveType::AllToAll).await;

        if data.len() != self.options.burst_size as usize {
            return Err("Data size must be equal to burst size".into());
        }

        debug!("Worker {} sending all_to_all", self.worker_id);
        // first send to all workers
        futures::future::try_join_all(data.into_iter().enumerate().map(|(i, data)| {
            self.send_collective(i as u32, data, CollectiveType::AllToAll, Some(counter))
        }))
        .await?;

        debug!("Worker {} receiving all_to_all", self.worker_id);
        //let message_count = Arc::new(AtomicU32::new(0));
        // then receive from all workers
        let mut r = futures::future::try_join_all((0..self.options.burst_size).map(|_| {
            //let message_count = message_count.clone();
            async move {
                let r = self
                    .receive_message(|msg| {
                        msg.collective == CollectiveType::AllToAll
                            && msg.counter.unwrap() == counter
                    })
                    .await?;
                //message_count.fetch_add(1, Ordering::Relaxed);
                //let message_count = message_count.load(Ordering::Relaxed);
                // debug!(
                //     "Worker {} received {} all_to_all messages, remaining {}",
                //     self.worker_id,
                //     message_count,
                //     self.options.burst_size - message_count
                // );
                Ok::<Message, Error>(r)
            }
        }))
        .await?;

        debug!("Worker {} sorting all_to_all", self.worker_id);
        // Sort by sender_id
        r.sort_by_key(|msg| msg.sender_id);

        // Increment all_to_all counter
        self.increment_counter(&CollectiveType::AllToAll).await;
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
            self.local_send_receive.send(dest, &msg).await?;
        } else {
            self.remote_send_receive.send(dest, &msg).await?;
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

    async fn receive_message<P>(&self, filter: P) -> Result<Message>
    where
        P: Fn(&Message) -> bool,
    {
        // Receive from local channel
        let local = self.receive_message_local(&filter);

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

    async fn receive_message_local<P>(&self, filter: P) -> Result<Message>
    where
        P: Fn(&Message) -> bool,
    {
        // Receive from local channel
        loop {
            // receive blocking
            let msg = self.receive_local().await?;
            if filter(&msg) {
                return Ok(msg);
            }
            self.save_message(msg).await;
        }
    }

    async fn receive_multiple_messages<P>(
        &self,
        local: usize,
        remote: usize,
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
                let msg = self.receive_local().await?;
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

    async fn receive_local(&self) -> Result<Message> {
        self.local_send_receive.recv().await
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
