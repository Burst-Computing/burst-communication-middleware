use crate::{
    chunk_store::chunk_message, counter::AtomicCounter, message_store::MessageStoreChunked,
    CollectiveType, Error, Message, Result,
};
use async_trait::async_trait;
use bytes::Bytes;
use std::{
    collections::{HashMap, HashSet},
    sync::{atomic::AtomicU32, Arc},
};

const ROOT_ID: u32 = 0;

pub trait SendReceiveProxy: SendProxy + ReceiveProxy + Send + Sync {}

#[async_trait]
pub trait SendProxy: Send + Sync {
    async fn send(&self, dest: u32, msg: &Message) -> Result<()>;
}

#[async_trait]
pub trait ReceiveProxy: Send + Sync {
    async fn recv(&self) -> Result<Message>;
}

pub trait BroadcastProxy: BroadcastSendProxy + BroadcastReceiveProxy + Send + Sync {}

#[async_trait]
pub trait BroadcastSendProxy: Send + Sync {
    async fn broadcast_send(&self, msg: &Message) -> Result<()>;
}

#[async_trait]
pub trait BroadcastReceiveProxy: Send + Sync {
    async fn broadcast_recv(&self) -> Result<Message>;
}

#[async_trait]
pub trait SendReceiveFactory<T>: Send + Sync {
    async fn create_proxies(
        burst_options: Arc<BurstOptions>,
        options: T,
        broadcast_proxy: Box<dyn BroadcastSendProxy>,
    ) -> Result<(
        HashMap<u32, Box<dyn SendReceiveProxy>>,
        Box<dyn BroadcastSendProxy>,
    )>;
}

#[async_trait]
pub trait SendReceiveLocalFactory<T>: Send + Sync {
    async fn create_proxies(
        burst_options: Arc<BurstOptions>,
        options: T,
    ) -> Result<(
        HashMap<u32, (Box<dyn SendReceiveProxy>, Box<dyn BroadcastProxy>)>,
        Box<dyn BroadcastSendProxy>,
    )>;
}

#[derive(Clone, Debug)]
pub struct BurstOptions {
    pub burst_id: String,
    pub burst_size: u32,
    pub group_ranges: HashMap<String, HashSet<u32>>,
    pub group_id: String,
    pub enable_message_chunking: bool,
    pub message_chunk_size: usize,
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
        enable_message_chunking: bool,
        message_chunk_size: usize,
    ) -> Self {
        Self {
            burst_id,
            burst_size,
            group_ranges,
            group_id,
            enable_message_chunking,
            message_chunk_size,
        }
    }
}

#[derive(Clone)]
pub struct BurstMiddleware {
    options: Arc<BurstOptions>,

    worker_id: u32,
    group: HashSet<u32>,

    local_send_receive: Arc<dyn SendReceiveProxy>,
    remote_send_receive: Arc<dyn SendReceiveProxy>,

    local_broadcast: Arc<dyn BroadcastProxy>,
    remote_broadcast_send: Arc<dyn BroadcastSendProxy>,

    counters: Arc<HashMap<CollectiveType, AtomicU32>>,

    message_store: Arc<MessageStoreChunked>,
    enable_message_chunking: bool,
    message_chunk_size: usize,
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

        let (mut local_proxies, broadcast_send) =
            LocalImpl::create_proxies(options.clone(), local_impl_options).await?;
        let (mut remote_proxies, broadcast_send) =
            RemoteImpl::create_proxies(options.clone(), remote_impl_options, broadcast_send)
                .await?;

        let broadcast_send: Arc<dyn BroadcastSendProxy> = broadcast_send.into();

        let mut proxies = HashMap::new();

        for id in current_group {
            let (lsp, lbp) = local_proxies.remove(id).unwrap();
            let rsp = remote_proxies.remove(id).unwrap();
            let proxy = BurstMiddleware::new(
                options.clone(),
                lsp.into(),
                rsp.into(),
                lbp.into(),
                broadcast_send.clone(),
                *id,
                current_group.clone(),
            );
            proxies.insert(*id, proxy);
        }

        Ok(proxies)
    }

    pub fn new(
        options: Arc<BurstOptions>,
        local_send_receive: Arc<dyn SendReceiveProxy>,
        remote_send_receive: Arc<dyn SendReceiveProxy>,
        local_broadcast: Arc<dyn BroadcastProxy>,
        remote_broadcast_send: Arc<dyn BroadcastSendProxy>,
        worker_id: u32,
        group: HashSet<u32>,
    ) -> Self {
        let enable_message_chunking = options.enable_message_chunking;
        let message_chunk_size = options.message_chunk_size;

        // create counters
        let counters = AtomicCounter::new(
            [
                CollectiveType::Direct,
                CollectiveType::Broadcast,
                CollectiveType::Gather,
                CollectiveType::Scatter,
                CollectiveType::AllToAll,
            ]
            .into_iter(),
        );

        let message_store = Arc::new(MessageStoreChunked::new(
            (0..options.burst_size).into_iter(),
            &[
                CollectiveType::Direct,
                CollectiveType::Broadcast,
                CollectiveType::Scatter,
                CollectiveType::Gather,
                CollectiveType::AllToAll,
            ],
        ));

        Self {
            options,
            worker_id,
            group,
            local_send_receive,
            remote_send_receive,
            local_broadcast,
            remote_broadcast_send,
            counters: Arc::new(counters),
            message_store,
            enable_message_chunking: enable_message_chunking,
            message_chunk_size: message_chunk_size,
        }
    }

    pub async fn send(&self, dest: u32, data: Bytes) -> Result<()> {
        let counter = self.get_counter(&CollectiveType::Direct);
        self.send_collective(dest, data, CollectiveType::Direct, counter)
            .await?;
        self.increment_counter(&CollectiveType::Direct);
        Ok(())
    }

    pub async fn recv(&self, from: u32) -> Result<Message> {
        // If there is a message in the buffer, return it
        if let Some(msg) = self.message_store.get_any(&from, &CollectiveType::Direct) {
            return Ok(msg);
        };

        self.receive_any_message(from, &CollectiveType::Direct)
            .await
    }

    pub async fn broadcast(&self, data: Option<Bytes>) -> Result<Message> {
        let counter = self.get_counter(&CollectiveType::Broadcast);

        // If root worker, broadcast
        if self.worker_id == ROOT_ID {
            if data.is_none() {
                return Err("Root worker must send data".into());
            }
            let data = data.unwrap();
            let chunk_id = 0;
            let num_chunks = 1;

            let msg = Message {
                sender_id: self.worker_id,
                chunk_id,
                num_chunks,
                counter,
                collective: CollectiveType::Broadcast,
                data,
            };

            let local_fut = self.local_broadcast.broadcast_send(&msg);

            if self.enable_message_chunking {
                // do remote send with chunking if enabled
                let chunked_messages = chunk_message(&msg, self.message_chunk_size);
                log::debug!("Chunked message in {} parts", chunked_messages.len());

                futures::future::try_join_all(
                    chunked_messages
                        .iter()
                        .map(|msg| self.remote_broadcast_send.broadcast_send(msg)),
                )
                .await?;
            } else {
                // do remote send in one chunk
                self.remote_broadcast_send.broadcast_send(&msg).await?;
            }

            local_fut.await?;
        } else {
            // If there is a message in the buffer, return it
            if let Some(msg) =
                self.message_store
                    .get(&ROOT_ID, &CollectiveType::Broadcast, &counter)
            {
                // Increment broadcast counter
                self.increment_counter(&CollectiveType::Broadcast);
                return Ok(msg);
            }
        }

        // Broadcast messages will only be received via the local channel
        // Remote broadcast messages will be sent to the local channel
        // by a separate thread
        let m = self.receive_broadcast_message(counter).await?;

        // Increment broadcast counter
        self.increment_counter(&CollectiveType::Broadcast);

        Ok(m)
    }

    pub async fn gather(&self, data: Bytes) -> Result<Option<Vec<Message>>> {
        let counter = self.get_counter(&CollectiveType::Gather);

        let mut r = None;

        if self.worker_id == ROOT_ID {
            let mut gathered = Vec::new();
            gathered.push(Message {
                sender_id: self.worker_id,
                chunk_id: 0,
                num_chunks: 1,
                counter,
                collective: CollectiveType::Gather,
                data,
            });

            // If there are messages in the buffer get them all
            let messages = self.get_all_messages_collective(counter, &CollectiveType::Gather);

            let sender_ids = messages.iter().map(|x| x.sender_id).collect::<HashSet<_>>();

            // If there are messages missing, receive them
            if sender_ids.len() != self.options.burst_size as usize {
                let msgs = futures::future::try_join_all(
                    (0..self.options.burst_size)
                        .filter(|id| !sender_ids.contains(id) && *id != self.worker_id)
                        .map(|id| self.receive_message(id, &CollectiveType::Gather, counter)),
                )
                .await?;
                gathered.extend(msgs);
            }

            gathered.extend(messages);

            // Sort by sender_id
            gathered.sort_by_key(|msg| msg.sender_id);

            r = Some(gathered);
        } else {
            self.send_collective(ROOT_ID, data, CollectiveType::Gather, counter)
                .await?;
        }

        // Increment gather counter
        self.increment_counter(&CollectiveType::Gather);

        Ok(r)
    }

    pub async fn scatter(&self, data: Option<Vec<Bytes>>) -> Result<Option<Message>> {
        let counter = self.get_counter(&CollectiveType::Scatter);
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
                self.send_collective(i as u32 + 1, data, CollectiveType::Scatter, counter)
            }))
            .await?;
        } else {
            // If there is a message in the buffer, return it
            r = if let Some(msg) =
                self.get_message_collective(ROOT_ID, &CollectiveType::Scatter, counter)
            {
                Some(msg)
            } else {
                Some(
                    self.receive_message(ROOT_ID, &CollectiveType::Scatter, counter)
                        .await?,
                )
            };
        }

        // Increment scatter counter
        self.increment_counter(&CollectiveType::Scatter);
        Ok(r)
    }

    pub async fn all_to_all(&self, data: Vec<Bytes>) -> Result<Vec<Message>> {
        let counter = self.get_counter(&CollectiveType::AllToAll);

        if data.len() != self.options.burst_size as usize {
            return Err("Data size must be equal to burst size".into());
        }

        // first send to all workers
        futures::future::try_join_all(data.into_iter().enumerate().map(|(i, data)| {
            self.send_collective(i as u32, data, CollectiveType::AllToAll, counter)
        }))
        .await?;

        let mut r = futures::future::try_join_all((0..self.options.burst_size).map(|from| {
            //let message_count = message_count.clone();
            async move {
                let r = if let Some(msg) =
                    self.get_message_collective(from, &CollectiveType::AllToAll, counter)
                {
                    msg
                } else {
                    self.receive_message(from, &CollectiveType::AllToAll, counter)
                        .await?
                };
                Ok::<Message, Error>(r)
            }
        }))
        .await?;

        // Sort by sender_id
        r.sort_by_key(|msg| msg.sender_id);

        // Increment all_to_all counter
        self.increment_counter(&CollectiveType::AllToAll);
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
        counter: u32,
    ) -> Result<()> {
        if dest >= self.options.burst_size {
            return Err("worker with id {} does not exist".into());
        }

        let msg = Message {
            sender_id: self.worker_id,
            chunk_id: 0,
            num_chunks: 1,
            counter,
            collective,
            data,
        };

        if self.group.contains(&dest) {
            // do local send always in one chunk
            return self.local_send_receive.send(dest, &msg).await;
        } else {
            if self.enable_message_chunking {
                // do remote send with chunking if enabled
                let chunked_messages = chunk_message(&msg, self.message_chunk_size);
                log::debug!("Chunked message in {} parts", chunked_messages.len());

                futures::future::try_join_all(
                    chunked_messages
                        .iter()
                        .map(|msg| self.remote_send_receive.send(dest, msg)),
                )
                .await?;

                return Ok(());
            } else {
                // do remote send in one chunk
                return self.remote_send_receive.send(dest, &msg).await;
            }
        };
    }

    fn get_message_collective(
        &self,
        from: u32,
        collective: &CollectiveType,
        counter: u32,
    ) -> Option<Message> {
        return self.message_store.get(&from, collective, &counter);
    }

    fn get_all_messages_collective(
        &self,
        counter: u32,
        collective: &CollectiveType,
    ) -> Vec<Message> {
        let mut result = Vec::new();
        // From all senders except self worker id
        for id in HashSet::<u32>::from_iter((0..self.options.burst_size).into_iter())
            .difference(&HashSet::from_iter(vec![self.worker_id]))
        {
            result.extend(self.message_store.get(id, collective, &counter));
        }
        result
    }

    async fn receive_message(
        &self,
        from: u32,
        collective: &CollectiveType,
        counter: u32,
    ) -> Result<Message> {
        if from >= self.options.burst_size {
            return Err("worker with id {} does not exist".into());
        }
        log::debug!(
            "worker {} waiting for message from {}",
            self.worker_id,
            from
        );
        loop {
            // receive blocking
            let recv_msg = self.receive(from).await?;
            log::debug!("worker {} received message {:?}", self.worker_id, recv_msg);

            if recv_msg.num_chunks == 1
                && recv_msg.counter == counter
                && recv_msg.collective == *collective
            {
                return Ok(recv_msg);
            }

            self.message_store.insert(recv_msg);

            if let Some(msg) = self.message_store.get(&from, collective, &counter) {
                return Ok(msg);
            };
        }
    }

    async fn receive_any_message(&self, from: u32, collective: &CollectiveType) -> Result<Message> {
        if from >= self.options.burst_size {
            return Err("worker with id {} does not exist".into());
        }

        loop {
            // receive blocking
            let msg = self.receive(from).await?;
            log::debug!("worker {} received message {:?}", self.worker_id, msg);

            if msg.num_chunks == 1 && msg.collective == *collective {
                return Ok(msg);
            }

            self.message_store.insert(msg);
            if let Some(msg) = self.message_store.get_any(&from, collective) {
                log::debug!("got complete message {:?}", msg);
                return Ok(msg);
            };
            // if msg.collective == *collective {
            //     return Ok(msg);
            // } else {
            //     self.message_store.insert(msg);
            // }
        }
    }

    async fn receive_broadcast_message(&self, counter: u32) -> Result<Message> {
        loop {
            // receive blocking
            let msg = self.receive_broadcast().await?;
            log::debug!("worker {} received message {:?}", self.worker_id, msg);

            if msg.num_chunks == 1 && msg.counter == counter {
                return Ok(msg);
            }

            let sender_id = msg.sender_id;
            self.message_store.insert(msg);
            if let Some(msg) =
                self.message_store
                    .get(&sender_id, &CollectiveType::Broadcast, &counter)
            {
                log::debug!("Got complete message {:?}", msg);
                return Ok(msg);
            };

            // let msg = self.receive_broadcast().await?;
            // if msg.counter == counter && msg.collective == CollectiveType::Broadcast {
            // return Ok(msg);
            // } else {
            // self.message_store.insert(msg);
            // }
        }
    }

    async fn receive(&self, from: u32) -> Result<Message> {
        if from >= self.options.burst_size {
            return Err("worker with id {} does not exist".into());
        }

        let proxy = if self.group.contains(&from) {
            &self.local_send_receive
        } else {
            &self.remote_send_receive
        };

        proxy.recv().await
    }

    async fn receive_broadcast(&self) -> Result<Message> {
        self.local_broadcast.broadcast_recv().await
    }

    fn get_counter(&self, collective: &CollectiveType) -> u32 {
        AtomicCounter::get(&*self.counters, collective).unwrap()
    }

    fn increment_counter(&self, collective: &CollectiveType) {
        AtomicCounter::inc(&*self.counters, collective);
    }
}
