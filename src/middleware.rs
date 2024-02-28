use crate::{
    chunk_store::chunk_message, impl_chainable_setter, message_store::MessageStoreChunked,
    CollectiveType, Message, Result,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::FuturesUnordered, StreamExt};
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
    sync::Arc,
};

const ROOT_ID: u32 = 0;

const MB: usize = 1024 * 1024;
const DEFAULT_MESSAGE_CHUNK_SIZE: usize = 1 * MB;

pub trait SendReceiveProxy: SendProxy + ReceiveProxy + Send + Sync {}

#[async_trait]
pub trait SendProxy: Send + Sync {
    async fn send(&self, dest: u32, msg: Message) -> Result<()>;
}

#[async_trait]
pub trait ReceiveProxy: Send + Sync {
    async fn recv(&self, source: u32) -> Result<Message>;
}

pub trait BroadcastProxy: BroadcastSendProxy + BroadcastReceiveProxy + Send + Sync {}

#[async_trait]
pub trait BroadcastSendProxy: Send + Sync {
    async fn broadcast_send(&self, msg: Message) -> Result<()>;
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
    ) -> Result<HashMap<u32, (Box<dyn SendReceiveProxy>, Box<dyn BroadcastProxy>)>>;
}

#[async_trait]
pub trait SendReceiveLocalFactory<T>: Send + Sync {
    async fn create_proxies(
        burst_options: Arc<BurstOptions>,
        options: T,
    ) -> Result<HashMap<u32, (Box<dyn SendReceiveProxy>, Box<dyn BroadcastProxy>)>>;
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

#[derive(Clone, Debug)]
pub struct BurstInfo {
    pub burst_id: String,
    pub burst_size: u32,
    pub group_ranges: HashMap<String, HashSet<u32>>,
    pub worker_id: u32,
    pub group_id: String,
}

impl BurstOptions {
    pub fn new(
        burst_size: u32,
        group_ranges: HashMap<String, HashSet<u32>>,
        group_id: String,
    ) -> Self {
        Self {
            burst_id: "default".to_string(),
            burst_size,
            group_ranges,
            group_id,
            enable_message_chunking: false,
            message_chunk_size: DEFAULT_MESSAGE_CHUNK_SIZE,
        }
    }

    impl_chainable_setter!(burst_id, String);
    impl_chainable_setter!(burst_size, u32);
    impl_chainable_setter!(group_ranges, HashMap<String, HashSet<u32>>);
    impl_chainable_setter!(group_id, String);
    impl_chainable_setter!(enable_message_chunking, bool);
    impl_chainable_setter!(message_chunk_size, usize);

    pub fn build(&self) -> Self {
        self.clone()
    }
}

pub struct BurstMiddleware {
    options: Arc<BurstOptions>,

    worker_id: u32,
    group: HashSet<u32>,
    group_worker_leader: u32,

    local_send_receive: Arc<dyn SendReceiveProxy>,
    remote_send_receive: Arc<dyn SendReceiveProxy>,

    local_broadcast: Arc<dyn BroadcastProxy>,
    remote_broadcast: Arc<dyn BroadcastProxy>,

    collective_counters: HashMap<CollectiveType, u32>,
    send_counters: HashMap<u32, u32>,
    receive_counters: HashMap<u32, u32>,

    message_buffer: MessageStoreChunked,
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
        log::debug!("Creating proxies {:?}", options);
        let options = Arc::new(options);
        let current_group = options.group_ranges.get(&options.group_id).unwrap();

        let mut local_proxies =
            LocalImpl::create_proxies(options.clone(), local_impl_options).await?;
        let mut remote_proxies =
            RemoteImpl::create_proxies(options.clone(), remote_impl_options).await?;

        let mut proxies = HashMap::new();

        for id in current_group {
            let (local_direct_proxy, local_broadcast_proxy) = local_proxies.remove(id).unwrap();
            let (remote_direct_proxy, remote_broadcast_proxy) = remote_proxies.remove(id).unwrap();
            let proxy = BurstMiddleware::new(
                options.clone(),
                local_direct_proxy,
                remote_direct_proxy,
                local_broadcast_proxy,
                remote_broadcast_proxy,
                *id,
                current_group.clone(),
            );
            proxies.insert(*id, proxy);
        }

        Ok(proxies)
    }

    pub fn new(
        options: Arc<BurstOptions>,
        local_send_receive: Box<dyn SendReceiveProxy>,
        remote_send_receive: Box<dyn SendReceiveProxy>,
        local_broadcast: Box<dyn BroadcastProxy>,
        remote_broadcast: Box<dyn BroadcastProxy>,
        worker_id: u32,
        group: HashSet<u32>,
    ) -> Self {
        let enable_message_chunking = options.enable_message_chunking;
        let message_chunk_size = options.message_chunk_size;

        // create counters
        let counters = [
            CollectiveType::Broadcast,
            CollectiveType::Gather,
            CollectiveType::Scatter,
            CollectiveType::AllToAll,
        ]
        .into_iter()
        .map(|c| (c, 0))
        .collect();

        let send_counters: HashMap<u32, u32> = (0..options.burst_size).map(|id| (id, 0)).collect();
        let receive_counters = send_counters.clone();

        let message_store = MessageStoreChunked::new(
            0..options.burst_size,
            &[
                CollectiveType::Direct,
                CollectiveType::Broadcast,
                CollectiveType::Scatter,
                CollectiveType::Gather,
                CollectiveType::AllToAll,
            ],
        );

        // The worker with the lowest id in the group is the group leader
        let group_worker_leader = *group.iter().min().unwrap();

        Self {
            options,
            worker_id,
            group,
            group_worker_leader,
            local_send_receive: local_send_receive.into(),
            remote_send_receive: remote_send_receive.into(),
            local_broadcast: local_broadcast.into(),
            remote_broadcast: remote_broadcast.into(),
            collective_counters: counters,
            send_counters,
            receive_counters,
            message_buffer: message_store,
            enable_message_chunking,
            message_chunk_size,
        }
    }

    pub async fn send(&mut self, dest: u32, data: Bytes) -> Result<()> {
        let counter = Self::get_counter(&self.send_counters, &dest)?;
        let msg = Message {
            sender_id: self.worker_id,
            chunk_id: 0,
            num_chunks: 1,
            counter,
            collective: CollectiveType::Direct,
            data,
        };
        self.send_message(dest, msg).await?;
        Self::increment_counter(&mut self.send_counters, &dest)?;
        Ok(())
    }

    pub async fn recv(&mut self, from: u32) -> Result<Message> {
        // let counter = self.get_counter(&CollectiveType::Direct);
        let counter = Self::get_counter(&self.receive_counters, &from)?;
        let msg = self
            .get_message(from, &CollectiveType::Direct, counter)
            .await?;
        Self::increment_counter(&mut self.receive_counters, &from)?;
        Ok(msg)
    }

    pub async fn broadcast(&mut self, data: Option<Bytes>, root: u32) -> Result<Message> {
        let counter = Self::get_counter(&self.collective_counters, &CollectiveType::Broadcast)?;

        if self.worker_id == root {
            // The root worker sends the broadcast message
            let data = data.expect("Root worker must send data");

            let msg = Message {
                sender_id: self.worker_id,
                chunk_id: 0,
                num_chunks: 1,
                counter,
                collective: CollectiveType::Broadcast,
                data,
            };

            // Send the message to the local channel for the local group
            let local_fut = self.local_broadcast.broadcast_send(msg.clone());

            if self.enable_message_chunking {
                // do remote send with chunking if enabled
                let chunked_messages = chunk_message(&msg, self.message_chunk_size);
                log::debug!("Chunked message in {} parts", chunked_messages.len());

                let futures = chunked_messages
                    .into_iter()
                    .map(|msg| Self::broadcast_send(Arc::clone(&self.remote_broadcast), msg))
                    .map(tokio::spawn)
                    .collect::<FuturesUnordered<_>>();
                futures::future::try_join_all(futures).await?;
            } else {
                // do remote send in one chunk
                self.remote_broadcast.broadcast_send(msg).await?;
            }

            local_fut.await?;
        } else {
            // For non-root workers, check if the root worker is remote
            // and if this worker is the group leader
            if !self.group.contains(&root) && self.worker_id == self.group_worker_leader {
                // Only the group leader receives the broadcast message via the remote channel
                // And it will send it to the rest of the group via the local channel
                let msg = self.get_broadcast_message(root, counter).await?;
                // let msg = self.remote_broadcast.broadcast_recv().await?;
                self.local_broadcast.broadcast_send(msg).await?;
            }
        }

        // Eventually all workers (root, local and remote)
        // will receive the broadcast message via the local channel
        let msg = self.local_broadcast.broadcast_recv().await?;
        // Increment broadcast counter
        Self::increment_counter(&mut self.collective_counters, &CollectiveType::Broadcast)?;

        Ok(msg)
    }

    pub async fn gather(&mut self, data: Bytes) -> Result<Option<Vec<Message>>> {
        let counter = Self::get_counter(&self.collective_counters, &CollectiveType::Gather)?;

        let mut result: Option<Vec<Message>> = None;
        let msg = Message {
            sender_id: self.worker_id,
            chunk_id: 0,
            num_chunks: 1,
            counter,
            collective: CollectiveType::Gather,
            data,
        };

        if self.worker_id == ROOT_ID {
            let mut gathered = Vec::with_capacity(self.options.burst_size as usize);
            gathered.push(msg);

            // create a new hashset with all worker ids except self
            let sender_ids = HashSet::<u32>::from_iter(0..self.options.burst_size)
                .difference(&HashSet::from_iter(vec![self.worker_id]))
                .copied()
                .collect::<HashSet<u32>>();
            let messages = self
                .get_messages(&CollectiveType::Gather, counter, sender_ids)
                .await?;

            gathered.extend(messages);
            gathered.sort_by_key(|msg| msg.sender_id);

            result = Some(gathered)
        } else {
            self.send_message(ROOT_ID, msg).await?;
        }

        Self::increment_counter(&mut self.collective_counters, &CollectiveType::Gather)?;
        Ok(result)
    }

    pub async fn scatter(&mut self, data: Option<Vec<Bytes>>) -> Result<Message> {
        let counter = Self::get_counter(&self.collective_counters, &CollectiveType::Scatter)?;

        let result: Message;

        if self.worker_id == ROOT_ID {
            let data = data.expect("Root worker must send data");
            if data.len() != (self.options.burst_size as usize) {
                return Err("Data size must be equal to burst size".into());
            }

            result = Message {
                sender_id: ROOT_ID,
                chunk_id: 0,
                num_chunks: 1,
                counter,
                collective: CollectiveType::Scatter,
                data: data[ROOT_ID as usize].clone(),
            };

            let messages = data
                .into_iter()
                .enumerate()
                .map(|(to, data)| {
                    (
                        to as u32,
                        Message {
                            sender_id: ROOT_ID,
                            chunk_id: 0,
                            num_chunks: 1,
                            counter,
                            collective: CollectiveType::Scatter,
                            data,
                        },
                    )
                })
                .collect();

            self.send_messages(messages).await?;
        } else {
            result = self
                .get_message(ROOT_ID, &CollectiveType::Scatter, counter)
                .await?;
        }

        // Increment scatter counter
        Self::increment_counter(&mut self.collective_counters, &CollectiveType::Scatter)?;
        Ok(result)
    }

    pub async fn all_to_all(&mut self, data: Vec<Bytes>) -> Result<Vec<Message>> {
        let counter = Self::get_counter(&self.collective_counters, &CollectiveType::AllToAll)?;

        if data.len() != (self.options.burst_size as usize) {
            return Err("Data size must be equal to burst size".into());
        }

        // first send to all workers
        let send_messages = data
            .into_iter()
            .enumerate()
            .map(|(to, data)| {
                (
                    to as u32,
                    Message {
                        sender_id: ROOT_ID,
                        chunk_id: 0,
                        num_chunks: 1,
                        counter,
                        collective: CollectiveType::Scatter,
                        data,
                    },
                )
            })
            .collect();

        self.send_messages(send_messages).await?;

        let mut received_messages = self
            .get_messages(
                &CollectiveType::AllToAll,
                counter,
                HashSet::from_iter(0..self.options.burst_size),
            )
            .await?;

        // Sort by sender_id
        received_messages.sort_by_key(|msg| msg.sender_id);

        // Increment all_to_all counter
        Self::increment_counter(&mut self.collective_counters, &CollectiveType::AllToAll)?;
        Ok(received_messages)
    }

    pub fn info(&self) -> BurstInfo {
        BurstInfo {
            burst_id: self.options.burst_id.clone(),
            burst_size: self.options.burst_size,
            group_ranges: self.options.group_ranges.clone(),
            worker_id: self.worker_id,
            group_id: self.options.group_id.clone(),
        }
    }

    async fn send_message(&self, to: u32, msg: Message) -> Result<()> {
        if to >= self.options.burst_size {
            return Err("worker with id {} does not exist".into());
        }

        if self.group.contains(&to) {
            // do local send always in one chunk
            return self.local_send_receive.send(to, msg).await;
        } else if self.enable_message_chunking {
            // do remote send with chunking if enabled
            let chunked_messages = chunk_message(&msg, self.message_chunk_size);
            log::debug!("Chunked message in {} parts", chunked_messages.len());
            self.send_messages_to(to, chunked_messages).await?;
        } else {
            // do remote send in one chunk
            return self.remote_send_receive.send(to, msg).await;
        }

        Ok(())
    }

    async fn send_messages(&self, msgs: Vec<(u32, Message)>) -> Result<()> {
        let futures: FuturesUnordered<_> = msgs
            .into_iter()
            .map(|(to, msg)| {
                if self.group.contains(&to) {
                    (to, Arc::clone(&self.local_send_receive), msg)
                } else {
                    (to, Arc::clone(&self.remote_send_receive), msg)
                }
            })
            .flat_map(|(to, proxy, msg)| {
                if self.enable_message_chunking {
                    // do remote send with chunking if enabled
                    let chunked_messages = chunk_message(&msg, self.message_chunk_size);
                    log::debug!("Chunked message in {} parts", chunked_messages.len());
                    chunked_messages
                        .into_iter()
                        .map(|msg| (to, Arc::clone(&proxy), msg))
                        .collect::<Vec<_>>()
                } else {
                    // do remote send in one chunk
                    vec![(to, proxy, msg)]
                }
            })
            .map(|(id, proxy, msg)| Self::proxy_send(id, proxy, msg))
            .map(tokio::spawn)
            .collect::<FuturesUnordered<_>>();

        futures::future::try_join_all(futures).await?;
        Ok(())
    }

    async fn send_messages_to(&self, to: u32, msgs: Vec<Message>) -> Result<()> {
        let proxy = if self.group.contains(&to) {
            &self.local_send_receive
        } else {
            &self.remote_send_receive
        };
        let futures = msgs
            .into_iter()
            .map(|msg| Self::proxy_send(to, Arc::clone(proxy), msg))
            .map(tokio::spawn)
            .collect::<FuturesUnordered<_>>();

        futures::future::try_join_all(futures).await?;
        Ok(())
    }

    async fn get_message(
        &mut self,
        from: u32,
        collective: &CollectiveType,
        counter: u32,
    ) -> Result<Message> {
        if from >= self.options.burst_size {
            return Err("worker with id {} does not exist".into());
        }
        log::debug!(
            "[Worker {:?}] get_message: from => {:?} collective => {:?} counter => {:?}",
            self.worker_id,
            from,
            collective,
            counter
        );

        loop {
            // Check if complete message is in buffer
            if let Some(msg) = self.message_buffer.get(&from, collective, &counter) {
                return Ok(msg);
            };

            // Block and receive next message
            let proxy = if self.group.contains(&from) {
                &self.local_send_receive
            } else {
                &self.remote_send_receive
            };

            let msg = proxy.recv(from).await?;
            log::debug!("[Worker {}] received message {:?}", self.worker_id, msg);

            // Check if this is the message we are waiting for
            if msg.counter == counter && msg.collective == *collective && msg.sender_id == from {
                if msg.num_chunks == 1 {
                    return Ok(msg);
                } else {
                    // If message is chunked, we need to receive all chunks
                    let complete_msg = self.get_complete_message(msg, Arc::clone(proxy)).await?;
                    return Ok(complete_msg);
                }
            } else {
                // got a message with another collective or counter, loop until we receive what we want
                self.message_buffer.insert(msg);
                log::debug!(
                    "[Worker {}] Put message in buffer, get next message",
                    self.worker_id
                )
            }
        }
    }

    async fn get_complete_message(
        &mut self,
        msg_chunk: Message,
        proxy: Arc<dyn SendReceiveProxy>,
    ) -> Result<Message> {
        if !self.enable_message_chunking || (msg_chunk.num_chunks) < 2 {
            panic!("get_complete_message called with non-chunked message")
        }

        let from = msg_chunk.sender_id;
        let collective = msg_chunk.collective;
        let counter = msg_chunk.counter;
        let num_chunks = msg_chunk.num_chunks;

        // put first chunk into buffer, we will put the rest as we receive them
        self.message_buffer.insert(msg_chunk);

        // Check if we have some chunks already in the buffer
        let missing_chunks =
            match self
                .message_buffer
                .num_chunks_stored(&from, &collective, &counter)
            {
                Some(n) => num_chunks - n, // we will have at least 1 chunk in the buffer
                None => panic!("Inserted first chunk into buffer but now it's gone"), // we should have at least the first chunk in the buffer
            };

        if missing_chunks == 0 {
            // we already have all chunks in the buffer, this was the last one
            if let Some(msg) = self.message_buffer.get(&from, &collective, &counter) {
                return Ok(msg);
            } else {
                // something went wrong
                return Err("There are no missing chunks but the message is not complete".into());
            }
        }

        log::debug!(
            "[Worker {}] Waiting for {} missing chunks for message (collective={}, counter={}, sender={})",
            self.worker_id,
            missing_chunks,
            collective,
            counter,
            from
        );

        // we will expect, at least, N - 1 more messages, where N is the number of chunks
        let mut futures = (0..missing_chunks)
            .map(|_| Self::proxy_recv(from, Arc::clone(&proxy)))
            .map(tokio::spawn)
            .collect::<FuturesUnordered<_>>();

        // Loop until all chunks are received
        while let Some(fut) = futures.next().await {
            match fut {
                Ok((id, fut_res)) => {
                    let msg = fut_res?;
                    log::debug!("[Worker {}] received message {:?}", self.worker_id, msg);

                    if msg.sender_id != id || msg.counter != counter || msg.collective != collective
                    {
                        // we got a message with another collective, counter or sender, we will need to receive yet another message
                        futures.push(tokio::spawn(Self::proxy_recv(id, Arc::clone(&proxy))));
                    }

                    // Put msg into buffer, either if it's a chunk or another unrelated message
                    self.message_buffer.insert(msg);
                }
                Err(e) => {
                    log::error!("Error receiving message {:?}", e);
                }
            }
        }

        log::debug!("[Worker {}] received all chunks", self.worker_id);

        // at this point, we should have all chunks in the buffer
        if let Some(msg) = self.message_buffer.get(&from, &collective, &counter) {
            Ok(msg)
        } else {
            // something went wrong
            // TODO try receiving more messages?
            Err("Waited for all chunks but some are missing".into())
        }
    }

    async fn get_messages(
        &mut self,
        collective: &CollectiveType,
        counter: u32,
        sender_ids: HashSet<u32>,
    ) -> Result<Vec<Message>> {
        let mut messages = Vec::with_capacity(sender_ids.len());

        // Retrieve pending messages from buffer
        let mut sender_ids_found: HashSet<u32> = HashSet::new();
        for id in sender_ids.iter() {
            if let Some(msg) = self.message_buffer.get(id, collective, &counter) {
                messages.push(msg);
                sender_ids_found.insert(*id);
            }
        }

        // Receive missing messages
        let mut futures: FuturesUnordered<_> = sender_ids
            .difference(&sender_ids_found)
            .map(|id| {
                if self.group.contains(id) {
                    (*id, Arc::clone(&self.local_send_receive))
                } else {
                    (*id, Arc::clone(&self.remote_send_receive))
                }
            })
            .map(|(id, proxy)| Self::proxy_recv(id, proxy))
            .map(tokio::spawn)
            .collect::<FuturesUnordered<_>>();

        // Loop until all messages are received
        while let Some(fut) = futures.next().await {
            match fut {
                Ok((id, fut_res)) => {
                    let msg = fut_res?;
                    if msg.num_chunks == 1
                        && msg.counter == counter
                        && msg.collective == *collective
                    {
                        messages.push(msg);
                    } else {
                        let sender_id = msg.sender_id;

                        // Received either a partial message, or other collective message
                        // put it into buffer
                        self.message_buffer.insert(msg);

                        // check if we have a complete message in the buffer
                        if let Some(msg) = self.message_buffer.get(&sender_id, collective, &counter)
                        {
                            messages.push(msg);
                        } else {
                            // if not, spawn a new task to receive another message
                            let proxy = if self.group.contains(&id) {
                                Arc::clone(&self.local_send_receive)
                            } else {
                                Arc::clone(&self.remote_send_receive)
                            };
                            futures.push(tokio::spawn(Self::proxy_recv(id, proxy)));
                        }
                    }
                }
                Err(e) => {
                    log::error!("Error receiving message {:?}", e);
                }
            }
        }

        Ok(messages)
    }

    async fn get_broadcast_message(&mut self, root: u32, counter: u32) -> Result<Message> {
        // Check if complete message is in buffer
        if let Some(msg) = self
            .message_buffer
            .get(&ROOT_ID, &CollectiveType::Broadcast, &counter)
        {
            return Ok(msg);
        };

        // Loop until we receive a message with the counter we are waiting for
        loop {
            let msg = Self::broadcast_recv(Arc::clone(&self.remote_broadcast)).await?;
            log::debug!(
                "[Worker {}] received broadcast message {:?}",
                self.worker_id,
                msg
            );

            // Check if this is the message we are waiting for
            if msg.counter == counter {
                if msg.num_chunks == 1 {
                    return Ok(msg);
                }

                // If message is chunked, we need to receive all chunks
                if !self.enable_message_chunking || (msg.num_chunks) < 2 {
                    panic!("get_complete_message called with non-chunked message")
                }

                // put first chunk into buffer, we will put the rest as we receive them
                let num_chunks = msg.num_chunks;
                self.message_buffer.insert(msg);

                // Check if we have some chunks already in the buffer
                let missing_chunks = match self.message_buffer.num_chunks_stored(
                    &root,
                    &CollectiveType::Broadcast,
                    &counter,
                ) {
                    Some(n) => num_chunks - n, // we will have at least 1 chunk in the buffer
                    None => panic!("Inserted first chunk into buffer but now it's gone"), // we should have at least the first chunk in the buffer
                };

                if missing_chunks == 0 {
                    // we already have all chunks in the buffer, this was the last one
                    if let Some(msg) =
                        self.message_buffer
                            .get(&root, &CollectiveType::Broadcast, &counter)
                    {
                        return Ok(msg);
                    } else {
                        // something went wrong
                        return Err(
                            "There are no missing chunks but the message is not complete".into(),
                        );
                    }
                }

                log::debug!(
                    "Waiting for {} missing chunks for broadcast message (counter={})",
                    missing_chunks,
                    counter,
                );

                // we will expect, at least, N - 1 more messages, where N is the number of chunks
                let mut futures = (0..missing_chunks)
                    .map(|_| Self::broadcast_recv(Arc::clone(&self.remote_broadcast)))
                    .map(tokio::spawn)
                    .collect::<FuturesUnordered<_>>();

                // Loop until all chunks are received
                while let Some(fut) = futures.next().await {
                    match fut {
                        Ok(fut_res) => {
                            let msg = fut_res?;
                            log::debug!("Received broadcast message {:?}", msg);

                            if msg.counter != counter {
                                // we got a message with another counter
                                // we will need to receive yet another message
                                futures.push(tokio::spawn(Self::broadcast_recv(Arc::clone(
                                    &self.remote_broadcast,
                                ))));
                            }

                            // Put msg into buffer, either if it's a chunk or another unrelated message
                            self.message_buffer.insert(msg);
                        }
                        Err(e) => {
                            log::error!("Error receiving message {:?}", e);
                        }
                    }
                }

                log::debug!("Received all broadcast chunks for counter={}", counter);

                // at this point, we should have all chunks in the buffer
                if let Some(msg) =
                    self.message_buffer
                        .get(&root, &CollectiveType::Broadcast, &counter)
                {
                    return Ok(msg);
                } else {
                    // something went wrong
                    // TODO try receiving more messages?
                    return Err("Waited for all chunks but some are missing".into());
                }
            }
            // we got a message with another counter, put it into buffer and get next message
            self.message_buffer.insert(msg);
        }
    }

    async fn proxy_recv(from: u32, proxy: Arc<dyn SendReceiveProxy>) -> (u32, Result<Message>) {
        (from, proxy.recv(from).await)
    }

    async fn proxy_send(to: u32, proxy: Arc<dyn SendReceiveProxy>, msg: Message) -> Result<()> {
        proxy.send(to, msg).await
    }

    async fn broadcast_send(proxy: Arc<dyn BroadcastProxy>, msg: Message) -> Result<()> {
        proxy.broadcast_send(msg).await
    }

    async fn broadcast_recv(proxy: Arc<dyn BroadcastProxy>) -> Result<Message> {
        proxy.broadcast_recv().await
    }

    fn get_counter<T>(map: &HashMap<T, u32>, key: &T) -> Result<u32>
    where
        T: Eq + PartialEq + Hash + Debug,
    {
        map.get(key)
            .copied()
            .ok_or(format!("Counter not found for key {:?}", key).into())
    }

    fn increment_counter<T>(map: &mut HashMap<T, u32>, key: &T) -> Result<()>
    where
        T: Eq + PartialEq + Hash + Debug,
    {
        let v = map
            .get_mut(key)
            .ok_or(format!("Counter not found for key {:?}", key))?;
        *v += 1;
        Ok(())
    }
}
