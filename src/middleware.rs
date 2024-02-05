use crate::{
    chunk_store::chunk_message, counter::AtomicCounter, message_store::MessageStoreChunked,
    CollectiveType, Message, Result,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::FuturesUnordered, StreamExt};
use std::{
    collections::{HashMap, HashSet},
    sync::{atomic::AtomicU32, Arc},
};

const ROOT_ID: u32 = 0;

pub trait SendReceiveProxy: SendProxy + ReceiveProxy + Send + Sync {}

#[async_trait]
pub trait SendProxy: Send + Sync {
    async fn send(&self, dest: u32, msg: Message) -> Result<()>;
}

#[async_trait]
pub trait ReceiveProxy: Send + Sync {
    async fn recv(&self) -> Result<Message>;
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

pub struct BurstMiddleware {
    options: Arc<BurstOptions>,

    worker_id: u32,
    group: HashSet<u32>,

    local_send_receive: Arc<dyn SendReceiveProxy>,
    remote_send_receive: Arc<dyn SendReceiveProxy>,

    local_broadcast: Arc<dyn BroadcastProxy>,
    remote_broadcast_send: Arc<dyn BroadcastSendProxy>,

    collective_counters: HashMap<CollectiveType, AtomicU32>,
    send_counters: HashMap<u32, AtomicU32>,
    receive_counters: HashMap<u32, AtomicU32>,

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
                CollectiveType::Broadcast,
                CollectiveType::Gather,
                CollectiveType::Scatter,
                CollectiveType::AllToAll,
            ]
            .into_iter(),
        );
        let send_counters: HashMap<u32, AtomicU32> =
            AtomicCounter::new((0..options.burst_size).into_iter());
        let receive_counters: HashMap<u32, AtomicU32> =
            AtomicCounter::new((0..options.burst_size).into_iter());

        let message_store = MessageStoreChunked::new(
            (0..options.burst_size).into_iter(),
            &[
                CollectiveType::Direct,
                CollectiveType::Broadcast,
                CollectiveType::Scatter,
                CollectiveType::Gather,
                CollectiveType::AllToAll,
            ],
        );

        Self {
            options,
            worker_id,
            group,
            local_send_receive,
            remote_send_receive,
            local_broadcast,
            remote_broadcast_send,
            collective_counters: counters,
            send_counters: send_counters,
            receive_counters: receive_counters,
            message_buffer: message_store,
            enable_message_chunking: enable_message_chunking,
            message_chunk_size: message_chunk_size,
        }
    }

    pub async fn send(&mut self, dest: u32, data: Bytes) -> Result<()> {
        let counter = AtomicCounter::get(&self.send_counters, &dest).unwrap();
        let msg = Message {
            sender_id: self.worker_id,
            chunk_id: 0,
            num_chunks: 1,
            counter: counter,
            collective: CollectiveType::Direct,
            data,
        };
        self.send_message(dest, msg).await?;
        AtomicCounter::inc(&self.send_counters, &dest);
        Ok(())
    }

    pub async fn recv(&mut self, from: u32) -> Result<Message> {
        // let counter = self.get_counter(&CollectiveType::Direct);
        let counter = AtomicCounter::get(&self.receive_counters, &from).unwrap();
        let msg = self
            .get_message(from, &CollectiveType::Direct, counter)
            .await?;
        AtomicCounter::inc(&self.receive_counters, &from);
        return Ok(msg);
    }

    pub async fn broadcast(&mut self, data: Option<Bytes>) -> Result<Message> {
        let counter = self.get_counter(&CollectiveType::Broadcast);

        // If root worker, broadcast
        if self.worker_id == ROOT_ID {
            let data = data.expect("Root worker must send data");

            let msg = Message {
                sender_id: self.worker_id,
                chunk_id: 0,
                num_chunks: 1,
                counter,
                collective: CollectiveType::Broadcast,
                data,
            };

            // for local send we do in one chunk only
            let local_fut = self.local_broadcast.broadcast_send(msg.clone());

            if self.enable_message_chunking {
                // do remote send with chunking if enabled
                let chunked_messages = chunk_message(&msg, self.message_chunk_size);
                log::debug!("Chunked message in {} parts", chunked_messages.len());

                let futures = chunked_messages
                    .into_iter()
                    .map(|msg| Self::broadcast_send(Arc::clone(&self.remote_broadcast_send), msg))
                    .map(tokio::spawn)
                    .collect::<FuturesUnordered<_>>();
                futures::future::try_join_all(futures).await?;
            } else {
                // do remote send in one chunk
                self.remote_broadcast_send.broadcast_send(msg).await?;
            }

            local_fut.await?;
        }

        // Broadcast messages will only be received via the local channel
        // Remote broadcast messages will be sent to the local channel
        // by a separate thread
        // Root worker and local group workers will receive the broadcast message
        // via the local channel
        let m = self.get_broadcast_message(counter).await?;

        // Increment broadcast counter
        self.increment_counter(&CollectiveType::Broadcast);

        Ok(m)
    }

    pub async fn gather(&mut self, data: Bytes) -> Result<Option<Vec<Message>>> {
        let counter = self.get_counter(&CollectiveType::Gather);

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
            let sender_ids = HashSet::<u32>::from_iter((0..self.options.burst_size).into_iter())
                .difference(&HashSet::from_iter(vec![self.worker_id]))
                .map(|id| *id)
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

        self.increment_counter(&CollectiveType::Gather);
        Ok(result)
    }

    pub async fn scatter(&mut self, data: Option<Vec<Bytes>>) -> Result<Message> {
        let counter = self.get_counter(&CollectiveType::Scatter);

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
        self.increment_counter(&CollectiveType::Scatter);
        Ok(result)
    }

    pub async fn all_to_all(&mut self, data: Vec<Bytes>) -> Result<Vec<Message>> {
        let counter = self.get_counter(&CollectiveType::AllToAll);

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
                HashSet::from_iter((0..self.options.burst_size).into_iter()),
            )
            .await?;

        // Sort by sender_id
        received_messages.sort_by_key(|msg| msg.sender_id);

        // Increment all_to_all counter
        self.increment_counter(&CollectiveType::AllToAll);
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
        } else {
            if self.enable_message_chunking {
                // do remote send with chunking if enabled
                let chunked_messages = chunk_message(&msg, self.message_chunk_size);
                log::debug!("Chunked message in {} parts", chunked_messages.len());
                self.send_messages_to(to, chunked_messages).await?;
            } else {
                // do remote send in one chunk
                return self.remote_send_receive.send(to, msg).await;
            }
        }

        return Ok(());
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
            .map(|(to, proxy, msg)| {
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
            .flatten()
            .map(|(id, proxy, msg)| Self::proxy_send(id, proxy, msg))
            .map(tokio::spawn)
            .collect::<FuturesUnordered<_>>();

        futures::future::try_join_all(futures).await?;
        return Ok(());
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
        return Ok(());
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
        let msg = proxy.recv().await?;

        log::debug!("[Worker {}] received message {:?}", self.worker_id, msg);

        // Check if this is the message we are waiting for
        if msg.num_chunks == 1
            && msg.counter == counter
            && msg.collective == *collective
            && msg.sender_id == from
        {
            return Ok(msg);
        }

        if self.enable_message_chunking {
            let missing_chunks = msg.num_chunks - 1;
            self.message_buffer.insert(msg);

            log::debug!(
                "[Worker {}] Waiting for {} missing chunks",
                self.worker_id,
                missing_chunks
            );

            // we will expect, at least, N - 1 more messages, where N is the number of chunks
            let mut futures = (0..missing_chunks)
                .map(|_| Self::proxy_recv(from, Arc::clone(proxy)))
                .map(tokio::spawn)
                .collect::<FuturesUnordered<_>>();

            // Loop until all chunks are received
            while let Some(fut) = futures.next().await {
                match fut {
                    Ok((id, fut_res)) => {
                        let msg = fut_res?;
                        log::debug!("[Worker {}] received message {:?}", self.worker_id, msg);

                        if msg.counter != counter || msg.collective != *collective {
                            // we got a message with another collective or counter, we will need to receive yet another message
                            futures.push(tokio::spawn(Self::proxy_recv(id, Arc::clone(proxy))));
                        }

                        self.message_buffer.insert(msg);
                    }
                    Err(e) => {
                        log::error!("Error receiving message {:?}", e);
                    }
                }
            }

            log::debug!("[Worker {}] received all chunks", self.worker_id);

            // at this point, we should have all chunks in the buffer
            if let Some(msg) = self.message_buffer.get(&from, collective, &counter) {
                return Ok(msg);
            } else {
                // something went wrong
                // TODO try receiving more messages?
                return Err("Waited for all chunks but some are missing".into());
            }
        } else {
            // got a message with another collective or counter, loop until we receive what we want
            loop {
                let msg = proxy.recv().await?;
                log::debug!("[Worker {}] received message {:?}", self.worker_id, msg);

                // Check if this is the message we are waiting for
                if msg.num_chunks == 1
                    && msg.counter == counter
                    && msg.collective == *collective
                    && msg.sender_id == from
                {
                    return Ok(msg);
                } else {
                    // If not, it into buffer and consume next message
                    self.message_buffer.insert(msg);
                    log::debug!(
                        "[Worker {}] Put message in buffer, get next message",
                        self.worker_id
                    )
                }
            }
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
            if let Some(msg) = self.message_buffer.get(&id, collective, &counter) {
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

        return Ok(messages);
    }

    // TODO -- this is a copy of get_message, refactor
    async fn get_broadcast_message(&mut self, counter: u32) -> Result<Message> {
        // Check if complete message is in buffer
        if let Some(msg) = self
            .message_buffer
            .get(&ROOT_ID, &CollectiveType::Broadcast, &counter)
        {
            return Ok(msg);
        };

        // Block and receive next message
        let msg = Self::broadcast_recv(Arc::clone(&self.local_broadcast)).await?;
        log::debug!(
            "[Worker {}] received broadcast message {:?}",
            self.worker_id,
            msg
        );

        // Check if this is the message we are waiting for
        if msg.num_chunks == 1 && msg.counter == counter {
            return Ok(msg);
        }

        if self.enable_message_chunking {
            let missing_chunks = msg.num_chunks - 1;
            self.message_buffer.insert(msg);

            log::debug!(
                "[Worker {}] Broadcast -- Waiting for {} missing chunks",
                self.worker_id,
                missing_chunks
            );

            // we will expect, at least, N - 1 more messages, where N is the number of chunks
            let mut futures = (0..missing_chunks)
                .map(|_| Self::broadcast_recv(Arc::clone(&self.local_broadcast)))
                .map(tokio::spawn)
                .collect::<FuturesUnordered<_>>();

            // Loop until all chunks are received
            while let Some(fut) = futures.next().await {
                match fut {
                    Ok(fut_res) => {
                        let msg = fut_res?;
                        log::debug!(
                            "[Worker {}] received broadcast message {:?}",
                            self.worker_id,
                            msg
                        );

                        if msg.counter != counter {
                            // we got a message with another counter, we will need to receive yet another message
                            futures.push(tokio::spawn(Self::broadcast_recv(Arc::clone(
                                &self.local_broadcast,
                            ))));
                        }

                        self.message_buffer.insert(msg);
                    }
                    Err(e) => {
                        log::error!("Error receiving message {:?}", e);
                    }
                }
            }

            log::debug!("[Worker {}] received all broadcast chunks", self.worker_id);

            // at this point, we should have all chunks in the buffer
            if let Some(msg) =
                self.message_buffer
                    .get(&ROOT_ID, &CollectiveType::Broadcast, &counter)
            {
                return Ok(msg);
            } else {
                // something went wrong
                // TODO try receiving more messages?
                return Err("(Broadcast) Waited for all chunks but some are missing".into());
            }
        } else {
            // got a message with another counter, loop until we receive what we want
            loop {
                let msg = self.local_broadcast.broadcast_recv().await?;
                log::debug!("[Worker {}] received message {:?}", self.worker_id, msg);

                // Check if this is the message we are waiting for
                if msg.num_chunks == 1 && msg.counter == counter {
                    return Ok(msg);
                } else {
                    // If not, it into buffer and consume next message
                    self.message_buffer.insert(msg);
                    log::debug!(
                        "[Worker {}] Broadcast -- Put message in buffer, get next message",
                        self.worker_id
                    )
                }
            }
        }

        // loop {
        //     // receive blocking
        //     let msg = Self::broadcast_recv(Arc::clone(&self.local_broadcast)).await?;
        //     log::debug!("worker {} received message {:?}", self.worker_id, msg);

        //     if msg.num_chunks == 1 && msg.counter == counter {
        //         return Ok(msg);
        //     }

        //     let sender_id = msg.sender_id;
        //     self.message_store.insert(msg);
        //     if let Some(msg) =
        //         self.message_store
        //             .get(&sender_id, &CollectiveType::Broadcast, &counter)
        //     {
        //         log::debug!("Got complete message {:?}", msg);
        //         return Ok(msg);
        //     };
        // }
    }

    async fn proxy_recv(from: u32, proxy: Arc<dyn SendReceiveProxy>) -> (u32, Result<Message>) {
        (from, proxy.recv().await)
    }

    async fn proxy_send(to: u32, proxy: Arc<dyn SendReceiveProxy>, msg: Message) -> Result<()> {
        proxy.send(to, msg).await
    }

    async fn broadcast_send(proxy: Arc<dyn BroadcastSendProxy>, msg: Message) -> Result<()> {
        proxy.broadcast_send(msg).await
    }

    async fn broadcast_recv(proxy: Arc<dyn BroadcastProxy>) -> Result<Message> {
        proxy.broadcast_recv().await
    }

    fn get_counter(&self, collective: &CollectiveType) -> u32 {
        AtomicCounter::get(&self.collective_counters, collective).unwrap()
    }

    fn increment_counter(&self, collective: &CollectiveType) {
        AtomicCounter::inc(&self.collective_counters, collective);
    }
}
