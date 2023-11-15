use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tokio::sync::{
    broadcast::{Receiver, Sender},
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    Mutex,
};

use crate::{
    impl_chainable_setter, BroadcastSendProxy, BurstOptions, CollectiveType, Message, Proxy,
    ReceiveProxy, Result, SendProxy, SendReceiveLocalFactory, SendReceiveProxy,
};

const DEFAULT_BROADCAST_CHANNEL_SIZE: usize = 1024 * 1024;

#[derive(Clone, Debug)]
pub struct TokioChannelOptions {
    pub broadcast_channel_size: usize,
}

impl TokioChannelOptions {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    impl_chainable_setter! {
        broadcast_channel_size, usize
    }

    pub fn build(&self) -> Self {
        self.clone()
    }
}

impl Default for TokioChannelOptions {
    fn default() -> Self {
        Self {
            broadcast_channel_size: DEFAULT_BROADCAST_CHANNEL_SIZE,
        }
    }
}

pub struct TokioChannelImpl;

#[async_trait]
impl SendReceiveLocalFactory<TokioChannelOptions> for TokioChannelImpl {
    async fn create_proxies(
        burst_options: Arc<BurstOptions>,
        channel_options: TokioChannelOptions,
    ) -> Result<(HashMap<u32, Box<dyn Proxy>>, Box<dyn BroadcastSendProxy>)> {
        let current_group = burst_options
            .group_ranges
            .get(&burst_options.group_id)
            .unwrap();

        let channel_options = Arc::new(channel_options);

        // create local channels
        let mut local_channel_tx = HashMap::new();
        let mut local_channel_rx = HashMap::new();

        for id in current_group {
            let (tx, rx) = mpsc::unbounded_channel::<Message>();
            local_channel_tx.insert(*id, tx);
            local_channel_rx.insert(*id, rx);
        }

        // create broadcast channel for this group
        let (tx, _) =
            tokio::sync::broadcast::channel::<Message>(channel_options.broadcast_channel_size);

        let mut hmap = HashMap::new();

        let local_channel_tx = Arc::new(local_channel_tx);

        futures::future::try_join_all(current_group.iter().map(|worker_id| {
            let local_tx = local_channel_tx.clone();
            let tx = tx.clone();
            let local_channel_rx = local_channel_rx.remove(worker_id).unwrap();
            async move {
                TokioChannelProxy::new(local_tx.clone(), local_channel_rx, tx.clone(), *worker_id)
                    .await
            }
        }))
        .await?
        .into_iter()
        .for_each(|proxy| {
            hmap.insert(proxy.worker_id, Box::new(proxy) as Box<dyn Proxy>);
        });

        Ok((hmap, Box::new(TokioChannelBroadcastSendProxy::new(tx))))
    }
}

pub struct TokioChannelProxy {
    worker_id: u32,
    sender: Box<dyn SendProxy>,
    receiver: Box<dyn ReceiveProxy>,
    broadcast_sender: Box<dyn BroadcastSendProxy>,
    broadcast_receiver: Box<dyn ReceiveProxy>,
}

pub struct TokioChannelSendProxy {
    local_channel_tx: Arc<HashMap<u32, UnboundedSender<Message>>>,
}

pub struct TokioChannelReceiveProxy {
    local_channel_rx: Mutex<UnboundedReceiver<Message>>,
}

pub struct TokioChannelBroadcastSendProxy {
    broadcast_channel_tx: Sender<Message>,
}

pub struct TokioChannelBroadcastReceiveProxy {
    broadcast_channel_rx: Mutex<Receiver<Message>>,
}

impl Proxy for TokioChannelProxy {}

#[async_trait]
impl BroadcastSendProxy for TokioChannelProxy {
    async fn broadcast_send(&self, msg: &Message) -> Result<()> {
        if msg.collective != CollectiveType::Broadcast {
            return Err("Cannot send non-broadcast message to broadcast".into());
        } else {
            self.broadcast_sender.broadcast_send(msg).await?;
        }
        Ok(())
    }
}

impl SendReceiveProxy for TokioChannelProxy {}

#[async_trait]
impl SendProxy for TokioChannelProxy {
    async fn send(&self, dest: u32, msg: &Message) -> Result<()> {
        self.sender.send(dest, msg).await
    }
}

#[async_trait]
impl ReceiveProxy for TokioChannelProxy {
    async fn recv(&self) -> Result<Message> {
        tokio::select! {
            msg = self.receiver.recv() => {
                msg
            },
            msg = self.broadcast_receiver.recv() => {
                msg
            }
        }
    }
}

impl TokioChannelProxy {
    pub async fn new(
        local_channel_tx: Arc<HashMap<u32, UnboundedSender<Message>>>,
        local_channel_rx: UnboundedReceiver<Message>,
        broadcast_channel_tx: Sender<Message>,
        worker_id: u32,
    ) -> Result<Self> {
        Ok(Self {
            worker_id,
            sender: Box::new(TokioChannelSendProxy::new(local_channel_tx).await?),
            receiver: Box::new(TokioChannelReceiveProxy::new(local_channel_rx).await?),
            broadcast_receiver: Box::new(TokioChannelBroadcastReceiveProxy::new(
                broadcast_channel_tx.subscribe(),
            )),
            broadcast_sender: Box::new(TokioChannelBroadcastSendProxy::new(broadcast_channel_tx)),
        })
    }
}

#[async_trait]
impl SendProxy for TokioChannelSendProxy {
    async fn send(&self, dest: u32, msg: &Message) -> Result<()> {
        if msg.collective == CollectiveType::Broadcast {
            Err("Cannot send broadcast message to a single destination".into())
        } else {
            if let Some(tx) = self.local_channel_tx.get(&dest) {
                tx.send(msg.clone())?;
            } else {
                return Err("Destination not found".into());
            }
            Ok(())
        }
    }
}

impl TokioChannelSendProxy {
    pub async fn new(
        local_channel_tx: Arc<HashMap<u32, UnboundedSender<Message>>>,
    ) -> Result<Self> {
        Ok(Self { local_channel_tx })
    }
}

#[async_trait]
impl ReceiveProxy for TokioChannelReceiveProxy {
    async fn recv(&self) -> Result<Message> {
        // Receive from local channel
        // receive blocking
        if let Some(msg) = self.local_channel_rx.lock().await.recv().await {
            Ok(msg)
        } else {
            Err("Local channel closed".into())
        }
    }
}

impl TokioChannelReceiveProxy {
    pub async fn new(local_channel_rx: UnboundedReceiver<Message>) -> Result<Self> {
        Ok(Self {
            local_channel_rx: Mutex::new(local_channel_rx),
        })
    }
}

#[async_trait]
impl BroadcastSendProxy for TokioChannelBroadcastSendProxy {
    async fn broadcast_send(&self, msg: &Message) -> Result<()> {
        if msg.collective != CollectiveType::Broadcast {
            Err("Cannot send non-broadcast message to broadcast".into())
        } else {
            self.broadcast_channel_tx.send(msg.clone())?;
            Ok(())
        }
    }
}

impl TokioChannelBroadcastSendProxy {
    pub fn new(broadcast_channel_tx: Sender<Message>) -> Self {
        Self {
            broadcast_channel_tx,
        }
    }
}

#[async_trait]
impl ReceiveProxy for TokioChannelBroadcastReceiveProxy {
    async fn recv(&self) -> Result<Message> {
        // Receive from broadcast channel
        // receive blocking
        match self.broadcast_channel_rx.lock().await.recv().await {
            Ok(msg) => Ok(msg),
            Err(e) => Err(e.into()),
        }
    }
}

impl TokioChannelBroadcastReceiveProxy {
    pub fn new(broadcast_channel_rx: Receiver<Message>) -> Self {
        Self {
            broadcast_channel_rx: Mutex::new(broadcast_channel_rx),
        }
    }
}
