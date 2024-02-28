use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use lapin::protocol::tx;
use tokio::sync::{
    broadcast::{Receiver, Sender},
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    Mutex,
};

use crate::{
    impl_chainable_setter, BroadcastProxy, BroadcastReceiveProxy, BroadcastSendProxy, BurstOptions,
    CollectiveType, Message, ReceiveProxy, Result, SendProxy, SendReceiveLocalFactory,
    SendReceiveProxy,
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

    impl_chainable_setter!(broadcast_channel_size, usize);

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
    ) -> Result<HashMap<u32, (Box<dyn SendReceiveProxy>, Box<dyn BroadcastProxy>)>> {
        let current_group = burst_options
            .group_ranges
            .get(&burst_options.group_id)
            .unwrap();

        let channel_options = Arc::new(channel_options);

        // create local channels
        let mut tx_channels = HashMap::new();
        let mut rx_channels = HashMap::new();

        for worker_id in current_group {
            let (tx, rx) = mpsc::unbounded_channel::<Message>();
            tx_channels.insert(*worker_id, tx);
            rx_channels.insert(*worker_id, rx);
        }

        // tx_channels is shared across all proxies
        let tx_channels = Arc::new(tx_channels);

        // create broadcast channel for this group
        let (broadcast_tx, _) =
            tokio::sync::broadcast::channel::<Message>(channel_options.broadcast_channel_size);

        let mut proxies = HashMap::new();

        current_group
            .iter()
            .map(|worker_id| {
                let worker_channel_rx = rx_channels.remove(worker_id).unwrap();
                (
                    TokioChannelProxy::new(*worker_id, tx_channels.clone(), worker_channel_rx),
                    TokioChannelBroadcastProxy::new(*worker_id, broadcast_tx.clone()),
                )
            })
            .for_each(|(proxy, broadcast_proxy)| {
                proxies.insert(
                    proxy.worker_id,
                    (
                        Box::new(proxy) as Box<dyn SendReceiveProxy>,
                        Box::new(broadcast_proxy) as Box<dyn BroadcastProxy>,
                    ),
                );
            });

        Ok(proxies)
    }
}

// DIRECT PROXIES

pub struct TokioChannelProxy {
    worker_id: u32,
    sender: Box<dyn SendProxy>,
    receiver: Box<dyn ReceiveProxy>,
}

pub struct TokioChannelSendProxy {
    tx_channels: Arc<HashMap<u32, UnboundedSender<Message>>>,
}

pub struct TokioChannelReceiveProxy {
    rx_channel: Mutex<UnboundedReceiver<Message>>,
}

impl SendReceiveProxy for TokioChannelProxy {}

#[async_trait]
impl SendProxy for TokioChannelProxy {
    async fn send(&self, dest: u32, msg: Message) -> Result<()> {
        self.sender.send(dest, msg).await
    }
}

#[async_trait]
impl ReceiveProxy for TokioChannelProxy {
    async fn recv(&self, source: u32) -> Result<Message> {
        self.receiver.recv(source).await
    }
}

impl TokioChannelProxy {
    pub fn new(
        worker_id: u32,
        tx_channels: Arc<HashMap<u32, UnboundedSender<Message>>>,
        rx_channel: UnboundedReceiver<Message>,
    ) -> Self {
        Self {
            worker_id,
            sender: Box::new(TokioChannelSendProxy::new(tx_channels)),
            receiver: Box::new(TokioChannelReceiveProxy::new(rx_channel)),
        }
    }
}

#[async_trait]
impl SendProxy for TokioChannelSendProxy {
    async fn send(&self, dest: u32, msg: Message) -> Result<()> {
        if let Some(tx) = self.tx_channels.get(&dest) {
            tx.send(msg.clone())?;
        } else {
            return Err("Destination not found".into());
        }
        Ok(())
    }
}

impl TokioChannelSendProxy {
    pub fn new(local_channel_tx: Arc<HashMap<u32, UnboundedSender<Message>>>) -> Self {
        Self {
            tx_channels: local_channel_tx,
        }
    }
}

#[async_trait]
impl ReceiveProxy for TokioChannelReceiveProxy {
    async fn recv(&self, _source: u32) -> Result<Message> {
        if let Some(msg) = self.rx_channel.lock().await.recv().await {
            Ok(msg)
        } else {
            Err("Local channel closed".into())
        }
    }
}

impl TokioChannelReceiveProxy {
    pub fn new(local_channel_rx: UnboundedReceiver<Message>) -> Self {
        Self {
            rx_channel: Mutex::new(local_channel_rx),
        }
    }
}

// BROADCAST PROXIES

pub struct TokioChannelBroadcastProxy {
    broadcast_sender: Box<dyn BroadcastSendProxy>,
    broadcast_receiver: Box<dyn BroadcastReceiveProxy>,
}

pub struct TokioChannelBroadcastSendProxy {
    worker_id: u32,
    broadcast_channel_tx: Sender<Message>,
}

pub struct TokioChannelBroadcastReceiveProxy {
    worker_id: u32,
    broadcast_channel_rx: Mutex<Receiver<Message>>,
}

impl BroadcastProxy for TokioChannelBroadcastProxy {}

impl TokioChannelBroadcastProxy {
    pub fn new(worker_id: u32, broadcast_channel_tx: Sender<Message>) -> Self {
        Self {
            broadcast_receiver: Box::new(TokioChannelBroadcastReceiveProxy::new(
                worker_id,
                broadcast_channel_tx.subscribe(),
            )),
            broadcast_sender: Box::new(TokioChannelBroadcastSendProxy::new(
                worker_id,
                broadcast_channel_tx,
            )),
        }
    }
}

#[async_trait]
impl BroadcastSendProxy for TokioChannelBroadcastProxy {
    async fn broadcast_send(&self, msg: Message) -> Result<()> {
        self.broadcast_sender.broadcast_send(msg).await
    }
}

#[async_trait]
impl BroadcastReceiveProxy for TokioChannelBroadcastProxy {
    async fn broadcast_recv(&self) -> Result<Message> {
        self.broadcast_receiver.broadcast_recv().await
    }
}

#[async_trait]
impl BroadcastSendProxy for TokioChannelBroadcastSendProxy {
    async fn broadcast_send(&self, msg: Message) -> Result<()> {
        log::debug!("[worker {}] Send broadcast local channel", self.worker_id,);
        self.broadcast_channel_tx.send(msg.clone())?;
        Ok(())
    }
}

impl TokioChannelBroadcastSendProxy {
    pub fn new(worker_id: u32, broadcast_channel_tx: Sender<Message>) -> Self {
        Self {
            broadcast_channel_tx,
            worker_id,
        }
    }
}

#[async_trait]
impl BroadcastReceiveProxy for TokioChannelBroadcastReceiveProxy {
    async fn broadcast_recv(&self) -> Result<Message> {
        log::debug!(
            "[worker {}] Receive broadcast local channel",
            self.worker_id
        );
        match self.broadcast_channel_rx.lock().await.recv().await {
            Ok(msg) => Ok(msg),
            Err(e) => Err(e.into()),
        }
    }
}

impl TokioChannelBroadcastReceiveProxy {
    pub fn new(worker_id: u32, broadcast_channel_rx: Receiver<Message>) -> Self {
        Self {
            broadcast_channel_rx: Mutex::new(broadcast_channel_rx),
            worker_id,
        }
    }
}
