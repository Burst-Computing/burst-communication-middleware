use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{
    broadcast::{Receiver, Sender},
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    Mutex,
};

use crate::{
    impl_chainable_setter, BurstOptions, LocalBroadcastProxy, LocalBroadcastReceiveProxy,
    LocalBroadcastSendProxy, LocalMessage, LocalReceiveProxy, LocalSendProxy,
    LocalSendReceiveProxy, Result, SendReceiveLocalFactory,
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
impl<T> SendReceiveLocalFactory<TokioChannelOptions, T> for TokioChannelImpl
where
    T: From<Bytes> + Into<Bytes> + Send + Sync + Clone + 'static,
{
    async fn create_local_proxies(
        burst_options: Arc<BurstOptions>,
        channel_options: TokioChannelOptions,
    ) -> Result<
        HashMap<
            u32,
            (
                Box<dyn LocalSendReceiveProxy<T>>,
                Box<dyn LocalBroadcastProxy<T>>,
            ),
        >,
    > {
        let current_group = burst_options
            .group_ranges
            .get(&burst_options.group_id)
            .unwrap();

        let channel_options = Arc::new(channel_options);

        // create local channels
        let mut tx_channels = HashMap::new();
        let mut rx_channels = HashMap::new();

        for worker_id in current_group {
            let (tx, rx) = mpsc::unbounded_channel::<LocalMessage<_>>();
            tx_channels.insert(*worker_id, tx);
            rx_channels.insert(*worker_id, rx);
        }

        // tx_channels is shared across all proxies
        let tx_channels = Arc::new(tx_channels);

        // create broadcast channel for this group
        let (broadcast_tx, _) = tokio::sync::broadcast::channel::<LocalMessage<_>>(
            channel_options.broadcast_channel_size,
        );

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
                        Box::new(proxy) as Box<dyn LocalSendReceiveProxy<T>>,
                        Box::new(broadcast_proxy) as Box<dyn LocalBroadcastProxy<T>>,
                    ),
                );
            });

        Ok(proxies)
    }
}

// DIRECT PROXIES

pub struct TokioChannelProxy<T> {
    worker_id: u32,
    sender: Box<dyn LocalSendProxy<T>>,
    receiver: Box<dyn LocalReceiveProxy<T>>,
}

pub struct TokioChannelSendProxy<T> {
    tx_channels: Arc<HashMap<u32, UnboundedSender<LocalMessage<T>>>>,
}

pub struct TokioChannelReceiveProxy<T> {
    rx_channel: Mutex<UnboundedReceiver<LocalMessage<T>>>,
}

impl<T> LocalSendReceiveProxy<T> for TokioChannelProxy<T> where T: From<Bytes> + Into<Bytes> + Send {}

#[async_trait]
impl<T> LocalSendProxy<T> for TokioChannelProxy<T>
where
    T: From<Bytes> + Into<Bytes> + Send,
{
    async fn local_send(&self, dest: u32, msg: LocalMessage<T>) -> Result<()> {
        self.sender.local_send(dest, msg).await
    }
}

#[async_trait]
impl<T> LocalReceiveProxy<T> for TokioChannelProxy<T>
where
    T: From<Bytes> + Into<Bytes>,
{
    async fn local_recv(&self, source: u32) -> Result<LocalMessage<T>> {
        self.receiver.local_recv(source).await
    }
}

impl<T> TokioChannelProxy<T>
where
    T: From<Bytes> + Into<Bytes> + Send + Sync + Clone + 'static,
{
    pub fn new(
        worker_id: u32,
        tx_channels: Arc<HashMap<u32, UnboundedSender<LocalMessage<T>>>>,
        rx_channel: UnboundedReceiver<LocalMessage<T>>,
    ) -> Self {
        Self {
            worker_id,
            sender: Box::new(TokioChannelSendProxy::new(tx_channels)),
            receiver: Box::new(TokioChannelReceiveProxy::new(rx_channel)),
        }
    }
}

#[async_trait]
impl<T> LocalSendProxy<T> for TokioChannelSendProxy<T>
where
    T: From<Bytes> + Into<Bytes> + Send + Sync + Clone + 'static,
{
    async fn local_send(&self, dest: u32, msg: LocalMessage<T>) -> Result<()> {
        if let Some(tx) = self.tx_channels.get(&dest) {
            tx.send(msg.clone())?;
        } else {
            return Err("Destination not found".into());
        }
        Ok(())
    }
}

impl<T> TokioChannelSendProxy<T>
where
    T: From<Bytes> + Into<Bytes>,
{
    pub fn new(local_channel_tx: Arc<HashMap<u32, UnboundedSender<LocalMessage<T>>>>) -> Self {
        Self {
            tx_channels: local_channel_tx,
        }
    }
}

#[async_trait]
impl<T> LocalReceiveProxy<T> for TokioChannelReceiveProxy<T>
where
    T: From<Bytes> + Into<Bytes> + Send,
{
    async fn local_recv(&self, _source: u32) -> Result<LocalMessage<T>> {
        if let Some(msg) = self.rx_channel.lock().await.recv().await {
            Ok(msg)
        } else {
            Err("Local channel closed".into())
        }
    }
}

impl<T> TokioChannelReceiveProxy<T>
where
    T: From<Bytes> + Into<Bytes>,
{
    pub fn new(local_channel_rx: UnboundedReceiver<LocalMessage<T>>) -> Self {
        Self {
            rx_channel: Mutex::new(local_channel_rx),
        }
    }
}

// BROADCAST PROXIES

pub struct TokioChannelBroadcastProxy<T> {
    broadcast_sender: Box<dyn LocalBroadcastSendProxy<T>>,
    broadcast_receiver: Box<dyn LocalBroadcastReceiveProxy<T>>,
}

pub struct TokioChannelBroadcastSendProxy<T> {
    worker_id: u32,
    broadcast_channel_tx: Sender<LocalMessage<T>>,
}

pub struct TokioChannelBroadcastReceiveProxy<T> {
    worker_id: u32,
    broadcast_channel_rx: Mutex<Receiver<LocalMessage<T>>>,
}

impl<T> LocalBroadcastProxy<T> for TokioChannelBroadcastProxy<T> where
    T: From<Bytes> + Into<Bytes> + Send
{
}

impl<T> TokioChannelBroadcastProxy<T>
where
    T: From<Bytes> + Into<Bytes> + Send + Sync + Clone + 'static,
{
    pub fn new(worker_id: u32, broadcast_channel_tx: Sender<LocalMessage<T>>) -> Self {
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
impl<T> LocalBroadcastSendProxy<T> for TokioChannelBroadcastProxy<T>
where
    T: From<Bytes> + Into<Bytes> + Send,
{
    async fn local_broadcast_send(&self, msg: LocalMessage<T>) -> Result<()> {
        self.broadcast_sender.local_broadcast_send(msg).await
    }
}

#[async_trait]
impl<T> LocalBroadcastReceiveProxy<T> for TokioChannelBroadcastProxy<T>
where
    T: From<Bytes> + Into<Bytes> + Send,
{
    async fn local_broadcast_recv(&self) -> Result<LocalMessage<T>> {
        self.broadcast_receiver.local_broadcast_recv().await
    }
}

#[async_trait]
impl<T> LocalBroadcastSendProxy<T> for TokioChannelBroadcastSendProxy<T>
where
    T: From<Bytes> + Into<Bytes> + Send + Sync + Clone + 'static,
{
    async fn local_broadcast_send(&self, msg: LocalMessage<T>) -> Result<()> {
        log::debug!("[worker {}] Send broadcast local channel", self.worker_id,);
        self.broadcast_channel_tx.send(msg.clone())?;
        Ok(())
    }
}

impl<T> TokioChannelBroadcastSendProxy<T>
where
    T: From<Bytes> + Into<Bytes>,
{
    pub fn new(worker_id: u32, broadcast_channel_tx: Sender<LocalMessage<T>>) -> Self {
        Self {
            worker_id,
            broadcast_channel_tx,
        }
    }
}

#[async_trait]
impl<T> LocalBroadcastReceiveProxy<T> for TokioChannelBroadcastReceiveProxy<T>
where
    T: From<Bytes> + Into<Bytes> + Send + Clone,
{
    async fn local_broadcast_recv(&self) -> Result<LocalMessage<T>> {
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

impl<T> TokioChannelBroadcastReceiveProxy<T>
where
    T: From<Bytes> + Into<Bytes>,
{
    pub fn new(worker_id: u32, broadcast_channel_rx: Receiver<LocalMessage<T>>) -> Self {
        Self {
            broadcast_channel_rx: Mutex::new(broadcast_channel_rx),
            worker_id,
        }
    }
}
