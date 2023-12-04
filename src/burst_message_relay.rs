use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;

use burst_message_relay::{client::client::Client, config::ClientConfig};

use tokio::sync::Mutex;

use crate::{
    impl_chainable_setter, BroadcastSendProxy, BurstOptions, CollectiveType, Message, ReceiveProxy,
    Result, SendProxy, SendReceiveFactory, SendReceiveProxy,
};

#[derive(Clone)]
pub struct BurstMessageRelayOptions {
    pub server_uri: String,
    pub config: ClientConfig,
}

impl BurstMessageRelayOptions {
    pub fn new(server_uri: String) -> Self {
        Self {
            server_uri,
            ..Default::default()
        }
    }

    impl_chainable_setter! {
        config, ClientConfig
    }

    pub fn build(&self) -> Self {
        self.clone()
    }
}

impl Default for BurstMessageRelayOptions {
    fn default() -> Self {
        Self {
            server_uri: "localhost:8000".into(),
            config: ClientConfig::default(),
        }
    }
}

pub struct BurstMessageRelayImpl;

#[async_trait]
impl SendReceiveFactory<BurstMessageRelayOptions> for BurstMessageRelayImpl {
    async fn create_proxies(
        burst_options: Arc<BurstOptions>,
        server_options: BurstMessageRelayOptions,
        broadcast_proxy: Box<dyn BroadcastSendProxy>,
    ) -> Result<(
        HashMap<u32, Box<dyn SendReceiveProxy>>,
        Box<dyn BroadcastSendProxy>,
    )> {
        let server_options = Arc::new(server_options);
        let mut client = Client::new(&server_options.server_uri, ClientConfig::default());
        client.connect().await;

        client
            .init_queues(
                &(0..burst_options.burst_size)
                    .into_iter()
                    .collect::<Vec<_>>(),
            )
            .await;
        client.close().await;

        let client = Arc::new(client);

        let current_group = burst_options
            .group_ranges
            .get(&burst_options.group_id)
            .unwrap();

        let mut hmap = HashMap::new();

        futures::future::try_join_all(current_group.iter().map(|worker_id| {
            let c = client.clone();
            let o = server_options.clone();
            let b = burst_options.clone();
            async move { StreamServerProxy::new(o, b, *worker_id).await }
        }))
        .await?
        .into_iter()
        .for_each(|proxy| {
            hmap.insert(
                proxy.worker_id,
                Box::new(proxy) as Box<dyn SendReceiveProxy>,
            );
        });

        // spawn task to receive broadcast messages and send them to the broadcast proxy
        // let broadcast_channel = client.create_channel().await.unwrap();
        // tokio::spawn(async move {
        //     loop {
        //         let data = broadcast_channel
        //             .broadcast(burst_options.group_id)
        //             .await
        //             .unwrap();
        //         let msg = Message::from(data);
        //         broadcast_proxy.broadcast_send(&msg).await.unwrap();
        //     }
        // });

        Ok((
            hmap,
            Box::new(StreamServerBroadcastProxy::new(client, server_options)),
        ))
    }
}

pub struct StreamServerProxy {
    server_options: Arc<BurstMessageRelayOptions>,
    burst_options: Arc<BurstOptions>,
    worker_id: u32,
    receiver: Box<dyn ReceiveProxy>,
    sender: Box<dyn SendProxy>,
}

pub struct StreamServerSendProxy {
    client: Mutex<Client>,
    server_options: Arc<BurstMessageRelayOptions>,
    burst_options: Arc<BurstOptions>,
}

pub struct StreamServerReceiveProxy {
    client: Mutex<Client>,
    options: Arc<BurstMessageRelayOptions>,
    worker_id: u32,
}

pub struct StreamServerBroadcastProxy {
    client: Arc<Client>,
    server_options: Arc<BurstMessageRelayOptions>,
}

impl SendReceiveProxy for StreamServerProxy {}

#[async_trait]
impl SendProxy for StreamServerProxy {
    async fn send(&self, dest: u32, msg: &Message) -> Result<()> {
        self.sender.send(dest, msg).await
    }
}

#[async_trait]
impl ReceiveProxy for StreamServerProxy {
    async fn recv(&self) -> Result<Message> {
        self.receiver.recv().await
    }
}

impl StreamServerProxy {
    pub async fn new(
        server_options: Arc<BurstMessageRelayOptions>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
    ) -> Result<Self> {
        Ok(Self {
            worker_id,
            sender: Box::new(
                StreamServerSendProxy::new(server_options.clone(), burst_options.clone()).await?,
            ),
            receiver: Box::new(
                StreamServerReceiveProxy::new(
                    server_options.clone(),
                    burst_options.clone(),
                    worker_id,
                )
                .await?,
            ),
            server_options,
            burst_options,
        })
    }
}

#[async_trait]
impl SendProxy for StreamServerSendProxy {
    async fn send(&self, dest: u32, msg: &Message) -> Result<()> {
        if msg.collective == CollectiveType::Broadcast {
            Err("Cannot send broadcast message to a single destination".into())
        } else {
            let data: [&[u8]; 2] = msg.into();
            self.client.lock().await.send_refs(dest, &data).await;
            Ok(())
        }
    }
}

impl StreamServerSendProxy {
    pub async fn new(
        server_options: Arc<BurstMessageRelayOptions>,
        burst_options: Arc<BurstOptions>,
    ) -> Result<Self> {
        let mut client = Client::new(&server_options.server_uri, server_options.config.clone());
        client.connect().await;
        Ok(Self {
            client: Mutex::new(client),
            server_options,
            burst_options,
        })
    }
}

#[async_trait]
impl ReceiveProxy for StreamServerReceiveProxy {
    async fn recv(&self) -> Result<Message> {
        let data = self.client.lock().await.recv(self.worker_id).await;
        let msg = Message::from(data);
        Ok(msg)
    }
}

impl StreamServerReceiveProxy {
    pub async fn new(
        server_options: Arc<BurstMessageRelayOptions>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
    ) -> Result<Self> {
        let mut client = Client::new(&server_options.server_uri, server_options.config.clone());
        client.connect().await;
        Ok(Self {
            client: Mutex::new(client),
            options: server_options,
            worker_id,
        })
    }
}

#[async_trait]
impl BroadcastSendProxy for StreamServerBroadcastProxy {
    async fn broadcast_send(&self, msg: &Message) -> Result<()> {
        unimplemented!()
        // if msg.collective != CollectiveType::Broadcast {
        //     Err("Cannot send non-broadcast message to broadcast".into())
        // } else {
        //     let data = msg.into();
        //     self.client.lock().await.broadcast_root(data).await;
        //     Ok(())
        // }
    }
}

impl StreamServerBroadcastProxy {
    pub fn new(client: Arc<Client>, server_options: Arc<BurstMessageRelayOptions>) -> Self {
        Self {
            client,
            server_options,
        }
    }
}
