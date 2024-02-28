use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;

use burst_message_relay::client::{client::Client, connection_pool::ConnectionPool};

use crate::{
    impl_chainable_setter, BroadcastProxy, BroadcastReceiveProxy, BroadcastSendProxy, BurstOptions,
    CollectiveType, Error, Message, ReceiveProxy, Result, SendProxy, SendReceiveFactory,
    SendReceiveProxy,
};

#[derive(Clone)]
pub struct BurstMessageRelayOptions {
    pub server_uri: String,
}

impl BurstMessageRelayOptions {
    pub fn new(server_uri: String) -> Self {
        Self {
            server_uri,
            ..Default::default()
        }
    }

    impl_chainable_setter! {
        server_uri, String
    }

    pub fn build(&self) -> Self {
        self.clone()
    }
}

impl Default for BurstMessageRelayOptions {
    fn default() -> Self {
        Self {
            server_uri: "localhost:8000".into(),
        }
    }
}

pub struct BurstMessageRelayImpl;

#[async_trait]
impl SendReceiveFactory<BurstMessageRelayOptions> for BurstMessageRelayImpl {
    async fn create_proxies(
        burst_options: Arc<BurstOptions>,
        server_options: BurstMessageRelayOptions,
    ) -> Result<HashMap<u32, (Box<dyn SendReceiveProxy>, Box<dyn BroadcastProxy>)>> {
        let current_group = burst_options
            .group_ranges
            .get(&burst_options.group_id)
            .unwrap();

        let server_options = Arc::new(server_options);

        let connection_pool = Arc::new(ConnectionPool::new(
            &server_options.server_uri,
            current_group.len() as u32 + 1,
        ));
        connection_pool.initialize_conns().await;

        let mut client = Client::new(connection_pool.clone());

        /* client
        .init_queues(
            &(0..burst_options.burst_size)
                .into_iter()
                .collect::<Vec<_>>(),
        )
        .await;  */

        for group_id in burst_options.group_ranges.keys() {
            client.create_bc_group(group_id, 1).await;
        }

        let mut proxies = HashMap::new();

        futures::future::try_join_all(current_group.iter().map(|worker_id| {
            let cp = connection_pool.clone();
            let bo = burst_options.clone();
            async move {
                let proxy = StreamServerProxy::new(cp.clone(), *worker_id).await?;
                let broadcast_proxy =
                    StreamServerBroadcastProxy::new(bo.clone(), cp.clone()).await?;
                Ok::<_, Error>((proxy, broadcast_proxy))
            }
        }))
        .await?
        .into_iter()
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

pub struct StreamServerProxy {
    receiver: Box<dyn ReceiveProxy>,
    sender: Box<dyn SendProxy>,
    worker_id: u32,
}

pub struct StreamServerSendProxy {
    connection_pool: Arc<ConnectionPool>,
}

pub struct StreamServerReceiveProxy {
    worker_id: u32,
    connection_pool: Arc<ConnectionPool>,
}

impl SendReceiveProxy for StreamServerProxy {}

#[async_trait]
impl SendProxy for StreamServerProxy {
    async fn send(&self, dest: u32, msg: Message) -> Result<()> {
        self.sender.send(dest, msg).await
    }
}

#[async_trait]
impl ReceiveProxy for StreamServerProxy {
    async fn recv(&self, source: u32) -> Result<Message> {
        self.receiver.recv(source).await
    }
}

impl StreamServerProxy {
    pub async fn new(connection_pool: Arc<ConnectionPool>, worker_id: u32) -> Result<Self> {
        Ok(Self {
            worker_id,
            sender: Box::new(StreamServerSendProxy::new(connection_pool.clone()).await?),
            receiver: Box::new(
                StreamServerReceiveProxy::new(connection_pool.clone(), worker_id).await?,
            ),
        })
    }
}

#[async_trait]
impl SendProxy for StreamServerSendProxy {
    async fn send(&self, dest: u32, msg: Message) -> Result<()> {
        if msg.collective == CollectiveType::Broadcast {
            Err("Cannot send broadcast message to a single destination".into())
        } else {
            let data: &[&[u8]; 2] = &(&msg).into();
            let mut client = Client::new(self.connection_pool.clone());
            client.send_refs(dest, data).await;

            Ok(())
        }
    }
}

impl StreamServerSendProxy {
    pub async fn new(connection_pool: Arc<ConnectionPool>) -> Result<Self> {
        Ok(Self { connection_pool })
    }
}

#[async_trait]
impl ReceiveProxy for StreamServerReceiveProxy {
    async fn recv(&self, _source: u32) -> Result<Message> {
        let mut client = Client::new(self.connection_pool.clone());
        let data = client.recv(self.worker_id).await;
        Ok(Message::from(data))
    }
}

impl StreamServerReceiveProxy {
    pub async fn new(connection_pool: Arc<ConnectionPool>, worker_id: u32) -> Result<Self> {
        Ok(Self {
            worker_id,
            connection_pool,
        })
    }
}

// BROADCAST PROXIES

pub struct StreamServerBroadcastProxy {
    broadcast_sender: Box<dyn BroadcastSendProxy>,
    broadcast_receiver: Box<dyn BroadcastReceiveProxy>,
}

pub struct StreamServerBroadcastSendProxy {
    dest_groups: Vec<String>,
    connection_pool: Arc<ConnectionPool>,
}

pub struct StreamServerBroadcastReceiveProxy {
    connection_pool: Arc<ConnectionPool>,
    group_id: String,
}

impl BroadcastProxy for StreamServerBroadcastProxy {}

#[async_trait]
impl BroadcastSendProxy for StreamServerBroadcastProxy {
    async fn broadcast_send(&self, msg: Message) -> Result<()> {
        self.broadcast_sender.broadcast_send(msg).await
    }
}

#[async_trait]
impl BroadcastReceiveProxy for StreamServerBroadcastProxy {
    async fn broadcast_recv(&self) -> Result<Message> {
        self.broadcast_receiver.broadcast_recv().await
    }
}

impl StreamServerBroadcastProxy {
    pub async fn new(
        burst_options: Arc<BurstOptions>,
        connection_pool: Arc<ConnectionPool>,
    ) -> Result<Self> {
        Ok(Self {
            broadcast_sender: Box::new(StreamServerBroadcastSendProxy::new(
                burst_options.clone(),
                connection_pool.clone(),
            )),
            broadcast_receiver: Box::new(StreamServerBroadcastReceiveProxy::new(
                burst_options.clone(),
                connection_pool.clone(),
            )),
        })
    }
}

impl StreamServerBroadcastSendProxy {
    pub fn new(burst_options: Arc<BurstOptions>, connection_pool: Arc<ConnectionPool>) -> Self {
        let dest_groups = burst_options
            .group_ranges
            .keys()
            .filter(|group_id| **group_id != burst_options.group_id)
            .cloned()
            .collect();
        Self {
            dest_groups,
            connection_pool,
        }
    }
}

#[async_trait]
impl BroadcastSendProxy for StreamServerBroadcastSendProxy {
    async fn broadcast_send(&self, msg: Message) -> Result<()> {
        if msg.collective != CollectiveType::Broadcast {
            Err("Cannot send non-broadcast message to broadcast".into())
        } else {
            let mut tasks = Vec::new();

            for dest in &self.dest_groups {
                let mes = msg.clone();
                let str = dest.clone();
                let conn_pool = self.connection_pool.clone();
                let task = tokio::spawn(async move {
                    let mut client = Client::new(conn_pool.clone());
                    let data: [&[u8]; 2] = (&mes).into();
                    client.broadcast_root_refs(&str, &data).await;
                });
                tasks.push(task);
            }
            futures::future::join_all(tasks).await;

            Ok(())
        }
    }
}

impl StreamServerBroadcastReceiveProxy {
    pub fn new(burst_options: Arc<BurstOptions>, connection_pool: Arc<ConnectionPool>) -> Self {
        Self {
            connection_pool,
            group_id: burst_options.group_id.clone(),
        }
    }
}

#[async_trait]
impl BroadcastReceiveProxy for StreamServerBroadcastReceiveProxy {
    async fn broadcast_recv(&self) -> Result<Message> {
        let mut client = Client::new(self.connection_pool.clone());
        let data = client.broadcast(&self.group_id).await;
        Ok(Message::from(data))
    }
}

// impl StreamServerBroadcastProxy {
//     pub async fn new(
//         _server_options: Arc<BurstMessageRelayOptions>,
//         connection_pool: Arc<ConnectionPool>,
//         burst_options: Arc<BurstOptions>,
//     ) -> Result<Self> {
//         Ok(Self {
//             connection_pool,
//             burst_options,
//         })
//     }
// }
