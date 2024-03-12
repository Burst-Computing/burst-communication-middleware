use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;

use burst_message_relay::client::{client::Client, connection_pool::ConnectionPool};

use crate::{
    impl_chainable_setter, BurstOptions, CollectiveType, Error, RemoteBroadcastProxy,
    RemoteBroadcastReceiveProxy, RemoteBroadcastSendProxy, RemoteMessage, RemoteReceiveProxy,
    RemoteSendProxy, RemoteSendReceiveFactory, RemoteSendReceiveProxy, Result,
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
impl RemoteSendReceiveFactory<BurstMessageRelayOptions> for BurstMessageRelayImpl {
    async fn create_remote_proxies(
        burst_options: Arc<BurstOptions>,
        server_options: BurstMessageRelayOptions,
    ) -> Result<
        HashMap<
            u32,
            (
                Box<dyn RemoteSendReceiveProxy>,
                Box<dyn RemoteBroadcastProxy>,
            ),
        >,
    > {
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
                    Box::new(proxy) as Box<dyn RemoteSendReceiveProxy>,
                    Box::new(broadcast_proxy) as Box<dyn RemoteBroadcastProxy>,
                ),
            );
        });

        Ok(proxies)
    }
}

// DIRECT PROXIES

pub struct StreamServerProxy {
    receiver: Box<dyn RemoteReceiveProxy>,
    sender: Box<dyn RemoteSendProxy>,
    worker_id: u32,
}

pub struct StreamServerSendProxy {
    connection_pool: Arc<ConnectionPool>,
}

pub struct StreamServerReceiveProxy {
    worker_id: u32,
    connection_pool: Arc<ConnectionPool>,
}

impl RemoteSendReceiveProxy for StreamServerProxy {}

#[async_trait]
impl RemoteSendProxy for StreamServerProxy {
    async fn remote_send(&self, dest: u32, msg: RemoteMessage) -> Result<()> {
        self.sender.remote_send(dest, msg).await
    }
}

#[async_trait]
impl RemoteReceiveProxy for StreamServerProxy {
    async fn remote_recv(&self, source: u32) -> Result<RemoteMessage> {
        self.receiver.remote_recv(source).await
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
impl RemoteSendProxy for StreamServerSendProxy {
    async fn remote_send(&self, dest: u32, msg: RemoteMessage) -> Result<()> {
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
impl RemoteReceiveProxy for StreamServerReceiveProxy {
    async fn remote_recv(&self, _source: u32) -> Result<RemoteMessage> {
        let mut client = Client::new(self.connection_pool.clone());
        let data = client.recv(self.worker_id).await;
        Ok(RemoteMessage::from(data))
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
    broadcast_sender: Box<dyn RemoteBroadcastSendProxy>,
    broadcast_receiver: Box<dyn RemoteBroadcastReceiveProxy>,
}

pub struct StreamServerBroadcastSendProxy {
    dest_groups: Vec<String>,
    connection_pool: Arc<ConnectionPool>,
}

pub struct StreamServerBroadcastReceiveProxy {
    connection_pool: Arc<ConnectionPool>,
    group_id: String,
}

impl RemoteBroadcastProxy for StreamServerBroadcastProxy {}

#[async_trait]
impl RemoteBroadcastSendProxy for StreamServerBroadcastProxy {
    async fn remote_broadcast_send(&self, msg: RemoteMessage) -> Result<()> {
        self.broadcast_sender.broadcast_send(msg).await
    }
}

#[async_trait]
impl RemoteBroadcastReceiveProxy for StreamServerBroadcastProxy {
    async fn remote_broadcast_recv(&self) -> Result<RemoteMessage> {
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
impl RemoteBroadcastSendProxy for StreamServerBroadcastSendProxy {
    async fn remote_broadcast_send(&self, msg: RemoteMessage) -> Result<()> {
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
impl RemoteBroadcastReceiveProxy for StreamServerBroadcastReceiveProxy {
    async fn remote_broadcast_recv(&self) -> Result<RemoteMessage> {
        let mut client = Client::new(self.connection_pool.clone());
        let data = client.broadcast(&self.group_id).await;
        Ok(RemoteMessage::from(data))
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
