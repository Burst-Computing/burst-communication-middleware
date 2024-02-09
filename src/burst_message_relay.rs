use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;

use burst_message_relay::{
    client::client::Client, client::connection_pool::ConnectionPool,
};

use crate::{
    impl_chainable_setter, BroadcastSendProxy, BurstOptions, CollectiveType, Message, ReceiveProxy,
    Result, SendProxy, SendReceiveFactory, SendReceiveProxy,
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
        broadcast_proxy: Box<dyn BroadcastSendProxy>,
    ) -> Result<(
        HashMap<u32, Box<dyn SendReceiveProxy>>,
        Box<dyn BroadcastSendProxy>,
    )> {
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
            .await; */

        for group_id in burst_options.group_ranges.keys() {
            client.create_bc_group(group_id, 1).await;
        }

        let mut hmap = HashMap::new();

        futures::future::try_join_all(current_group.iter().map(|worker_id| {
            let o = server_options.clone();
            let conn_pool_clone = connection_pool.clone();
            async move { StreamServerProxy::new(o, conn_pool_clone.clone(), *worker_id).await }
        }))
        .await?
        .into_iter()
        .for_each(|proxy| {
            hmap.insert(
                proxy.worker_id,
                Box::new(proxy) as Box<dyn SendReceiveProxy>,
            );
        });

        let broadcast_proxy_arc = Arc::new(broadcast_proxy);

        //spawn task to receive broadcast messages and send them to the broadcast proxy
        let conn_pool = connection_pool.clone();
        tokio::spawn({
            let b = burst_options.clone();
            async move {

                loop {

                    let mut client = Client::new(conn_pool.clone());

                    let data = client.broadcast(&b.group_id).await;
                    let msg = Message::from(data);

                    if msg.num_chunks == 1 {
                        broadcast_proxy_arc.clone().broadcast_send(msg).await.unwrap();
                    } else {
                        let group_id = b.group_id.clone();
                        let missing_chunks = msg.num_chunks - 1;
                        broadcast_proxy_arc.broadcast_send(msg).await.unwrap();

                        let mut tasks = Vec::new();
                        for _ in 0..missing_chunks {
                            let connection_pool = conn_pool.clone();
                            let bc_proxy_arc = broadcast_proxy_arc.clone();
                            let gp_id = group_id.clone();
                            let task = tokio::spawn(async move {
                                let mut client =
                                    Client::new(connection_pool.clone());
                                let data = client.broadcast(&gp_id).await;
                                let msg = Message::from(data);
                                bc_proxy_arc.broadcast_send(msg).await.unwrap();
                            });
                            tasks.push(task);
                        }

                        futures::future::join_all(tasks).await;
                    }
                }
            }
        });

        Ok((
            hmap,
            Box::new(
                StreamServerBroadcastProxy::new(
                    server_options,
                    connection_pool.clone(),
                    burst_options,
                )
                .await?,
            ),
        ))
    }
}

pub struct StreamServerProxy {
    worker_id: u32,
    receiver: Box<dyn ReceiveProxy>,
    sender: Box<dyn SendProxy>,
}

pub struct StreamServerSendProxy {
    connection_pool: Arc<ConnectionPool>,
}

pub struct StreamServerReceiveProxy {
    worker_id: u32,
    connection_pool: Arc<ConnectionPool>,
}

pub struct StreamServerBroadcastProxy {
    connection_pool: Arc<ConnectionPool>,
    burst_options: Arc<BurstOptions>,
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
    async fn recv(&self) -> Result<Message> {
        self.receiver.recv().await
    }
}

impl StreamServerProxy {
    pub async fn new(
        server_options: Arc<BurstMessageRelayOptions>,
        connection_pool: Arc<ConnectionPool>,
        worker_id: u32,
    ) -> Result<Self> {
        Ok(Self {
            worker_id,
            sender: Box::new(
                StreamServerSendProxy::new(server_options.clone(), connection_pool.clone()).await?,
            ),
            receiver: Box::new(
                StreamServerReceiveProxy::new(
                    server_options.clone(),
                    connection_pool.clone(),
                    worker_id,
                )
                .await?,
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
    pub async fn new(
        _server_options: Arc<BurstMessageRelayOptions>,
        connection_pool: Arc<ConnectionPool>,
    ) -> Result<Self> {
        Ok(Self {
            connection_pool,
        })
    }
}

#[async_trait]
impl ReceiveProxy for StreamServerReceiveProxy {
    async fn recv(&self) -> Result<Message> {
        let mut client = Client::new(self.connection_pool.clone());
        let data = client.recv(self.worker_id).await;

        let msg = Message::from(data);

        Ok(msg)
    }
}

impl StreamServerReceiveProxy {
    pub async fn new(
        _server_options: Arc<BurstMessageRelayOptions>,
        connection_pool: Arc<ConnectionPool>,
        worker_id: u32,
    ) -> Result<Self> {
        Ok(Self {
            worker_id,
            connection_pool,
        })
    }
}

#[async_trait]
impl BroadcastSendProxy for StreamServerBroadcastProxy {
    async fn broadcast_send(&self, msg: Message) -> Result<()> {
        if msg.collective != CollectiveType::Broadcast {
            Err("Cannot send non-broadcast message to broadcast".into())
        } else {

            let mut tasks = Vec::new();

            for dest in self
                .burst_options
                .group_ranges
                .keys()
                .filter(|dest| **dest != self.burst_options.group_id)
            {
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

impl StreamServerBroadcastProxy {
    pub async fn new(
        _server_options: Arc<BurstMessageRelayOptions>,
        connection_pool: Arc<ConnectionPool>,
        burst_options: Arc<BurstOptions>,
    ) -> Result<Self> {
        Ok(Self {
            connection_pool,
            burst_options,
        })
    }
}
