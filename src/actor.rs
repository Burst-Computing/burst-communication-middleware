use crate::middleware::BurstMiddleware;
use crate::types::{Message, Result};
use crate::BurstInfo;
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

// https://ryhl.io/blog/actors-with-tokio/

struct MiddlewareActor {
    receiver: mpsc::Receiver<ActorMessage>,
    middleware: BurstMiddleware,
}

#[derive(Debug)]
enum ActorMessage {
    SendMessage {
        payload: Bytes,
        worker_dest: u32,
        respond_to: oneshot::Sender<Result<()>>,
    },
    ReceiveMessage {
        from: u32,
        respond_to: oneshot::Sender<Result<Message>>,
    },
    Broadcast {
        payload: Option<Bytes>,
        root: u32,
        respond_to: oneshot::Sender<Result<Message>>,
    },
    Scatter {
        payloads: Option<Vec<Bytes>>,
        root: u32,
        respond_to: oneshot::Sender<Result<Message>>,
    },
    Gather {
        payload: Bytes,
        root: u32,
        respond_to: oneshot::Sender<Result<Option<Vec<Message>>>>,
    },
    AllToAll {
        payload: Vec<Bytes>,
        respond_to: oneshot::Sender<Result<Vec<Message>>>,
    },
}

impl MiddlewareActor {
    fn new(receiver: mpsc::Receiver<ActorMessage>, middleware: BurstMiddleware) -> Self {
        MiddlewareActor {
            receiver,
            middleware,
        }
    }

    async fn run(&mut self) {
        log::debug!(
            "[Middleware Actor] Running for {}",
            self.middleware.info().worker_id
        );
        while let Some(msg) = self.receiver.recv().await {
            // log::debug!("[Middleware Actor] Handling message: {:?}", msg);
            self.handle_message(msg).await;
        }
    }

    async fn handle_message(&mut self, msg: ActorMessage) {
        match msg {
            ActorMessage::SendMessage {
                payload,
                worker_dest,
                respond_to,
            } => {
                let result = self.middleware.send(worker_dest, payload).await;
                match respond_to.send(result) {
                    Ok(_) => {}
                    Err(_) => {
                        log::error!(
                            "Failed to send response to {}",
                            self.middleware.info().worker_id
                        );
                    }
                };
            }
            ActorMessage::ReceiveMessage { from, respond_to } => {
                let result = self.middleware.recv(from).await;
                match respond_to.send(result) {
                    Ok(_) => {}
                    Err(_) => {
                        log::error!(
                            "Failed to send response to {}",
                            self.middleware.info().worker_id
                        );
                    }
                };
            }
            ActorMessage::Broadcast {
                root,
                payload,
                respond_to,
            } => {
                let result = self.middleware.broadcast(payload, root).await;
                match respond_to.send(result) {
                    Ok(_) => {}
                    Err(_) => {
                        log::error!(
                            "Failed to send response to {}",
                            self.middleware.info().worker_id
                        );
                    }
                };
            }
            ActorMessage::Scatter {
                payloads,
                root,
                respond_to,
            } => {
                let result = self.middleware.scatter(payloads, root).await;
                match respond_to.send(result) {
                    Ok(_) => {}
                    Err(_) => {
                        log::error!(
                            "Failed to send response to {}",
                            self.middleware.info().worker_id
                        );
                    }
                };
            }
            ActorMessage::Gather {
                payload,
                root,
                respond_to,
            } => {
                let result = self.middleware.gather(payload, root).await;
                match respond_to.send(result) {
                    Ok(_) => {}
                    Err(_) => {
                        log::error!(
                            "Failed to send response to {}",
                            self.middleware.info().worker_id
                        );
                    }
                };
            }
            ActorMessage::AllToAll {
                payload,
                respond_to,
            } => {
                let result = self.middleware.all_to_all(payload).await;
                match respond_to.send(result) {
                    Ok(_) => {}
                    Err(_) => {
                        log::error!(
                            "Failed to send response to {}",
                            self.middleware.info().worker_id
                        );
                    }
                };
            }
        }
    }
}

#[derive(Clone)]
pub struct MiddlewareActorHandle {
    pub info: BurstInfo,
    sender: mpsc::Sender<ActorMessage>,
}

impl MiddlewareActorHandle {
    pub fn new(middleware: BurstMiddleware, tokio_runtime: &tokio::runtime::Runtime) -> Self {
        let (sender, receiver) = mpsc::channel(1);
        let info = middleware.info().clone();

        let mut actor = MiddlewareActor::new(receiver, middleware);
        tokio_runtime.spawn(async move { actor.run().await });

        Self { sender, info }
    }

    pub fn send(&self, dest: u32, data: Bytes) -> Result<()> {
        let (send, recv) = oneshot::channel();

        self.sender.blocking_send(ActorMessage::SendMessage {
            payload: data,
            worker_dest: dest,
            respond_to: send,
        })?;

        recv.blocking_recv()?
    }

    pub fn recv(&self, from: u32) -> Result<Message> {
        let (send, recv) = oneshot::channel();

        self.sender.blocking_send(ActorMessage::ReceiveMessage {
            from,
            respond_to: send,
        })?;

        recv.blocking_recv()?
    }

    pub fn broadcast(&self, data: Option<Bytes>, root: u32) -> Result<Message> {
        let (send, recv) = oneshot::channel();

        self.sender.blocking_send(ActorMessage::Broadcast {
            payload: data,
            root,
            respond_to: send,
        })?;

        recv.blocking_recv()?
    }

    pub fn gather(&self, data: Bytes, root: u32) -> Result<Option<Vec<Message>>> {
        let (send, recv) = oneshot::channel();

        self.sender.blocking_send(ActorMessage::Gather {
            payload: data,
            root,
            respond_to: send,
        })?;

        recv.blocking_recv()?
    }

    pub fn scatter(&self, data: Option<Vec<Bytes>>, root: u32) -> Result<Message> {
        let (send, recv) = oneshot::channel();

        self.sender.blocking_send(ActorMessage::Scatter {
            payloads: data,
            root,
            respond_to: send,
        })?;

        recv.blocking_recv()?
    }

    pub fn all_to_all(&self, data: Vec<Bytes>) -> Result<Vec<Message>> {
        let (send, recv) = oneshot::channel();

        self.sender.blocking_send(ActorMessage::AllToAll {
            payload: data,
            respond_to: send,
        })?;

        recv.blocking_recv()?
    }
}
