use crate::middleware::BurstMiddleware;
use crate::types::Result;
use crate::BurstInfo;
use bytes::Bytes;
use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot},
};

// https://ryhl.io/blog/actors-with-tokio/

struct MiddlewareActor<T> {
    receiver: mpsc::Receiver<ActorMessage<T>>,
    middleware: BurstMiddleware<T>,
}

#[derive(Debug)]
enum ActorMessage<T> {
    SendMessage {
        payload: T,
        worker_dest: u32,
        respond_to: oneshot::Sender<Result<()>>,
    },
    ReceiveMessage {
        from: u32,
        respond_to: oneshot::Sender<Result<T>>,
    },
    Broadcast {
        payload: Option<T>,
        root: u32,
        respond_to: oneshot::Sender<Result<T>>,
    },
    Scatter {
        payloads: Option<Vec<T>>,
        root: u32,
        respond_to: oneshot::Sender<Result<T>>,
    },
    Gather {
        payload: T,
        root: u32,
        respond_to: oneshot::Sender<Result<Option<Vec<T>>>>,
    },
    AllToAll {
        payload: Vec<T>,
        respond_to: oneshot::Sender<Result<Vec<T>>>,
    },
}

impl<T> MiddlewareActor<T>
where
    T: From<Bytes> + Into<Bytes>,
{
    fn new(receiver: mpsc::Receiver<ActorMessage<T>>, middleware: BurstMiddleware<T>) -> Self {
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

    async fn handle_message(&mut self, msg: ActorMessage<T>) {
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
                match self.middleware.recv(from).await {
                    Ok(res) => {
                        respond_to.send(Ok(res.data));
                    }
                    Err(e) => {
                        respond_to.send(Err(e));
                    }
                };
            }
            ActorMessage::Broadcast {
                root,
                payload,
                respond_to,
            } => {
                match self.middleware.broadcast(payload, root).await {
                    Ok(res) => {
                        respond_to.send(Ok(res.data));
                    }
                    Err(e) => {
                        respond_to.send(Err(e));
                    }
                };
            }
            ActorMessage::Scatter {
                payloads,
                root,
                respond_to,
            } => {
                match self.middleware.scatter(payloads, root).await {
                    Ok(res) => {
                        respond_to.send(Ok(res.data));
                    }
                    Err(e) => {
                        respond_to.send(Err(e));
                    }
                };
            }
            ActorMessage::Gather {
                payload,
                root,
                respond_to,
            } => {
                match self.middleware.gather(payload, root).await {
                    Ok(res) => {
                        if let Some(data) = res {
                            let data = data.into_iter().map(|m| m.data).collect::<Vec<T>>();
                            respond_to.send(Ok(Some(data)));
                        } else {
                            respond_to.send(Ok(None));
                        }
                    }
                    Err(e) => {
                        respond_to.send(Err(e));
                    }
                };
            }
            ActorMessage::AllToAll {
                payload,
                respond_to,
            } => {
                match self.middleware.all_to_all(payload).await {
                    Ok(res) => {
                        let data = res.into_iter().map(|m| m.data).collect::<Vec<T>>();
                        respond_to.send(Ok(data));
                    }
                    Err(e) => {
                        respond_to.send(Err(e));
                    }
                };
            }
        }
    }
}

#[derive(Clone)]
pub struct MiddlewareActorHandle<T> {
    pub info: BurstInfo,
    sender: mpsc::Sender<ActorMessage<T>>,
}

impl<T> MiddlewareActorHandle<T>
where
    T: From<Bytes> + Into<Bytes> + Send + Sync + 'static,
{
    pub fn new(middleware: BurstMiddleware<T>, tokio_runtime: &Handle) -> Self {
        let (sender, receiver) = mpsc::channel(1);
        let info = middleware.info().clone();

        let mut actor = MiddlewareActor::new(receiver, middleware);
        tokio_runtime.spawn(async move { actor.run().await });

        Self { sender, info }
    }

    pub fn send(&self, dest: u32, data: T) -> Result<()>
    where
        T: Into<Bytes>,
    {
        let (send, recv) = oneshot::channel();

        self.sender.blocking_send(ActorMessage::SendMessage {
            payload: data,
            worker_dest: dest,
            respond_to: send,
        })?;

        recv.blocking_recv()?
    }

    pub fn recv(&self, from: u32) -> Result<T> {
        let (send, recv) = oneshot::channel();

        self.sender.blocking_send(ActorMessage::ReceiveMessage {
            from,
            respond_to: send,
        })?;

        recv.blocking_recv()?
    }

    pub fn broadcast(&self, data: Option<T>, root: u32) -> Result<T> {
        let (send, recv) = oneshot::channel();

        self.sender.blocking_send(ActorMessage::Broadcast {
            payload: data,
            root,
            respond_to: send,
        })?;

        recv.blocking_recv()?
    }

    pub fn gather(&self, data: T, root: u32) -> Result<Option<Vec<T>>> {
        let (send, recv) = oneshot::channel();

        self.sender.blocking_send(ActorMessage::Gather {
            payload: data,
            root,
            respond_to: send,
        })?;

        recv.blocking_recv()?
    }

    pub fn scatter(&self, data: Option<Vec<T>>, root: u32) -> Result<T> {
        let (send, recv) = oneshot::channel();

        self.sender.blocking_send(ActorMessage::Scatter {
            payloads: data,
            root,
            respond_to: send,
        })?;

        recv.blocking_recv()?
    }

    pub fn all_to_all(&self, data: Vec<T>) -> Result<Vec<T>> {
        let (send, recv) = oneshot::channel();

        self.sender.blocking_send(ActorMessage::AllToAll {
            payload: data,
            respond_to: send,
        })?;

        recv.blocking_recv()?
    }
}
