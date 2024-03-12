use crate::{
    chunk_store::BytesMutChunkedMessageBody,
    types::{CollectiveType, LocalMessage, RemoteMessage},
    MessageMetadata,
};
use std::collections::hash_map;
use std::collections::HashMap;

type MessageKey = (u32, CollectiveType, u32); // (sender_id, collective, counter)

pub struct RemoteMessageBuffer {
    buffer: HashMap<MessageKey, BytesMutChunkedMessageBody>,
    chunk_size: usize,
}

impl RemoteMessageBuffer {
    pub fn new(chunk_size: usize) -> Self {
        RemoteMessageBuffer {
            buffer: HashMap::new(),
            chunk_size,
        }
    }

    pub fn insert(&mut self, msg: RemoteMessage) {
        let chunk_id = msg.chunk_id;

        if let hash_map::Entry::Vacant(e) =
            self.buffer
                .entry((msg.sender_id, msg.collective, msg.counter))
        {
            let mut message_body = BytesMutChunkedMessageBody::new(msg.num_chunks, self.chunk_size);
            message_body.insert(chunk_id, msg.data);
            e.insert(message_body);
        } else {
            let message_body = self
                .buffer
                .get_mut(&(msg.sender_id, msg.collective, msg.counter))
                .unwrap();
            message_body.insert(chunk_id, msg.data);
        }
    }

    pub fn get(
        &mut self,
        sender_id: &u32,
        collective: &CollectiveType,
        counter: &u32,
    ) -> Option<RemoteMessage> {
        let key = (*sender_id, *collective, *counter);
        let is_complete = match self.buffer.get(&key) {
            Some(chunk_store) => chunk_store.is_complete(),
            None => false,
        };

        if is_complete {
            let chunk_store = self.buffer.remove(&key).unwrap();
            let body = chunk_store.get_complete_body();
            Some(RemoteMessage {
                metadata: MessageMetadata {
                    sender_id: *sender_id,
                    chunk_id: 0,
                    num_chunks: 1,
                    counter: *counter,
                    collective: *collective,
                },
                data: body,
            })
        } else {
            None
        }
    }

    pub fn num_chunks_stored(
        &self,
        sender_id: &u32,
        collective: &CollectiveType,
        counter: &u32,
    ) -> Option<u32> {
        self.buffer
            .get(&(*sender_id, *collective, *counter))
            .map(|chunk_store| chunk_store.get_num_chunks_stored())
    }
}

pub struct LocalMessageBuffer<T> {
    buffer: HashMap<MessageKey, LocalMessage<T>>,
}

impl<T> LocalMessageBuffer<T> {
    pub fn new() -> Self {
        LocalMessageBuffer {
            buffer: HashMap::new(),
        }
    }

    pub fn insert(&mut self, msg: LocalMessage<T>) {
        self.buffer
            .insert((msg.sender_id, msg.collective, msg.counter), msg);
    }

    pub fn get(
        &mut self,
        sender_id: &u32,
        collective: &CollectiveType,
        counter: &u32,
    ) -> Option<LocalMessage<T>> {
        self.buffer.remove((sender_id, collective, counter))
    }
}
