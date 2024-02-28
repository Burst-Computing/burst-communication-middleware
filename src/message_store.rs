use crate::{
    chunk_store::BytesMutChunkedMessageBody,
    types::{CollectiveType, Message},
};
use std::collections::hash_map;
use std::collections::HashMap;

type WorkerCollectiveCounter = (u32, CollectiveType, u32);

pub struct MessageStore {
    messages: HashMap<WorkerCollectiveCounter, BytesMutChunkedMessageBody>,
    chunk_size: usize,
}

impl MessageStore {
    pub fn new(chunk_size: usize) -> Self {
        MessageStore {
            messages: HashMap::new(),
            chunk_size,
        }
    }

    pub fn insert(&mut self, msg: Message) {
        let chunk_id = msg.chunk_id;

        if let hash_map::Entry::Vacant(e) =
            self.messages
                .entry((msg.sender_id, msg.collective, msg.counter))
        {
            let mut message_body = BytesMutChunkedMessageBody::new(msg.num_chunks, self.chunk_size);
            message_body.insert(chunk_id, msg.data);
            e.insert(message_body);
        } else {
            let message_body = self
                .messages
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
    ) -> Option<Message> {
        let key = (*sender_id, *collective, *counter);
        let is_complete = match self.messages.get(&key) {
            Some(chunk_store) => chunk_store.is_complete(),
            None => false,
        };

        if is_complete {
            let chunk_store = self.messages.remove(&key).unwrap();
            let body = chunk_store.get_complete_body();
            Some(Message {
                sender_id: *sender_id,
                chunk_id: 0,
                num_chunks: 1,
                counter: *counter,
                collective: *collective,
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
        self.messages
            .get(&(*sender_id, *collective, *counter))
            .map(|chunk_store| chunk_store.get_num_chunks_stored())
    }
}
