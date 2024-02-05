use std::collections::HashMap;



use crate::{
    chunk_store::{ChunkedMessageBody, VecChunkedMessageBody},
    types::{CollectiveType, Message},
};

type WorkerCollectiveCounter = (u32, CollectiveType, u32);

#[derive(Clone)]
pub struct MessageStoreChunked {
    messages: HashMap<WorkerCollectiveCounter, VecChunkedMessageBody>,
}

impl MessageStoreChunked {
    /// Creates a new [`MessageStoreChunked`] instance and initializes it with the given sender IDs
    /// and collective types.
    ///
    /// # Arguments
    ///
    /// * `ids` - An [`Iterator`] over the [`Message::sender_id`] values to be used to initialize
    ///          the message store.
    /// * `collectives` - A slice containing the [`CollectiveType`] values to be used to initialize
    ///
    /// # Returns
    ///
    /// A new [`MessageStoreChunked`] instance.
    pub fn new(_ids: impl Iterator<Item = u32>, _collectives: &[CollectiveType]) -> Self {
        MessageStoreChunked {
            messages: HashMap::new(),
        }
    }

    /// Inserts a message chunk into the message store.
    ///
    /// The message is inserted into the store based on the [`Message::sender_id`],
    /// [`Message::collective`], and [`Message::counter`] fields.
    ///
    /// This method is thread-safe.
    ///
    /// # Arguments
    ///
    /// * `header` - The message header.
    /// * `data` - The message chunk data.
    pub fn insert(&mut self, msg: Message) {
        let chunk_id = msg.chunk_id;

        if !self
            .messages
            .contains_key(&(msg.sender_id, msg.collective, msg.counter))
        {
            let mut message_body = VecChunkedMessageBody::new(msg.num_chunks);
            message_body.insert(chunk_id, msg.data);
            self.messages
                .insert((msg.sender_id, msg.collective, msg.counter), message_body);
        } else {
            let message_body = self
                .messages
                .get_mut(&(msg.sender_id, msg.collective, msg.counter))
                .unwrap();
            message_body.insert(chunk_id, msg.data);
        }
    }

    /// Retrieves and removes the complete message that matches the given criteria
    /// ([`Message::sender_id`], [`Message::collective`], and [`Message::counter`]]).
    ///
    /// This method is thread-safe.
    ///
    /// # Arguments
    ///
    /// * `sender_id` - The sender ID of the message to retrieve.
    /// * `collective` - The collective type of the message to retrieve.
    /// * `counter` - The counter value of the message to retrieve.
    ///
    /// # Returns
    ///
    /// The retrieved message, if any.
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
            return Some(Message {
                sender_id: *sender_id,
                chunk_id: 0,
                num_chunks: 1,
                counter: *counter,
                collective: *collective,
                data: body,
            });
        } else {
            return None;
        }
    }
}
