use bytes::Bytes;

use crate::{
    chunk_store::{ChunkStore, VecChunkStore},
    types::{CollectiveType, Message},
};
use std::{
    collections::HashMap,
    sync::{Mutex, RwLock},
};

/// Trait representing a message store.
///
/// This trait is used to define the behavior of a message store, which is responsible for storing
/// and retrieving messages. It is [`Send`] and  [`Sync`], meaning it can be safely sent and shared
/// between multiple threads.
pub trait MessageStore: Send + Sync {
    /// Inserts a message into the message store.
    ///
    /// The message is inserted into the store based on the [`Message::sender_id`],
    /// [`Message::collective`], and [`Message::counter`] fields.
    ///
    /// This method is thread-safe.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be inserted.
    fn insert(&self, msg: Message);

    /// Retrieves and removes all messages that match the given criteria ([`Message::sender_id`],
    /// [`Message::collective`], and [`Message::counter`]]).
    ///
    /// This method is thread-safe.
    ///
    /// # Arguments
    ///
    /// * `sender_id` - The sender ID of the messages to retrieve.
    /// * `collective` - The collective type of the messages to retrieve.
    /// * `counter` - The counter value of the messages to retrieve.
    ///
    /// # Returns
    ///
    /// A [`Vec`] containing the retrieved messages.
    fn get(&self, sender_id: &u32, collective: &CollectiveType, counter: &u32) -> Vec<Message>;

    /// Retrieves and removes all messages that match the given criteria ([`Message::sender_id`] and
    /// [`Message::collective`]).
    ///
    /// This method is thread-safe.
    ///
    /// # Arguments
    ///
    /// * `sender_id` - The sender ID of the messages to retrieve.
    /// * `collective` - The collective type of the messages to retrieve.
    ///
    /// # Returns
    ///
    /// A [`Vec`] containing the retrieved messages.
    fn get_all(&self, sender_id: &u32, collective: &CollectiveType) -> Vec<Message>;

    /// Retrieves and removes any message that matches the given criteria ([`Message::sender_id`],
    /// and [`Message::collective`]).
    ///
    /// This method is thread-safe.
    ///
    /// # Arguments
    ///
    /// * `sender_id` - The sender ID of the messages to retrieve.
    /// * `collective` - The collective type of the messages to retrieve.
    ///
    /// # Returns
    ///
    /// The retrieved message, if any.
    fn get_any(&self, sender_id: &u32, collective: &CollectiveType) -> Option<Message>;
}

/// A [`MessageStore`] implementation that uses a [`HashMap`] and [`Vec`]s to store messages.
#[derive(Debug)]
pub struct MessageStoreHashMap {
    messages: HashMap<u32, HashMap<CollectiveType, RwLock<HashMap<u32, RwLock<Vec<Message>>>>>>,
}

impl MessageStoreHashMap {
    /// Creates a new [`MessageStoreHashMap`] instance and initializes it with the given sender IDs
    /// and collective types.
    ///
    /// # Arguments
    ///
    /// * `ids` - An [`Iterator`] over the [`Message::sender_id`] values to be used to initialize
    ///          the message store.
    /// * `collectives` - A slice containing the [`CollectiveType`] values to be used to initialize
    ///                  the message store.
    ///
    /// # Returns
    ///
    /// A new [`MessageStoreHashMap`] instance.
    pub fn new(ids: impl Iterator<Item = u32>, collectives: &[CollectiveType]) -> Self {
        MessageStoreHashMap {
            messages: ids
                .map(|id| {
                    (
                        id,
                        collectives
                            .iter()
                            .map(|c| (*c, RwLock::new(HashMap::new())))
                            .collect(),
                    )
                })
                .collect(),
        }
    }
}

impl MessageStore for MessageStoreHashMap {
    fn insert(&self, msg: Message) {
        let by_counter = self
            .messages
            .get(&msg.sender_id)
            .unwrap()
            .get(&msg.collective)
            .unwrap();
        let read_by_counter = by_counter.read().unwrap();
        if let Some(vec) = read_by_counter.get(&msg.counter) {
            let mut vec = vec.write().unwrap();
            let i = vec.binary_search_by(|probe| probe.counter.cmp(&msg.counter));
            if let Ok(i) = i {
                vec.insert(i, msg);
            } else {
                vec.push(msg);
            }
        } else {
            drop(read_by_counter);
            by_counter
                .write()
                .unwrap()
                .insert(msg.counter, RwLock::new(vec![msg]));
        }
    }

    fn get(&self, sender_id: &u32, collective: &CollectiveType, counter: &u32) -> Vec<Message> {
        let by_counter = self
            .messages
            .get(sender_id)
            .unwrap()
            .get(collective)
            .unwrap()
            .read()
            .unwrap();
        if let Some(vec) = by_counter.get(&counter) {
            let mut vec = vec.write().unwrap();
            let mut result = Vec::new();
            while let Ok(i) = vec.binary_search_by(|probe| probe.counter.cmp(counter)) {
                result.push(vec.remove(i));
            }
            result
        } else {
            Vec::new()
        }
    }

    fn get_all(&self, sender_id: &u32, collective: &CollectiveType) -> Vec<Message> {
        let mut by_counter = self
            .messages
            .get(sender_id)
            .unwrap()
            .get(collective)
            .unwrap()
            .write()
            .unwrap();

        by_counter
            .drain()
            .filter_map(|(_, v)| {
                let mut vec = v.write().unwrap();
                if vec.is_empty() {
                    None
                } else {
                    Some(vec.drain(..).collect::<Vec<_>>())
                }
            })
            .flatten()
            .collect()
    }

    fn get_any(&self, sender_id: &u32, collective: &CollectiveType) -> Option<Message> {
        let by_counter = self
            .messages
            .get(sender_id)
            .unwrap()
            .get(collective)
            .unwrap()
            .read()
            .unwrap();

        by_counter
            .keys()
            .next()
            .map(|k| by_counter.get(k))
            .flatten()
            .map(|v| v.write().unwrap())
            .map(|mut vec| vec.pop())
            .flatten()
    }
}

/// A [`MessageStore`] implementation that uses a [`HashMap`] and [`ChunkStore`]s to store messages.
pub struct MessageStoreChunked {
    messages: HashMap<u32, HashMap<CollectiveType, Mutex<HashMap<u32, VecChunkStore>>>>,
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
    pub fn new(ids: impl Iterator<Item = u32>, collectives: &[CollectiveType]) -> Self {
        MessageStoreChunked {
            messages: ids
                .map(|id| {
                    (
                        id,
                        collectives
                            .iter()
                            .map(|c| (*c, Mutex::new(HashMap::new())))
                            .collect(),
                    )
                })
                .collect(),
        }
    }
}

impl MessageStoreChunked {
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
    pub fn insert(&self, msg: Message) {
        let chunk_id = msg.chunk_id;

        let mut by_counter_write = self
            .messages
            .get(&msg.sender_id)
            .unwrap()
            .get(&msg.collective)
            .unwrap()
            .lock()
            .unwrap();

        if let Some(chunk_store) = by_counter_write.get_mut(&msg.counter) {
            chunk_store.insert(chunk_id, msg.data);
        } else {
            let counter = msg.counter;
            let mut chunk_store = VecChunkStore::new(msg.num_chunks);
            chunk_store.insert(chunk_id, msg.data);
            by_counter_write.insert(counter, chunk_store);
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
        &self,
        sender_id: &u32,
        collective: &CollectiveType,
        counter: &u32,
    ) -> Option<Message> {
        let mut by_counter_write = self
            .messages
            .get(sender_id)
            .unwrap()
            .get(collective)
            .unwrap()
            .lock()
            .unwrap();

        let is_complete = match by_counter_write.get(counter) {
            Some(chunk_store) => chunk_store.is_complete(),
            None => false,
        };

        if is_complete {
            let chunk_store = by_counter_write.remove(counter).unwrap();
            let body = chunk_store.get_complete_payload();
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

    pub fn get_any(&self, sender_id: &u32, collective: &CollectiveType) -> Option<Message> {
        let mut by_counter = self
            .messages
            .get(sender_id)
            .unwrap()
            .get(collective)
            .unwrap()
            .lock()
            .unwrap();

        let mut complete_counter: Option<u32> = None;
        for counter in by_counter.keys() {
            match by_counter.get(counter) {
                Some(chunk_store) => {
                    if chunk_store.is_complete() {
                        complete_counter = Some(*counter);
                        break;
                    }
                }
                None => continue,
            };
        }

        match complete_counter {
            Some(counter) => {
                let chunk_store = by_counter.remove(&counter).unwrap();
                let body = chunk_store.get_complete_payload();
                return Some(Message {
                    sender_id: *sender_id,
                    chunk_id: 0,
                    num_chunks: 1,
                    counter: counter,
                    collective: *collective,
                    data: body,
                });
            }
            None => return None,
        }
    }
}
