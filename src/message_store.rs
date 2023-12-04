use std::{collections::HashMap, sync::RwLock};

use crate::types::{CollectiveType, Message};

/// Trait representing a message store.
///
/// This trait is used to define the behavior of a message store, which is responsible for storing and retrieving messages.
/// It is `Send` and `Sync`, meaning it can be safely sent and shared between multiple threads.
pub trait MessageStore: Send + Sync {
    /// Inserts a message into the message store.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be inserted.
    ///
    fn insert(&self, msg: Message);

    /// Retrieves messages from the `MessageStore` based on sender ID, collective type, and counter,
    /// removing them from the store.
    ///
    /// # Arguments
    ///
    /// * `sender_id` - The sender ID of the messages to retrieve.
    /// * `collective` - The collective type of the messages to retrieve.
    /// * `counter` - The counter value of the messages to retrieve.
    ///
    /// # Returns
    ///
    /// A vector of messages that match the given criteria.
    fn get(&self, sender_id: &u32, collective: &CollectiveType, counter: &u32) -> Vec<Message>;
}

/// A `MessageStore` implementation that uses a `HashMap` to store messages.
#[derive(Debug)]
pub struct MessageStoreHashMap {
    messages: HashMap<u32, HashMap<CollectiveType, RwLock<HashMap<u32, RwLock<Vec<Message>>>>>>,
}

impl MessageStoreHashMap {
    /// Creates a new `MessageStore` with the given IDs and collective types.
    ///
    /// # Arguments
    ///
    /// * `ids` - An iterator of sender IDs.
    /// * `collectives` - A set of collective types.
    ///
    /// # Returns
    ///
    /// A new `MessageStore` instance.
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
            vec.write().unwrap().push(msg);
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
}
