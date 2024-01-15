use crate::Message;
use bytes::{Bytes, BytesMut};

/// Trait representing a chunk store.
///
/// This trait is used to define the behavior of a chunk store, which is responsible for storing
/// and retrieving chunks of a [`Message`].
pub trait ChunkStore {
    /// Inserts a chunk into the `ChunkStore` at the specified chunk ID.
    ///
    /// # Arguments
    ///
    /// * `chunk_id` - The ID of the chunk.
    /// * `chunk` - The chunk data.
    fn insert(&mut self, chunk_id: u32, chunk: Bytes);

    /// Checks if all the chunks have been received.
    ///
    /// # Returns
    ///
    /// `true` if all the chunks have been received, `false` otherwise.
    fn is_complete(&self) -> bool;

    /// Retrieves the complete message by combining all the chunks.
    ///
    /// # Returns
    ///
    /// The complete message.
    ///
    /// # Panics
    ///
    /// Panics if the message is not complete.
    fn get_complete_payload(self) -> Bytes;
}

/// A [`ChunkStore`] implementation that stores the chunks in a [`Vec`].
#[derive(Debug)]
pub struct VecChunkStore {
    array: Vec<Bytes>,
    num_chunks: u32,
    num_chunks_received: u32,
    is_complete: bool,
}

impl VecChunkStore {
    /// Creates a new [`ChunkStore`] instance.
    ///
    /// # Arguments
    ///
    /// * `num_chunks` - The total number of chunks in the message.
    /// * `msg_header` - The header of the message.
    ///
    /// # Returns
    ///
    /// A new `ChunkStore` instance.
    pub fn new(num_chunks: u32) -> Self {
        Self {
            array: vec![Vec::new().into(); num_chunks as usize],
            num_chunks,
            num_chunks_received: 0,
            is_complete: false,
        }
    }
}

impl ChunkStore for VecChunkStore {
    fn insert(&mut self, chunk_id: u32, chunk: Bytes) {
        self.array[chunk_id as usize] = chunk;
        self.num_chunks_received += 1;
        if self.num_chunks_received > self.num_chunks {
            panic!("Received more chunks than expected");
        } else if self.num_chunks_received == self.num_chunks {
            self.is_complete = true;
        }
    }

    fn is_complete(&self) -> bool {
        self.is_complete
    }

    fn get_complete_payload(self) -> Bytes {
        assert!(self.is_complete(), "Message is not complete");
        let mut data = BytesMut::with_capacity(self.array.iter().map(|x| x.len()).sum());
        for chunk in self.array {
            data.extend_from_slice(&chunk);
        }
        data.freeze()
    }
}

/// Splits a message into multiple smaller messages (chunks) based on the maximum chunk size.
///
/// # Arguments
///
/// * `msg` - The original message.
/// * `max_chunk_size` - The maximum size of each chunk.
///
/// # Returns
///
/// A vector of chunked messages.
pub fn chunk_message(msg: &Message, max_chunk_size: usize) -> Vec<Message> {
    let chunked_data = chunk_data(msg.data.clone(), max_chunk_size);
    let num_chunks = chunked_data.len();
    chunked_data
        .into_iter()
        .enumerate()
        .map(|(i, data)| Message {
            sender_id: msg.sender_id,
            chunk_id: i as u32,
            num_chunks: num_chunks as u32,
            counter: msg.counter,
            collective: msg.collective,
            data,
        })
        .collect()
}

/// Splits the data into multiple smaller chunks based on the maximum chunk size.
///
/// # Arguments
///
/// * `data` - The original data.
/// * `max_chunk_size` - The maximum size of each chunk.
///
/// # Returns
///
/// A vector of chunked data.
pub fn chunk_data(mut data: Bytes, max_chunk_size: usize) -> Vec<Bytes> {
    let mut chunks = Vec::new();
    while !data.is_empty() {
        let chunk = data.split_to(std::cmp::min(data.len(), max_chunk_size));
        chunks.push(chunk);
    }
    chunks
}
