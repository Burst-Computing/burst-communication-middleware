use crate::Message;
use bytes::{Bytes, BytesMut};

/// Trait representing a chunked message body.
///
/// This trait is used to define the behavior of a chunked message body, which is responsible for storing
/// and retrieving chunks of a [`Message`] body.
pub trait ChunkedMessageBody {
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
    fn get_complete_body(self) -> Bytes;
}

/// A [`ChunkedMessageBody`] implementation that stores the chunks in a [`Vec`].
#[derive(Debug)]
pub struct VecChunkedMessageBody {
    array: Vec<Bytes>,
    num_chunks: u32,
    num_chunks_received: u32,
    is_complete: bool,
}

impl VecChunkedMessageBody {
    /// Creates a new [`ChunkedMessageBody`] instance.
    ///
    /// # Arguments
    ///
    /// * `num_chunks` - The total number of chunks in the message.
    ///
    /// # Returns
    ///
    /// A new [`ChunkedMessageBody`] instance.
    pub fn new(num_chunks: u32) -> Self {
        Self {
            array: Vec::with_capacity(num_chunks as usize),
            num_chunks,
            num_chunks_received: 0,
            is_complete: false,
        }
    }
}

impl ChunkedMessageBody for VecChunkedMessageBody {
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

    fn get_complete_body(self) -> Bytes {
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
    let mut chunks = Vec::new();
    let mut body = msg.data.clone();
    while !body.is_empty() {
        let chunk = body.split_to(std::cmp::min(body.len(), max_chunk_size));
        chunks.push(chunk);
    }

    let num_chunks = chunks.len();
    chunks
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
