use std::fmt::{Debug, Display};

use bytes::Bytes;

#[derive(Clone)]
pub struct Message {
    pub sender_id: u32,
    pub chunk_id: u32,
    pub last_chunk: bool,
    pub counter: Option<u32>,
    pub collective: CollectiveType,
    pub data: Bytes,
}

impl Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Message")
            .field("sender_id", &self.sender_id)
            .field("chunk_id", &self.chunk_id)
            .field("last_chunk", &self.last_chunk)
            .field("counter", &self.counter)
            .field("collective", &self.collective)
            .field("data", &self.data.len())
            .finish()
    }
}

// types of collectives
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum CollectiveType {
    Broadcast,
    Scatter,
    Gather,
    None,
}

impl Display for CollectiveType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<&str> for CollectiveType {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "broadcast" => CollectiveType::Broadcast,
            "scatter" => CollectiveType::Scatter,
            "gather" => CollectiveType::Gather,
            _ => CollectiveType::None,
        }
    }
}

impl From<u32> for CollectiveType {
    fn from(n: u32) -> Self {
        match n {
            0 => CollectiveType::Broadcast,
            1 => CollectiveType::Scatter,
            2 => CollectiveType::Gather,
            _ => CollectiveType::None,
        }
    }
}
