use std::fmt::{Debug, Display};

use bytes::Bytes;
use redis::streams::StreamReadReply;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone)]
pub struct Message {
    pub sender_id: u32,
    pub chunk_id: u32,
    pub last_chunk: bool,
    pub counter: Option<u32>,
    pub collective: CollectiveType,
    pub data: Bytes,
}

impl FromIterator<(String, Vec<u8>)> for Message {
    fn from_iter<T: IntoIterator<Item = (String, Vec<u8>)>>(iter: T) -> Self {
        let mut sender_id = 0;
        let mut chunk_id = 0;
        let mut last_chunk = false;
        let mut counter = None;
        let mut collective = CollectiveType::None;
        let mut data = Bytes::new();

        for (k, v) in iter {
            match k.as_str() {
                "sender_id" => sender_id = u32::from_le_bytes(v[..4].try_into().unwrap()),
                "chunk_id" => chunk_id = u32::from_le_bytes(v[..4].try_into().unwrap()),
                "last_chunk" => last_chunk = u8::from_le_bytes(v[..1].try_into().unwrap()) != 0,
                "counter" => {
                    counter = match v.len() {
                        0 => None,
                        _ => Some(u32::from_le_bytes(v[..4].try_into().unwrap())),
                    }
                }
                "collective" => {
                    collective =
                        CollectiveType::from(u32::from_le_bytes(v[..4].try_into().unwrap()))
                }
                "data" => data = v.into(),
                _ => (),
            }
        }

        Message {
            sender_id,
            chunk_id,
            last_chunk,
            counter,
            collective,
            data,
        }
    }
}

pub struct MessageIntoIterator {
    msg: Message,
    index: u32,
}

impl Iterator for MessageIntoIterator {
    type Item = (String, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        let result = match self.index {
            0 => (
                "sender_id".to_string(),
                self.msg.sender_id.to_le_bytes().to_vec(),
            ),
            1 => (
                "chunk_id".to_string(),
                self.msg.chunk_id.to_le_bytes().to_vec(),
            ),
            2 => (
                "last_chunk".to_string(),
                (if self.msg.last_chunk { 1 } else { 0 as u32 })
                    .to_le_bytes()
                    .to_vec(),
            ),
            3 => (
                "counter".to_string(),
                match self.msg.counter {
                    None => Vec::new(),
                    Some(n) => n.to_le_bytes().to_vec(),
                },
            ),
            4 => (
                "collective".to_string(),
                (self.msg.collective as u32).to_le_bytes().to_vec(),
            ),
            5 => ("data".to_string(), self.msg.data.to_vec()),
            _ => return None,
        };
        self.index += 1;
        Some(result)
    }
}

impl IntoIterator for Message {
    type Item = (String, Vec<u8>);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        let v: Vec<(String, Vec<u8>)> = self.into();
        v.into_iter()
    }
}

impl From<Message> for Vec<(String, Vec<u8>)> {
    fn from(msg: Message) -> Self {
        let mut v = Vec::new();

        v.push((
            "sender_id".to_string(),
            msg.sender_id.to_le_bytes().to_vec(),
        ));
        v.push(("chunk_id".to_string(), msg.chunk_id.to_le_bytes().to_vec()));
        v.push((
            "last_chunk".to_string(),
            (if msg.last_chunk { 1 } else { 0 as u32 })
                .to_le_bytes()
                .to_vec(),
        ));
        if let Some(n) = msg.counter {
            v.push(("counter".to_string(), n.to_le_bytes().to_vec()));
        }
        v.push((
            "collective".to_string(),
            (msg.collective as u32).to_le_bytes().to_vec(),
        ));
        v.push(("data".to_string(), msg.data.to_vec()));

        v
    }
}

impl From<StreamReadReply> for Message {
    fn from(reply: StreamReadReply) -> Self {
        reply
            .keys
            .into_iter()
            .next()
            .unwrap()
            .ids
            .into_iter()
            .next()
            .unwrap()
            .map
            .into_iter()
            .map(|(k, v)| {
                (
                    k,
                    match v {
                        redis::Value::Data(v) => v,
                        _ => Vec::new(),
                    },
                )
            })
            .collect::<Message>()
    }
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
    AllToAll,
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
            "alltoall" => CollectiveType::AllToAll,
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
            3 => CollectiveType::AllToAll,
            _ => CollectiveType::None,
        }
    }
}
