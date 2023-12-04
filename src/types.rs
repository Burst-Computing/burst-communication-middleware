use std::fmt::{Debug, Display};

use bytes::Bytes;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone)]
#[repr(C)]
pub struct Message {
    pub sender_id: u32,
    pub chunk_id: u32,
    pub last_chunk: bool,
    pub counter: u32,
    pub collective: CollectiveType,
    pub data: Bytes,
}

// Serialize message to two chunks of contiguous bytes
// without memory allocations
impl<'a> From<&'a Message> for [&'a [u8]; 2] {
    fn from(msg: &'a Message) -> Self {
        let bytes = msg.data.as_ref();
        let msg_header = unsafe {
            std::slice::from_raw_parts(
                msg as *const Message as *const u8,
                std::mem::size_of::<Message>() - std::mem::size_of::<Bytes>(),
            )
        };
        [msg_header, bytes]
    }
}

// Deserialize message from a chunk of contiguous bytes
// without memory allocations
impl From<Vec<u8>> for Message {
    fn from(v: Vec<u8>) -> Self {
        let header_size = std::mem::size_of::<Message>() - std::mem::size_of::<Bytes>();
        let (header, _) = v.split_at(header_size);

        let mut msg = deserialize_header(&header);

        let mut data = Bytes::from(v);
        msg.data = data.split_off(header_size);

        msg
    }
}

impl From<(Vec<u8>, Vec<u8>)> for Message {
    fn from(v: (Vec<u8>, Vec<u8>)) -> Self {
        let (header, data) = v;

        let mut msg = deserialize_header(&header);
        msg.data = Bytes::from(data);

        msg
    }
}

// Deserialize the header from a chunk of contiguous bytes
fn deserialize_header(header: &[u8]) -> Message {
    let mut msg = Message {
        sender_id: 0,
        chunk_id: 0,
        last_chunk: false,
        counter: 0,
        collective: CollectiveType::Direct,
        data: Bytes::new(),
    };

    // owerwrite the message with the header data
    unsafe {
        std::ptr::copy_nonoverlapping(
            header.as_ptr(),
            &mut msg as *mut Message as *mut u8,
            header.len(),
        );
    }

    msg
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
    Direct,
    Broadcast,
    Scatter,
    Gather,
    AllToAll,
}

impl Display for CollectiveType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<&str> for CollectiveType {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "direct" => CollectiveType::Direct,
            "broadcast" => CollectiveType::Broadcast,
            "scatter" => CollectiveType::Scatter,
            "gather" => CollectiveType::Gather,
            "alltoall" => CollectiveType::AllToAll,
            _ => panic!("Invalid collective type: {:?}", s),
        }
    }
}

impl From<u32> for CollectiveType {
    fn from(n: u32) -> Self {
        match n {
            0 => CollectiveType::Direct,
            1 => CollectiveType::Broadcast,
            2 => CollectiveType::Scatter,
            3 => CollectiveType::Gather,
            4 => CollectiveType::AllToAll,
            _ => panic!("Invalid collective type: {:?}", n),
        }
    }
}
