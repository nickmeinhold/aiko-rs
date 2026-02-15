//! Data channel message types.

/// An incoming message received on a data channel.
#[derive(Debug, Clone)]
pub struct IncomingMessage {
    /// The label of the data channel this message was received on.
    pub channel: String,
    /// The raw payload bytes.
    pub payload: Vec<u8>,
}
