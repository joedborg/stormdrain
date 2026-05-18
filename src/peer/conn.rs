//! Async peer connection: reads and writes length-prefixed peer wire messages
//! over any async stream (plain TCP or MSE-encrypted).

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::{Duration, timeout};

use crate::{
    error::{Error, Result},
    peer::message::Message,
};

/// Maximum message payload we'll accept from a peer (prevents memory exhaustion).
const MAX_MSG_LEN: u32 = 1 << 23; // 8 MiB

/// Object-safe supertrait combining AsyncRead + AsyncWrite + Unpin + Send.
pub trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncStream for T {}

/// Async peer connection that reads and writes length-prefixed peer wire messages.
pub struct PeerConn {
    stream: Box<dyn AsyncStream>,
    read_buf: BytesMut,
}

impl PeerConn {
    /// Wrap any `AsyncRead + AsyncWrite + Unpin + Send` stream into a peer connection.
    pub fn new(stream: impl AsyncRead + AsyncWrite + Unpin + Send + 'static) -> Self {
        PeerConn {
            stream: Box::new(stream),
            read_buf: BytesMut::with_capacity(65536),
        }
    }

    /// Read the next message from the peer (blocks until one arrives).
    /// Returns `None` on a clean EOF between messages.
    pub async fn read_message(&mut self) -> Result<Option<Message>> {
        let len = match self.read_length_prefix().await? {
            None => return Ok(None),
            Some(0) => return Ok(Some(Message::KeepAlive)),
            Some(n) if n > MAX_MSG_LEN => {
                return Err(Error::Peer(format!(
                    "peer sent oversized message: {n} bytes"
                )));
            }
            Some(n) => n,
        };

        self.fill(len as usize).await?;
        let id = self.read_buf[0];
        let payload = self.read_buf[1..len as usize].to_vec();
        let _ = self.read_buf.split_to(len as usize);

        Ok(Some(Message::decode(id, &payload)?))
    }

    /// Read the 4-byte message length prefix.
    /// Returns `Ok(None)` if the peer disconnected cleanly between messages
    /// (the buffer was empty when EOF arrived). Returns `Ok(Some(len))` on
    /// success, or `Err` on I/O error or mid-message disconnect.
    async fn read_length_prefix(&mut self) -> Result<Option<u32>> {
        while self.read_buf.len() < 4 {
            let mut tmp = [0u8; 8192];
            let read = self.stream.read(&mut tmp).await?;
            if read == 0 {
                return if self.read_buf.is_empty() {
                    Ok(None) // clean disconnect between messages
                } else {
                    Err(Error::Peer("connection closed by peer".into()))
                };
            }
            self.read_buf.put_slice(&tmp[..read]);
        }
        let len = u32::from_be_bytes(self.read_buf[..4].try_into().unwrap());
        let _ = self.read_buf.split_to(4);
        Ok(Some(len))
    }

    /// Read next message with a wall-clock timeout.
    pub async fn read_message_timeout(&mut self, dur: Duration) -> Result<Option<Message>> {
        timeout(dur, self.read_message())
            .await
            .map_err(|_| Error::Peer("read timeout".into()))?
    }

    /// Write a message to the peer.
    pub async fn send(&mut self, msg: &Message) -> Result<()> {
        let encoded = msg.encode();
        self.stream.write_all(&encoded).await?;
        Ok(())
    }

    /// Fill the internal buffer until it has at least `n` bytes.
    async fn fill(&mut self, n: usize) -> Result<()> {
        while self.read_buf.len() < n {
            let mut tmp = [0u8; 8192];
            let read = self.stream.read(&mut tmp).await?;
            if read == 0 {
                return Err(Error::Peer("connection closed by peer".into()));
            }
            self.read_buf.put_slice(&tmp[..read]);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::message::Message;
    use tokio::io::duplex;

    #[tokio::test]
    async fn send_and_receive_choke() {
        let (a, b) = duplex(4096);
        let mut conn_a = PeerConn::new(a);
        let mut conn_b = PeerConn::new(b);

        conn_a.send(&Message::Choke).await.unwrap();
        let msg = conn_b.read_message().await.unwrap();
        assert!(matches!(msg, Some(Message::Choke)));
    }

    #[tokio::test]
    async fn send_and_receive_have() {
        let (a, b) = duplex(4096);
        let mut conn_a = PeerConn::new(a);
        let mut conn_b = PeerConn::new(b);

        conn_a.send(&Message::Have(42)).await.unwrap();
        let msg = conn_b.read_message().await.unwrap().unwrap();
        assert!(matches!(msg, Message::Have(42)));
    }

    #[tokio::test]
    async fn send_and_receive_keepalive() {
        let (a, b) = duplex(4096);
        let mut conn_a = PeerConn::new(a);
        let mut conn_b = PeerConn::new(b);

        conn_a.send(&Message::KeepAlive).await.unwrap();
        let msg = conn_b.read_message().await.unwrap();
        assert!(matches!(msg, Some(Message::KeepAlive)));
    }

    #[tokio::test]
    async fn multiple_messages_in_sequence() {
        let (a, b) = duplex(65536);
        let mut conn_a = PeerConn::new(a);
        let mut conn_b = PeerConn::new(b);

        conn_a.send(&Message::Interested).await.unwrap();
        conn_a.send(&Message::Have(100)).await.unwrap();
        conn_a.send(&Message::Unchoke).await.unwrap();

        assert!(matches!(
            conn_b.read_message().await.unwrap(),
            Some(Message::Interested)
        ));
        assert!(matches!(
            conn_b.read_message().await.unwrap(),
            Some(Message::Have(100))
        ));
        assert!(matches!(
            conn_b.read_message().await.unwrap(),
            Some(Message::Unchoke)
        ));
    }

    #[tokio::test]
    async fn read_message_timeout_triggers() {
        let (a, _b) = duplex(4096); // _b is not used; a will never receive data
        let mut conn_a = PeerConn::new(a);

        let result = conn_a.read_message_timeout(Duration::from_millis(50)).await;
        assert!(result.is_err());
    }
}
