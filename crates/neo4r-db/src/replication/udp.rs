use super::*;
use std::net::SocketAddr;

pub struct ReliableDatagramSocket {
    socket: UdpSocket,
    max_frame_bytes: usize,
}

impl ReliableDatagramSocket {
    pub fn bind(address: &str, max_frame_bytes: usize) -> DatabaseResult<Self> {
        let socket = UdpSocket::bind(address)
            .map_err(|err| DatabaseError::Replication(format!("bind udp {address}: {err}")))?;
        Ok(Self {
            socket,
            max_frame_bytes: max_frame_bytes.max(1),
        })
    }

    pub fn local_addr(&self) -> DatabaseResult<SocketAddr> {
        self.socket
            .local_addr()
            .map_err(|err| DatabaseError::Replication(format!("read udp local addr: {err}")))
    }

    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> DatabaseResult<()> {
        self.socket
            .set_read_timeout(timeout)
            .map_err(|err| DatabaseError::Replication(format!("set udp read timeout: {err}")))
    }

    pub fn send_frame_to(
        &self,
        frame: &ReliableDatagramFrame,
        target: SocketAddr,
    ) -> DatabaseResult<usize> {
        let payload = frame.encode();
        if payload.len() > self.max_frame_bytes {
            return Err(DatabaseError::Replication(format!(
                "udp frame {} exceeds max frame bytes {}",
                payload.len(),
                self.max_frame_bytes
            )));
        }
        self.socket
            .send_to(&payload, target)
            .map_err(|err| DatabaseError::Replication(format!("send udp frame: {err}")))
    }

    pub fn recv_frame_from(&self) -> DatabaseResult<(ReliableDatagramFrame, SocketAddr)> {
        let mut buf = vec![0; self.max_frame_bytes];
        let (len, source) = self
            .socket
            .recv_from(&mut buf)
            .map_err(|err| DatabaseError::Replication(format!("recv udp frame: {err}")))?;
        Ok((ReliableDatagramFrame::decode(&buf[..len])?, source))
    }
}
