use super::rdma::RdmaProbeTarget;
use crate::{DatabaseError, DatabaseResult};
use std::io::{Read, Write};
use std::net::{SocketAddr, ToSocketAddrs};
use std::os::raw::{c_int, c_void};
use std::time::Duration;

pub struct RsocketStream {
    fd: c_int,
}

impl RsocketStream {
    pub(super) fn connect_target(
        target: &RdmaProbeTarget,
        connect_timeout: Duration,
    ) -> DatabaseResult<Self> {
        let address = target.socket_address();
        let addrs = address
            .to_socket_addrs()
            .map_err(|err| DatabaseError::Replication(format!("resolve RDMA {address}: {err}")))?;
        let mut last_error = None;
        for addr in addrs {
            match Self::connect_addr(addr, connect_timeout) {
                Ok(stream) => return Ok(stream),
                Err(err) => last_error = Some(err),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            DatabaseError::Replication(format!("no socket address for RDMA {address}"))
        }))
    }

    fn connect_addr(addr: SocketAddr, connect_timeout: Duration) -> DatabaseResult<Self> {
        let raw = RawSocketAddress::from(addr);
        let fd = unsafe { rsocket(raw.family(), SOCK_STREAM, 0) };
        if fd < 0 {
            return Err(last_os_replication_error("rsocket"));
        }
        let stream = Self { fd };
        stream.set_timeout(SO_RCVTIMEO, connect_timeout)?;
        stream.set_timeout(SO_SNDTIMEO, connect_timeout)?;
        let connected = unsafe { rconnect(stream.fd, raw.as_ptr(), raw.len()) };
        if connected < 0 {
            let err = last_os_replication_error("rconnect");
            drop(stream);
            return Err(err);
        }
        Ok(stream)
    }

    fn set_timeout(&self, option: c_int, timeout: Duration) -> DatabaseResult<()> {
        let value = Timeval {
            tv_sec: timeout.as_secs() as TimevalSeconds,
            tv_usec: timeout.subsec_micros() as TimevalMicros,
        };
        let result = unsafe {
            rsetsockopt(
                self.fd,
                SOL_SOCKET,
                option,
                (&value as *const Timeval).cast::<c_void>(),
                std::mem::size_of::<Timeval>() as SockLen,
            )
        };
        if result < 0 {
            return Err(last_os_replication_error("rsetsockopt"));
        }
        Ok(())
    }
}

pub struct RdmaReplicationListener {
    fd: c_int,
}

impl RdmaReplicationListener {
    pub fn bind(address: &str) -> DatabaseResult<Self> {
        let addrs = address.to_socket_addrs().map_err(|err| {
            DatabaseError::Replication(format!("resolve RDMA bind {address}: {err}"))
        })?;
        let mut last_error = None;
        for addr in addrs {
            match Self::bind_addr(addr) {
                Ok(listener) => return Ok(listener),
                Err(err) => last_error = Some(err),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            DatabaseError::Replication(format!("no socket address for RDMA bind {address}"))
        }))
    }

    fn bind_addr(addr: SocketAddr) -> DatabaseResult<Self> {
        let raw = RawSocketAddress::from(addr);
        let fd = unsafe { rsocket(raw.family(), SOCK_STREAM, 0) };
        if fd < 0 {
            return Err(last_os_replication_error("rsocket"));
        }
        let listener = Self { fd };
        let bound = unsafe { rbind(listener.fd, raw.as_ptr(), raw.len()) };
        if bound < 0 {
            let err = last_os_replication_error("rbind");
            drop(listener);
            return Err(err);
        }
        let listening = unsafe { rlisten(listener.fd, 128) };
        if listening < 0 {
            let err = last_os_replication_error("rlisten");
            drop(listener);
            return Err(err);
        }
        Ok(listener)
    }

    pub fn accept(&self) -> DatabaseResult<RsocketStream> {
        let fd = unsafe { raccept(self.fd, std::ptr::null_mut(), std::ptr::null_mut()) };
        if fd < 0 {
            return Err(last_os_replication_error("raccept"));
        }
        Ok(RsocketStream { fd })
    }

    pub fn local_addr(&self) -> DatabaseResult<SocketAddr> {
        let mut storage = SockAddrStorage::default();
        let mut len = std::mem::size_of::<SockAddrStorage>() as SockLen;
        let result = unsafe {
            rgetsockname(
                self.fd,
                (&mut storage as *mut SockAddrStorage).cast::<SockAddr>(),
                &mut len,
            )
        };
        if result < 0 {
            return Err(last_os_replication_error("rgetsockname"));
        }
        storage.to_socket_addr()
    }
}

impl Drop for RdmaReplicationListener {
    fn drop(&mut self) {
        unsafe {
            let _ = rclose(self.fd);
        }
    }
}

impl Read for RsocketStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let read = unsafe { rrecv(self.fd, buf.as_mut_ptr().cast::<c_void>(), buf.len(), 0) };
        if read < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(read as usize)
        }
    }
}

impl Write for RsocketStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let written = unsafe { rsend(self.fd, buf.as_ptr().cast::<c_void>(), buf.len(), 0) };
        if written < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(written as usize)
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for RsocketStream {
    fn drop(&mut self) {
        unsafe {
            let _ = rclose(self.fd);
        }
    }
}

enum RawSocketAddress {
    V4(SockAddrIn),
    V6(SockAddrIn6),
}

impl RawSocketAddress {
    fn family(&self) -> c_int {
        match self {
            Self::V4(_) => AF_INET,
            Self::V6(_) => AF_INET6,
        }
    }

    fn as_ptr(&self) -> *const SockAddr {
        match self {
            Self::V4(addr) => (addr as *const SockAddrIn).cast::<SockAddr>(),
            Self::V6(addr) => (addr as *const SockAddrIn6).cast::<SockAddr>(),
        }
    }

    fn len(&self) -> SockLen {
        match self {
            Self::V4(_) => std::mem::size_of::<SockAddrIn>() as SockLen,
            Self::V6(_) => std::mem::size_of::<SockAddrIn6>() as SockLen,
        }
    }
}

impl From<SocketAddr> for RawSocketAddress {
    fn from(addr: SocketAddr) -> Self {
        match addr {
            SocketAddr::V4(addr) => Self::V4(SockAddrIn {
                sin_family: AF_INET as AddressFamily,
                sin_port: addr.port().to_be(),
                sin_addr: InAddr {
                    s_addr: u32::from_ne_bytes(addr.ip().octets()),
                },
                sin_zero: [0; 8],
            }),
            SocketAddr::V6(addr) => Self::V6(SockAddrIn6 {
                sin6_family: AF_INET6 as AddressFamily,
                sin6_port: addr.port().to_be(),
                sin6_flowinfo: addr.flowinfo(),
                sin6_addr: In6Addr {
                    s6_addr: addr.ip().octets(),
                },
                sin6_scope_id: addr.scope_id(),
            }),
        }
    }
}

#[repr(C)]
struct SockAddr {
    sa_family: AddressFamily,
    sa_data: [u8; 14],
}

#[repr(C)]
struct InAddr {
    s_addr: u32,
}

#[repr(C)]
struct SockAddrIn {
    sin_family: AddressFamily,
    sin_port: u16,
    sin_addr: InAddr,
    sin_zero: [u8; 8],
}

#[repr(C)]
struct In6Addr {
    s6_addr: [u8; 16],
}

#[repr(C)]
struct SockAddrIn6 {
    sin6_family: AddressFamily,
    sin6_port: u16,
    sin6_flowinfo: u32,
    sin6_addr: In6Addr,
    sin6_scope_id: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct SockAddrStorage {
    ss_family: AddressFamily,
    __data: [u8; 126],
}

impl Default for SockAddrStorage {
    fn default() -> Self {
        Self {
            ss_family: 0,
            __data: [0; 126],
        }
    }
}

impl SockAddrStorage {
    fn to_socket_addr(self) -> DatabaseResult<SocketAddr> {
        match self.ss_family as c_int {
            AF_INET => {
                let addr = unsafe {
                    std::ptr::read((&self as *const SockAddrStorage).cast::<SockAddrIn>())
                };
                Ok(SocketAddr::from((
                    std::net::Ipv4Addr::from(addr.sin_addr.s_addr.to_ne_bytes()),
                    u16::from_be(addr.sin_port),
                )))
            }
            AF_INET6 => {
                let addr = unsafe {
                    std::ptr::read((&self as *const SockAddrStorage).cast::<SockAddrIn6>())
                };
                Ok(SocketAddr::from((
                    std::net::Ipv6Addr::from(addr.sin6_addr.s6_addr),
                    u16::from_be(addr.sin6_port),
                )))
            }
            family => Err(DatabaseError::Replication(format!(
                "unsupported RDMA socket address family {family}"
            ))),
        }
    }
}

#[repr(C)]
struct Timeval {
    tv_sec: TimevalSeconds,
    tv_usec: TimevalMicros,
}

type AddressFamily = u16;
type SockLen = u32;
#[cfg(target_pointer_width = "64")]
type TimevalSeconds = i64;
#[cfg(target_pointer_width = "64")]
type TimevalMicros = i64;
#[cfg(target_pointer_width = "32")]
type TimevalSeconds = i32;
#[cfg(target_pointer_width = "32")]
type TimevalMicros = i32;

const AF_INET: c_int = 2;
const AF_INET6: c_int = 10;
const SOCK_STREAM: c_int = 1;
const SOL_SOCKET: c_int = 1;
const SO_RCVTIMEO: c_int = 20;
const SO_SNDTIMEO: c_int = 21;

fn last_os_replication_error(operation: &str) -> DatabaseError {
    DatabaseError::Replication(format!("{operation}: {}", std::io::Error::last_os_error()))
}

#[link(name = ":librdmacm.so.1")]
unsafe extern "C" {
    fn rsocket(domain: c_int, socket_type: c_int, protocol: c_int) -> c_int;
    fn rbind(fd: c_int, addr: *const SockAddr, addrlen: SockLen) -> c_int;
    fn rlisten(fd: c_int, backlog: c_int) -> c_int;
    fn raccept(fd: c_int, addr: *mut SockAddr, addrlen: *mut SockLen) -> c_int;
    fn rconnect(fd: c_int, addr: *const SockAddr, addrlen: SockLen) -> c_int;
    fn rgetsockname(fd: c_int, addr: *mut SockAddr, addrlen: *mut SockLen) -> c_int;
    fn rsetsockopt(
        fd: c_int,
        level: c_int,
        optname: c_int,
        optval: *const c_void,
        optlen: SockLen,
    ) -> c_int;
    fn rsend(fd: c_int, buf: *const c_void, len: usize, flags: c_int) -> isize;
    fn rrecv(fd: c_int, buf: *mut c_void, len: usize, flags: c_int) -> isize;
    fn rclose(fd: c_int) -> c_int;
}
