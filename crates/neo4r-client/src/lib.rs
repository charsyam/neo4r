//! Blocking Rust client SDK for neo4r native protocol.

pub use neo4r_core::{Node, Relationship, Value};
use neo4r_protocol::{
    encode_query_payload, parse_result_page_response, parse_result_start_response,
    parse_rows_response, read_frame, response_field, write_frame, NativeFrame, NativeMessageType,
};
pub use neo4r_query::{QueryParams, QueryRow, QueryValue};
use std::io;
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

#[derive(Debug)]
pub enum ClientError {
    Io(io::Error),
    Protocol(String),
    Server(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(formatter, "{err}"),
            Self::Protocol(message) => write!(formatter, "protocol error: {message}"),
            Self::Server(message) => write!(formatter, "server error: {message}"),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<io::Error> for ClientError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

pub type ClientResult<T> = Result<T, ClientError>;

#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub connect_timeout: Option<Duration>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Some(Duration::from_secs(3)),
        }
    }
}

pub struct Client {
    stream: TcpStream,
    next_request_id: u64,
}

impl Client {
    pub fn connect(address: impl ToSocketAddrs) -> ClientResult<Self> {
        Self::connect_with_config(address, ClientConfig::default())
    }

    pub fn connect_with_config(
        address: impl ToSocketAddrs,
        config: ClientConfig,
    ) -> ClientResult<Self> {
        let mut addrs = address.to_socket_addrs()?;
        let Some(address) = addrs.next() else {
            return Err(ClientError::Protocol(
                "address did not resolve to a socket address".to_string(),
            ));
        };
        let stream = match config.connect_timeout {
            Some(timeout) => TcpStream::connect_timeout(&address, timeout)?,
            None => TcpStream::connect(address)?,
        };
        Ok(Self {
            stream,
            next_request_id: 1,
        })
    }

    pub fn ping(&mut self) -> ClientResult<()> {
        self.expect_exact(NativeMessageType::Ping, Vec::new(), "OK\tPONG")
    }

    pub fn close(&mut self) -> ClientResult<()> {
        self.expect_exact(NativeMessageType::Quit, Vec::new(), "OK\tBYE")
    }

    pub fn query(&mut self, query: &str) -> ClientResult<Vec<QueryRow>> {
        self.query_with_params(query, &QueryParams::new())
    }

    pub fn query_with_params(
        &mut self,
        query: &str,
        params: &QueryParams,
    ) -> ClientResult<Vec<QueryRow>> {
        let payload = encode_query_payload(query, params).into_bytes();
        let response = self.request(NativeMessageType::Query, payload)?;
        let start = parse_result_start_response(&response).map_err(ClientError::Protocol)?;
        let mut rows = start.rows;
        let mut has_more = start.has_more;
        while has_more {
            let page = self.fetch(start.cursor_id, None)?;
            has_more = page.has_more;
            rows.extend(page.rows);
        }
        let _ = self.close_cursor(start.cursor_id);
        Ok(rows)
    }

    pub fn execute(&mut self, query: &str) -> ClientResult<Vec<QueryRow>> {
        self.query(query)
    }

    pub fn execute_with_params(
        &mut self,
        query: &str,
        params: &QueryParams,
    ) -> ClientResult<Vec<QueryRow>> {
        self.query_with_params(query, params)
    }

    pub fn command(&mut self, command: &str) -> ClientResult<String> {
        self.request(NativeMessageType::Command, command.as_bytes().to_vec())
    }

    pub fn profile(&mut self, query: &str, params: &QueryParams) -> ClientResult<String> {
        let payload = format!("PROFILE\t{}", encode_query_payload(query, params));
        let response = self.command(&payload)?;
        response_field(&response, "PROFILE").map_err(ClientError::Protocol)
    }

    pub fn query_plan(&mut self, query: &str, params: &QueryParams) -> ClientResult<String> {
        let payload = format!("QUERY_PLAN\t{}", encode_query_payload(query, params));
        let response = self.command(&payload)?;
        response_field(&response, "QUERY_PLAN").map_err(ClientError::Protocol)
    }

    pub fn statistics(&mut self) -> ClientResult<String> {
        let response = self.command("STATISTICS")?;
        response_field(&response, "STATISTICS").map_err(ClientError::Protocol)
    }

    pub fn storage_status(&mut self) -> ClientResult<String> {
        let response = self.command("STORAGE_STATUS")?;
        response_field(&response, "STORAGE_STATUS").map_err(ClientError::Protocol)
    }

    pub fn metadata_log(&mut self) -> ClientResult<String> {
        let response = self.command("METADATA_LOG")?;
        response_field(&response, "METADATA_LOG").map_err(ClientError::Protocol)
    }

    pub fn cluster_status(&mut self) -> ClientResult<String> {
        let response = self.command("CLUSTER_STATUS")?;
        response_field(&response, "CLUSTER_STATUS").map_err(ClientError::Protocol)
    }

    pub fn cluster_management_status(&mut self) -> ClientResult<String> {
        let response = self.command("CLUSTER_MANAGEMENT_STATUS")?;
        response_field(&response, "CLUSTER_MANAGEMENT_STATUS").map_err(ClientError::Protocol)
    }

    pub fn raw_rows_command(&mut self, command: &str) -> ClientResult<Vec<QueryRow>> {
        let response = self.command(command)?;
        parse_rows_response(&response).map_err(ClientError::Protocol)
    }

    fn fetch(
        &mut self,
        cursor_id: u64,
        page_size: Option<usize>,
    ) -> ClientResult<neo4r_protocol::ResultPage> {
        let payload = match page_size {
            Some(page_size) => format!("{cursor_id}\t{page_size}"),
            None => cursor_id.to_string(),
        };
        let response = self.request(NativeMessageType::Fetch, payload.into_bytes())?;
        parse_result_page_response(&response).map_err(ClientError::Protocol)
    }

    fn close_cursor(&mut self, cursor_id: u64) -> ClientResult<()> {
        self.expect_exact(
            NativeMessageType::CloseCursor,
            cursor_id.to_string().into_bytes(),
            &format!("OK\tCURSOR_CLOSED\t{cursor_id}"),
        )
    }

    fn expect_exact(
        &mut self,
        message_type: NativeMessageType,
        payload: Vec<u8>,
        expected: &str,
    ) -> ClientResult<()> {
        let response = self.request(message_type, payload)?;
        if response == expected {
            Ok(())
        } else {
            Err(ClientError::Protocol(format!(
                "expected {expected}, got {response}"
            )))
        }
    }

    fn request(
        &mut self,
        message_type: NativeMessageType,
        payload: Vec<u8>,
    ) -> ClientResult<String> {
        let request_id = self.allocate_request_id();
        write_frame(
            &mut self.stream,
            &NativeFrame::new(message_type, request_id, payload),
        )?;
        let frame = read_frame(&mut self.stream)?
            .ok_or_else(|| ClientError::Protocol("server closed connection".to_string()))?;
        if frame.request_id != request_id {
            return Err(ClientError::Protocol(format!(
                "response request id mismatch: expected {request_id}, got {}",
                frame.request_id
            )));
        }
        let payload = frame.payload_text()?.to_string();
        match frame.message_type {
            NativeMessageType::Response => Ok(payload),
            NativeMessageType::Error => Err(ClientError::Server(payload)),
            other => Err(ClientError::Protocol(format!(
                "unexpected response message type: {other:?}"
            ))),
        }
    }

    fn allocate_request_id(&mut self) -> u64 {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.saturating_add(1);
        request_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use neo4r_db::{DatabaseConfig, Neo4rDatabaseHandle};
    use neo4r_server::{TcpBackend, TcpBackendConfig};
    use std::fs;
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn client_executes_query_and_decodes_rows() {
        let dir = temp_dir("client-sdk-query");
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
        let backend = TcpBackend::with_config(
            db,
            TcpBackendConfig {
                default_page_size: 2,
                ..TcpBackendConfig::default()
            },
        );
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            backend.handle_native_stream(stream).unwrap();
        });

        let mut client = Client::connect(address).unwrap();
        client.ping().unwrap();
        let mut params = QueryParams::new();
        params.insert("name".to_string(), Value::String("Alice".to_string()));
        let rows = client
            .execute_with_params("CREATE (n:Person {name: $name}) RETURN n.name", &params)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("n.name"),
            Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
        );

        let rows = client.query("MATCH (n:Person) RETURN n.name").unwrap();
        assert_eq!(rows.len(), 1);
        assert!(client
            .profile("MATCH (n:Person) RETURN n", &QueryParams::new())
            .unwrap()
            .contains("rows=1"));
        assert!(client
            .query_plan("MATCH (n:Person) RETURN n", &QueryParams::new())
            .unwrap()
            .contains("access="));
        assert!(client
            .cluster_status()
            .unwrap()
            .contains("routing_version="));
        client.close().unwrap();
        server.join().unwrap();
        let _ = fs::remove_dir_all(dir);
    }

    fn temp_dir(prefix: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        path.push(format!("neo4r-{prefix}-{}-{nanos}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        path
    }
}
