//! Blocking Rust client SDK for neo4r native protocol.

pub use neo4r_core::{Node, Relationship, Value};
use neo4r_protocol::{
    encode_query_payload, parse_result_page_response, parse_result_start_response,
    parse_rows_response, read_frame, response_field, write_frame, NativeFrame, NativeMessageType,
};
pub use neo4r_query::{QueryParams, QueryRow, QueryValue};
use std::io;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub enum ClientError {
    Io(io::Error),
    Protocol(String),
    Redirect(RedirectInfo),
    Server(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(formatter, "{err}"),
            Self::Protocol(message) => write!(formatter, "protocol error: {message}"),
            Self::Redirect(redirect) => write!(formatter, "redirect: {redirect:?}"),
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedirectInfo {
    pub kind: String,
    pub shard_id: u64,
    pub leader: Option<u64>,
    pub address: Option<String>,
    pub routing_version: u64,
    pub ownership_epoch: u64,
    pub database: String,
    pub retryable: bool,
}

#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub connect_timeout: Option<Duration>,
    pub read_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
    pub retry_attempts: usize,
    pub retry_backoff: Duration,
    pub redirect_max_hops: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Some(Duration::from_secs(3)),
            read_timeout: Some(Duration::from_secs(30)),
            write_timeout: Some(Duration::from_secs(30)),
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(50),
            redirect_max_hops: 4,
        }
    }
}

pub struct Client {
    stream: TcpStream,
    next_request_id: u64,
    config: ClientConfig,
    topology_cache: TopologyCache,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TopologyCache {
    pub database: String,
    pub routing_version: u64,
    pub ownership_epoch: u64,
    pub last_address: Option<String>,
    pub expires_at: Option<Instant>,
}

impl TopologyCache {
    pub fn is_fresh(&self) -> bool {
        self.expires_at
            .map(|expires_at| expires_at > Instant::now())
            .unwrap_or(self.last_address.is_some())
    }

    pub fn routable_address(&self) -> Option<&str> {
        if self.is_fresh() {
            self.last_address.as_deref()
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
pub struct HttpAdminClient {
    host: String,
    port: u16,
    token: String,
}

impl HttpAdminClient {
    pub fn connect(host: impl Into<String>, port: u16, token: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            port,
            token: token.into(),
        }
    }

    pub fn create_database(&self, name: &str) -> ClientResult<String> {
        self.request_json(
            "POST",
            "/api/admin/databases",
            &format!(r#"{{"name":"{}"}}"#, json_escape(name)),
            None,
        )
    }

    pub fn select_database(&self, name: &str) -> ClientResult<String> {
        self.request_json(
            "POST",
            "/api/use-database",
            &format!(r#"{{"database":"{}"}}"#, json_escape(name)),
            None,
        )
    }

    pub fn list_databases(&self) -> ClientResult<String> {
        self.request_json("GET", "/api/admin/databases", "", None)
    }

    pub fn invoke_token(
        &self,
        name: &str,
        token_id: &str,
        token: &str,
        role: &str,
        expired_at: &str,
        database: Option<&str>,
        database_role: &str,
    ) -> ClientResult<String> {
        let mut payload = format!(
            r#"{{"name":"{}","token_id":"{}","role":"{}","token":"{}","expired_at":"{}""#,
            json_escape(name),
            json_escape(token_id),
            json_escape(role),
            json_escape(token),
            json_escape(expired_at)
        );
        if let Some(database) = database {
            payload.push_str(&format!(
                r#","database":"{}","database_role":"{}""#,
                json_escape(database),
                json_escape(database_role)
            ));
        }
        payload.push('}');
        self.request_json("POST", "/api/admin/invoke-token", &payload, None)
    }

    pub fn revoke_token(&self, name: &str, token_id: &str) -> ClientResult<String> {
        self.request_json(
            "POST",
            "/api/admin/revoke-token",
            &format!(
                r#"{{"name":"{}","token_id":"{}"}}"#,
                json_escape(name),
                json_escape(token_id)
            ),
            None,
        )
    }

    pub fn query(
        &self,
        query: &str,
        params_json: &str,
        database: Option<&str>,
        token: Option<&str>,
    ) -> ClientResult<String> {
        self.query_with_options(query, params_json, database, token, None, None)
    }

    pub fn query_with_options(
        &self,
        query: &str,
        params_json: &str,
        database: Option<&str>,
        token: Option<&str>,
        read_consistency: Option<&str>,
        max_staleness_ms: Option<u64>,
    ) -> ClientResult<String> {
        let mut payload = format!(
            r#"{{"query":"{}","params":{}"#,
            json_escape(query),
            params_json
        );
        if let Some(database) = database {
            payload.push_str(&format!(r#","database":"{}""#, json_escape(database)));
        }
        if let Some(read_consistency) = read_consistency {
            payload.push_str(&format!(
                r#","read_consistency":"{}""#,
                json_escape(read_consistency)
            ));
        }
        if let Some(max_staleness_ms) = max_staleness_ms {
            payload.push_str(&format!(r#","max_staleness_ms":{max_staleness_ms}"#));
        }
        payload.push('}');
        self.request_json("POST", "/api/query", &payload, token)
    }

    pub fn metrics(&self) -> ClientResult<String> {
        self.request_json("GET", "/api/metrics", "", None)
    }

    pub fn routing_table(&self) -> ClientResult<String> {
        self.request_json("GET", "/api/cluster/routing-table", "", None)
    }

    pub fn cluster_registry(&self) -> ClientResult<String> {
        self.request_json("GET", "/api/cluster/registry", "", None)
    }

    pub fn capabilities(&self) -> ClientResult<String> {
        self.request_json("GET", "/api/capabilities", "", None)
    }

    fn request_json(
        &self,
        method: &str,
        path: &str,
        body: &str,
        token: Option<&str>,
    ) -> ClientResult<String> {
        let mut stream = TcpStream::connect((self.host.as_str(), self.port))?;
        let token = token.unwrap_or(&self.token);
        let request = if body.is_empty() {
            format!(
                "{method} {path} HTTP/1.1\r\nHost: {}:{}\r\nAuthorization: Bearer {token}\r\nConnection: close\r\n\r\n",
                self.host, self.port
            )
        } else {
            format!(
                "{method} {path} HTTP/1.1\r\nHost: {}:{}\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                self.host,
                self.port,
                body.len()
            )
        };
        stream.write_all(request.as_bytes())?;
        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        let Some((head, body)) = response.split_once("\r\n\r\n") else {
            return Err(ClientError::Protocol("invalid HTTP response".to_string()));
        };
        if !head.starts_with("HTTP/1.1 2") && !head.starts_with("HTTP/1.0 2") {
            return Err(ClientError::Server(body.to_string()));
        }
        Ok(body.to_string())
    }
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
        let stream = connect_with_retry(&address, &config)?;
        stream.set_read_timeout(config.read_timeout)?;
        stream.set_write_timeout(config.write_timeout)?;
        Ok(Self {
            stream,
            next_request_id: 1,
            config,
            topology_cache: TopologyCache::default(),
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

    pub fn routing_table(&mut self) -> ClientResult<String> {
        let response = self.command("ROUTING_TABLE")?;
        response_field(&response, "ROUTING_TABLE").map_err(ClientError::Protocol)
    }

    pub fn cluster_registry(&mut self) -> ClientResult<String> {
        let response = self.command("CLUSTER_REGISTRY")?;
        let registry =
            response_field(&response, "CLUSTER_REGISTRY").map_err(ClientError::Protocol)?;
        self.update_topology_cache_from_registry(&registry);
        Ok(registry)
    }

    pub fn capabilities(&mut self) -> ClientResult<String> {
        let response = self.command("CAPABILITIES")?;
        response_field(&response, "CAPABILITIES").map_err(ClientError::Protocol)
    }

    pub fn topology_cache(&self) -> &TopologyCache {
        &self.topology_cache
    }

    pub fn connect_to_cached_target(&mut self) -> ClientResult<bool> {
        let Some(address) = self.topology_cache.routable_address() else {
            return Ok(false);
        };
        let stream = TcpStream::connect(address)?;
        stream.set_read_timeout(self.config.read_timeout)?;
        stream.set_write_timeout(self.config.write_timeout)?;
        self.stream = stream;
        Ok(true)
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
        let mut visited = Vec::new();
        for hop in 0..=self.config.redirect_max_hops {
            match self.request_once(message_type, payload.clone()) {
                Err(ClientError::Redirect(redirect))
                    if redirect.retryable && redirect.address.is_some() =>
                {
                    let address = redirect.address.as_deref().unwrap().to_string();
                    if visited.contains(&address) {
                        return Err(ClientError::Protocol(format!(
                            "redirect loop detected after visiting {}",
                            visited.join(" -> ")
                        )));
                    }
                    if hop == self.config.redirect_max_hops {
                        return Err(ClientError::Protocol(format!(
                            "redirect max hops exceeded at {address}"
                        )));
                    }
                    self.update_topology_cache_from_redirect(&redirect);
                    visited.push(address.clone());
                    let stream = TcpStream::connect(address)?;
                    stream.set_read_timeout(self.config.read_timeout)?;
                    stream.set_write_timeout(self.config.write_timeout)?;
                    self.stream = stream;
                }
                result => return result,
            }
        }
        Err(ClientError::Protocol(
            "redirect retry exhausted".to_string(),
        ))
    }

    fn request_once(
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
            NativeMessageType::Response if payload.starts_with("ERR\t") => {
                Err(parse_redirect_response(&payload)
                    .map(ClientError::Redirect)
                    .unwrap_or(ClientError::Server(payload)))
            }
            NativeMessageType::Response => Ok(payload),
            NativeMessageType::Error => Err(parse_redirect_response(&payload)
                .map(ClientError::Redirect)
                .unwrap_or(ClientError::Server(payload))),
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

    fn update_topology_cache_from_redirect(&mut self, redirect: &RedirectInfo) {
        self.topology_cache.database = redirect.database.clone();
        self.topology_cache.routing_version = redirect.routing_version;
        self.topology_cache.ownership_epoch = redirect.ownership_epoch;
        self.topology_cache.last_address = redirect.address.clone();
        self.topology_cache.expires_at = None;
    }

    fn update_topology_cache_from_registry(&mut self, registry: &str) {
        let mut ttl_ms = None;
        for part in registry.split_whitespace() {
            let Some((key, value)) = part.split_once('=') else {
                continue;
            };
            match key {
                "database" => self.topology_cache.database = value.to_string(),
                "routing_version" => {
                    self.topology_cache.routing_version = value.parse().unwrap_or_default()
                }
                "ownership_epoch" => {
                    self.topology_cache.ownership_epoch = value.parse().unwrap_or_default()
                }
                "ttl_ms" => ttl_ms = value.parse::<u64>().ok(),
                _ => {}
            }
        }
        if let Some(ttl_ms) = ttl_ms {
            self.topology_cache.expires_at = Some(Instant::now() + Duration::from_millis(ttl_ms));
        }
    }
}

fn parse_redirect_response(input: &str) -> Option<RedirectInfo> {
    let mut parts = input.split('\t');
    if parts.next()? != "ERR" {
        return None;
    }
    let kind = parts.next()?;
    if !matches!(
        kind,
        "MOVED" | "NOT_LEADER" | "STALE_ROUTING" | "STALE_EPOCH"
    ) {
        return None;
    }
    let mut redirect = RedirectInfo {
        kind: kind.to_string(),
        shard_id: 0,
        leader: None,
        address: None,
        routing_version: 0,
        ownership_epoch: 0,
        database: "default".to_string(),
        retryable: false,
    };
    for part in parts {
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };
        match key {
            "shard" => redirect.shard_id = value.parse().ok()?,
            "leader" if value != "none" => redirect.leader = value.parse().ok(),
            "address" if value != "missing" => redirect.address = Some(value.to_string()),
            "routing_version" => redirect.routing_version = value.parse().ok()?,
            "ownership_epoch" => redirect.ownership_epoch = value.parse().ok()?,
            "database" => redirect.database = value.to_string(),
            "retryable" => redirect.retryable = value == "true",
            _ => {}
        }
    }
    if redirect.ownership_epoch == 0 {
        redirect.ownership_epoch = redirect.routing_version;
    }
    Some(redirect)
}

fn connect_with_retry(
    address: &std::net::SocketAddr,
    config: &ClientConfig,
) -> ClientResult<TcpStream> {
    let mut attempt = 0;
    loop {
        let result = match config.connect_timeout {
            Some(timeout) => TcpStream::connect_timeout(address, timeout),
            None => TcpStream::connect(address),
        };
        match result {
            Ok(stream) => return Ok(stream),
            Err(err) if attempt < config.retry_attempts => {
                attempt += 1;
                thread::sleep(config.retry_backoff);
                if attempt > config.retry_attempts {
                    return Err(ClientError::Io(err));
                }
            }
            Err(err) => return Err(ClientError::Io(err)),
        }
    }
}

fn json_escape(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            ch if ch.is_control() => escaped.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => escaped.push(ch),
        }
    }
    escaped
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
        assert!(client
            .capabilities()
            .unwrap()
            .contains("ownership_epoch=true"));
        client.close().unwrap();
        server.join().unwrap();
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn client_parses_redirect_response() {
        let redirect = parse_redirect_response(
            "ERR\tMOVED\tshard=3\tleader=2\taddress=127.0.0.1:17688\trouting_version=17\tdatabase=tenant_a\tretryable=true",
        )
        .unwrap();
        assert_eq!(redirect.kind, "MOVED");
        assert_eq!(redirect.shard_id, 3);
        assert_eq!(redirect.leader, Some(2));
        assert_eq!(redirect.address.as_deref(), Some("127.0.0.1:17688"));
        assert_eq!(redirect.routing_version, 17);
        assert_eq!(redirect.ownership_epoch, 17);
        assert_eq!(redirect.database, "tenant_a");
        assert!(redirect.retryable);
    }

    #[test]
    fn client_parses_typed_stale_epoch_response() {
        let redirect = parse_redirect_response(
            "ERR\tSTALE_EPOCH\ttx_epoch=1\tcurrent_epoch=2\trouting_version=2\townership_epoch=2\tretryable=true",
        )
        .unwrap();
        assert_eq!(redirect.kind, "STALE_EPOCH");
        assert_eq!(redirect.routing_version, 2);
        assert_eq!(redirect.ownership_epoch, 2);
        assert!(redirect.retryable);
    }

    #[test]
    fn client_follows_redirect_once() {
        let target_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let target_addr = target_listener.local_addr().unwrap();
        let target_server = thread::spawn(move || {
            let (mut stream, _) = target_listener.accept().unwrap();
            let frame = read_frame(&mut stream).unwrap().unwrap();
            assert_eq!(frame.message_type, NativeMessageType::Ping);
            write_frame(
                &mut stream,
                &NativeFrame::new(
                    NativeMessageType::Response,
                    frame.request_id,
                    b"OK\tPONG".to_vec(),
                ),
            )
            .unwrap();
        });

        let redirect_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let redirect_addr = redirect_listener.local_addr().unwrap();
        let redirect_server = thread::spawn(move || {
            let (mut stream, _) = redirect_listener.accept().unwrap();
            let frame = read_frame(&mut stream).unwrap().unwrap();
            write_frame(
                &mut stream,
                &NativeFrame::new(
                    NativeMessageType::Error,
                    frame.request_id,
                    format!(
                        "ERR\tMOVED\tshard=0\tleader=2\taddress={target_addr}\trouting_version=1\tdatabase=default\tretryable=true"
                    )
                    .into_bytes(),
                ),
            )
            .unwrap();
        });

        let mut client = Client::connect(redirect_addr).unwrap();
        client.ping().unwrap();
        assert_eq!(client.topology_cache().routing_version, 1);
        assert_eq!(client.topology_cache().ownership_epoch, 1);

        redirect_server.join().unwrap();
        target_server.join().unwrap();
    }

    #[test]
    fn client_rejects_redirect_loop() {
        let loop_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let loop_addr = loop_listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            for _ in 0..2 {
                let (mut stream, _) = loop_listener.accept().unwrap();
                let frame = read_frame(&mut stream).unwrap().unwrap();
                write_frame(
                    &mut stream,
                    &NativeFrame::new(
                        NativeMessageType::Error,
                        frame.request_id,
                        format!(
                            "ERR\tMOVED\tshard=0\tleader=1\taddress={loop_addr}\trouting_version=2\townership_epoch=2\tdatabase=default\tretryable=true"
                        )
                        .into_bytes(),
                    ),
                )
                .unwrap();
            }
        });

        let mut client = Client::connect(loop_addr).unwrap();
        let err = client.ping().unwrap_err();
        assert!(format!("{err}").contains("redirect loop detected"));
        server.join().unwrap();
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
