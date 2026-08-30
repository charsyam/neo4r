use super::*;
mod backup;
mod json;
pub(crate) use backup::*;
pub(crate) use json::*;

pub(crate) struct HttpRequest {
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) query: HashMap<String, String>,
    pub(crate) headers: HashMap<String, String>,
    pub(crate) body: String,
}

impl HttpRequest {
    pub(crate) fn query_value(&self, key: &str) -> Option<String> {
        self.query.get(key).cloned()
    }

    pub(crate) fn header(&self, key: &str) -> Option<&str> {
        self.headers
            .get(&key.to_ascii_lowercase())
            .map(String::as_str)
    }
}

pub(crate) struct HttpResponse {
    pub(crate) status: u16,
    pub(crate) reason: &'static str,
    pub(crate) content_type: &'static str,
    pub(crate) body: String,
}

impl HttpResponse {
    pub(crate) fn html(body: &str) -> Self {
        Self {
            status: 200,
            reason: "OK",
            content_type: "text/html; charset=utf-8",
            body: body.to_string(),
        }
    }

    pub(crate) fn json(body: String) -> Self {
        Self::json_status(200, body)
    }

    pub(crate) fn json_status(status: u16, body: String) -> Self {
        let reason = match status {
            200 => "OK",
            400 => "Bad Request",
            401 => "Unauthorized",
            403 => "Forbidden",
            404 => "Not Found",
            503 => "Service Unavailable",
            500 => "Internal Server Error",
            _ => "OK",
        };
        Self {
            status,
            reason,
            content_type: "application/json; charset=utf-8",
            body,
        }
    }

    pub(crate) fn text(body: String) -> Self {
        Self {
            status: 200,
            reason: "OK",
            content_type: "text/plain; version=0.0.4; charset=utf-8",
            body,
        }
    }
}

pub(crate) fn read_http_request(stream: impl Read) -> io::Result<HttpRequest> {
    let mut reader = BufReader::new(stream);
    let mut request_line = String::new();
    if reader.read_line(&mut request_line)? == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "empty HTTP request",
        ));
    }
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or("").to_string();
    let target = request_parts.next().unwrap_or("/");
    let (path, query) = parse_http_target(target);

    let mut content_length = 0usize;
    let mut headers = HashMap::new();
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let header = line.trim_end_matches(['\r', '\n']);
        if header.is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse::<usize>().unwrap_or(0);
            }
        }
    }

    let mut body = vec![0_u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body)?;
    }

    Ok(HttpRequest {
        method,
        path,
        query,
        headers,
        body: String::from_utf8_lossy(&body).into_owned(),
    })
}

pub(crate) fn write_http_response(
    mut stream: impl Write,
    response: HttpResponse,
) -> io::Result<()> {
    let body = response.body.as_bytes();
    write!(
        stream,
        "HTTP/1.1 {} {}\r\ncontent-type: {}\r\ncontent-length: {}\r\ncache-control: no-store\r\nconnection: close\r\n\r\n",
        response.status,
        response.reason,
        response.content_type,
        body.len()
    )?;
    stream.write_all(body)?;
    stream.flush()
}

pub(crate) fn parse_http_target(target: &str) -> (String, HashMap<String, String>) {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let mut values = HashMap::new();
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        values.insert(percent_decode(key), percent_decode(value));
    }
    (path.to_string(), values)
}

pub(crate) fn percent_decode(input: &str) -> String {
    let mut output = Vec::with_capacity(input.len());
    let mut bytes = input.as_bytes().iter().copied();
    while let Some(byte) = bytes.next() {
        match byte {
            b'+' => output.push(b' '),
            b'%' => {
                let high = bytes.next();
                let low = bytes.next();
                if let (Some(high), Some(low)) = (high, low) {
                    if let (Some(high), Some(low)) = (hex_value(high), hex_value(low)) {
                        output.push((high << 4) | low);
                    }
                }
            }
            byte => output.push(byte),
        }
    }
    String::from_utf8_lossy(&output).into_owned()
}

pub(crate) fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
