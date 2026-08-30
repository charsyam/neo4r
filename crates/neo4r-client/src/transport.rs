use crate::{ClientError, ClientResult};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig as RustlsClientConfig, ClientConnection, RootCertStore, StreamOwned};
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Once};
use std::thread;

pub trait ClientTransport: Read + Write + Send {}

impl<T> ClientTransport for T where T: Read + Write + Send {}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NativeTlsClientConfig {
    pub server_name: String,
    pub ca_cert_path: PathBuf,
    pub client_cert_path: Option<PathBuf>,
    pub client_key_path: Option<PathBuf>,
}

static RUSTLS_PROVIDER_INIT: Once = Once::new();

pub(crate) fn connect_with_retry(
    address: &SocketAddr,
    config: &crate::ClientConfig,
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

pub(crate) fn connect_tls_transport(
    address: &SocketAddr,
    config: &crate::ClientConfig,
    tls_config: &NativeTlsClientConfig,
) -> ClientResult<Box<dyn ClientTransport>> {
    install_rustls_provider();
    let tcp = connect_with_retry(address, config)?;
    tcp.set_read_timeout(config.read_timeout)?;
    tcp.set_write_timeout(config.write_timeout)?;
    let rustls_config = Arc::new(build_rustls_client_config(tls_config)?);
    let server_name = ServerName::try_from(tls_config.server_name.clone())
        .map_err(|err| ClientError::Protocol(format!("invalid TLS server name: {err}")))?;
    let connection = ClientConnection::new(rustls_config, server_name)
        .map_err(|err| ClientError::Protocol(format!("failed to create TLS client: {err}")))?;
    Ok(Box::new(StreamOwned::new(connection, tcp)))
}

fn build_rustls_client_config(config: &NativeTlsClientConfig) -> ClientResult<RustlsClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = RustlsClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(|err| ClientError::Protocol(format!("invalid TLS versions: {err}")))?;
    let roots = load_roots(&config.ca_cert_path)?;
    match (&config.client_cert_path, &config.client_key_path) {
        (Some(cert_path), Some(key_path)) => {
            let certs = load_certs(cert_path)?;
            let key = load_private_key(key_path)?;
            builder
                .with_root_certificates(roots)
                .with_client_auth_cert(certs, key)
                .map_err(|err| ClientError::Protocol(format!("invalid client TLS cert: {err}")))
        }
        (None, None) => Ok(builder.with_root_certificates(roots).with_no_client_auth()),
        _ => Err(ClientError::Protocol(
            "both client_cert_path and client_key_path are required for mTLS".to_string(),
        )),
    }
}

fn install_rustls_provider() {
    RUSTLS_PROVIDER_INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn load_certs(path: &Path) -> ClientResult<Vec<CertificateDer<'static>>> {
    let mut reader = BufReader::new(File::open(path)?);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(ClientError::Io)
}

fn load_private_key(path: &Path) -> ClientResult<PrivateKeyDer<'static>> {
    let mut reader = BufReader::new(File::open(path)?);
    rustls_pemfile::private_key(&mut reader)?
        .ok_or_else(|| ClientError::Protocol(format!("no private key found in {}", path.display())))
}

fn load_roots(path: &Path) -> ClientResult<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for cert in load_certs(path)? {
        roots
            .add(cert)
            .map_err(|err| ClientError::Protocol(format!("invalid CA certificate: {err}")))?;
    }
    Ok(roots)
}
