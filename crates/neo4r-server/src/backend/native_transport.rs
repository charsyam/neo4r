use super::*;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig, ServerConnection, StreamOwned};
use std::fs::File;
use std::sync::Once;

static RUSTLS_PROVIDER_INIT: Once = Once::new();

pub(crate) enum NativeStreamParts {
    Split {
        reader: Box<dyn Read + Send>,
        writer: Box<dyn Write + Send>,
    },
    Unified(Box<dyn NativeTransport>),
}

pub(crate) trait IntoNativeStreamParts {
    fn into_native_stream_parts(self) -> io::Result<NativeStreamParts>;
}

pub(crate) trait NativeTransport: Read + Write + Send {}

impl<T> NativeTransport for T where T: Read + Write + Send {}

pub(crate) struct PlainNativeStream {
    stream: TcpStream,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NativeTlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub client_ca_path: Option<PathBuf>,
    pub require_client_auth: bool,
}

#[derive(Clone)]
pub(crate) struct NativeTlsAcceptor {
    config: Arc<ServerConfig>,
}

pub(crate) struct TlsNativeStream {
    stream: StreamOwned<ServerConnection, TcpStream>,
}

impl TlsNativeStream {
    pub(crate) fn into_stream(self) -> StreamOwned<ServerConnection, TcpStream> {
        self.stream
    }
}

impl PlainNativeStream {
    pub(crate) fn new(stream: TcpStream) -> Self {
        Self { stream }
    }
}

impl IntoNativeStreamParts for PlainNativeStream {
    fn into_native_stream_parts(self) -> io::Result<NativeStreamParts> {
        Ok(NativeStreamParts::Split {
            reader: Box::new(self.stream.try_clone()?),
            writer: Box::new(self.stream),
        })
    }
}

impl NativeTlsAcceptor {
    pub(crate) fn from_config(config: &NativeTlsConfig) -> io::Result<Self> {
        install_rustls_provider();
        let certs = load_certs(&config.cert_path)?;
        let key = load_private_key(&config.key_path)?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
        let server_config = if config.require_client_auth {
            let ca_path = config.client_ca_path.as_ref().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "native TLS client CA path is required when client auth is enabled",
                )
            })?;
            let roots = load_roots(ca_path)?;
            let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)
        } else {
            builder.with_no_client_auth().with_single_cert(certs, key)
        }
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
        Ok(Self {
            config: Arc::new(server_config),
        })
    }

    pub(crate) fn accept(&self, stream: TcpStream) -> io::Result<TlsNativeStream> {
        let connection = ServerConnection::new(self.config.clone())
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
        Ok(TlsNativeStream {
            stream: StreamOwned::new(connection, stream),
        })
    }
}

impl IntoNativeStreamParts for TlsNativeStream {
    fn into_native_stream_parts(self) -> io::Result<NativeStreamParts> {
        Ok(NativeStreamParts::Unified(Box::new(self.stream)))
    }
}

fn install_rustls_provider() {
    RUSTLS_PROVIDER_INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let mut reader = BufReader::new(File::open(path)?);
    rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()
}

fn load_private_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    let mut reader = BufReader::new(File::open(path)?);
    rustls_pemfile::private_key(&mut reader)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("no private key found in {}", path.display()),
        )
    })
}

fn load_roots(path: &Path) -> io::Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for cert in load_certs(path)? {
        roots
            .add(cert)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    }
    Ok(roots)
}
