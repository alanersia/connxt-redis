use crate::{
    client::ClientConfig,
    error::{Error, Result},
    protocol::{
        Command,
        codec::{Decoder, Limits},
    },
    types::{FromRedis, RedisValue},
};
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
trait AsyncStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> AsyncStream for T {}
type BoxStream = Pin<Box<dyn AsyncStream>>;
#[derive(Clone)]
pub struct AsyncClient {
    cfg: ClientConfig,
}
pub struct AsyncPool {
    client: AsyncClient,
    permits: Arc<Semaphore>,
}
pub struct AsyncPermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
    client: AsyncClient,
}
impl AsyncPool {
    pub fn new(cfg: ClientConfig, max: usize) -> Self {
        Self {
            client: AsyncClient::open(cfg),
            permits: Arc::new(Semaphore::new(max.max(1))),
        }
    }
    pub async fn get(&self) -> Result<AsyncPermit> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::PoolExhausted)?;
        Ok(AsyncPermit {
            _permit: permit,
            client: self.client.clone(),
        })
    }
}
impl AsyncPermit {
    pub async fn execute<T: FromRedis>(&self, c: Command) -> Result<T> {
        self.client.execute(c).await
    }
}
impl AsyncClient {
    pub fn open(cfg: ClientConfig) -> Self {
        Self { cfg }
    }
    pub async fn execute<T: FromRedis>(&self, c: Command) -> Result<T> {
        let mut stream = connect_stream(&self.cfg).await?;
        let mut decoder = Decoder::new(Limits::default());
        let mut setup = Vec::new();
        if let Some(password) = &self.cfg.options.password {
            setup.push(if let Some(user) = &self.cfg.options.username {
                Command::new("AUTH")
                    .arg(user.as_str())
                    .arg(password.as_str())
            } else {
                Command::new("AUTH").arg(password.as_str())
            });
        }
        if self.cfg.options.database != 0 {
            setup.push(Command::new("SELECT").arg(self.cfg.options.database as i64));
        }
        setup.push(c);
        let mut result = RedisValue::Null;
        for command in setup {
            stream
                .write_all(&crate::protocol::encode(&command.0))
                .await?;
            result = read_value(
                &mut stream,
                &mut decoder,
                self.cfg
                    .options
                    .command_timeout
                    .unwrap_or(self.cfg.options.connect_timeout),
            )
            .await?;
        }
        T::from_redis(result)
    }
    pub fn config(&self) -> &ClientConfig {
        &self.cfg
    }
}
async fn read_value(
    stream: &mut BoxStream,
    decoder: &mut Decoder,
    timeout: std::time::Duration,
) -> Result<RedisValue> {
    loop {
        let mut buf = [0u8; 8192];
        let n = tokio::time::timeout(timeout, stream.read(&mut buf))
            .await
            .map_err(|_| Error::Timeout)??;
        if n == 0 {
            return Err(Error::Protocol("connection closed".into()));
        }
        if let Some(v) = decoder.push(&buf[..n])? {
            if let RedisValue::Error(e) = &v {
                return Err(Error::Server(e.clone()));
            }
            return Ok(v);
        }
    }
}

async fn connect_stream(cfg: &ClientConfig) -> Result<BoxStream> {
    let addr = format!("{}:{}", cfg.url.host, cfg.url.port);
    let tcp = tokio::time::timeout(cfg.options.connect_timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| Error::Timeout)??;
    if !cfg.url.tls {
        return Ok(Box::pin(tcp));
    }
    #[cfg(feature = "tls")]
    {
        use rustls::{ClientConfig as RustlsConfig, RootCertStore, pki_types::ServerName};
        let mut roots = RootCertStore::empty();
        let native = rustls_native_certs::load_native_certs();
        for cert in native.certs {
            roots
                .add(cert)
                .map_err(|_| Error::Protocol("invalid native CA certificate".into()))?;
        }
        let config = RustlsConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let name = ServerName::try_from(cfg.url.host.clone())
            .map_err(|_| Error::InvalidUrl("invalid TLS hostname".into()))?;
        let stream = tokio_rustls::TlsConnector::from(Arc::new(config))
            .connect(name, tcp)
            .await
            .map_err(|e| Error::Protocol(format!("TLS connection failed: {e}")))?;
        Ok(Box::pin(stream))
    }
    #[cfg(not(feature = "tls"))]
    {
        let _ = tcp;
        Err(Error::Unsupported("rediss:// requires the tls feature"))
    }
}
