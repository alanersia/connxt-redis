use crate::error::{Error, Result};
use crate::{
    client::ConnectionOptions,
    protocol::{
        Command,
        codec::{Decoder, Limits},
    },
    types::RedisValue,
};
use rustls::{
    ClientConfig, ClientConnection, RootCertStore, StreamOwned,
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName},
};
use std::{
    io::{Read, Write},
    net::{TcpStream, ToSocketAddrs},
    sync::Arc,
};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsMode {
    Disabled,
    Preferred,
    Required,
}
pub struct TlsOptions {
    pub mode: TlsMode,
    pub ca_certs_der: Vec<Vec<u8>>,
    pub client_cert_chain_der: Vec<Vec<u8>>,
    pub client_key_pkcs8_der: Option<Vec<u8>>,
}
impl Default for TlsOptions {
    fn default() -> Self {
        Self {
            mode: TlsMode::Required,
            ca_certs_der: Vec::new(),
            client_cert_chain_der: Vec::new(),
            client_key_pkcs8_der: None,
        }
    }
}
pub fn required_cleartext_error(mode: TlsMode, tls: bool) -> Result<()> {
    if mode == TlsMode::Required && !tls {
        Err(Error::InvalidUrl(
            "TLS is required; refusing cleartext credentials".into(),
        ))
    } else {
        Ok(())
    }
}

pub struct TlsConnection {
    stream: StreamOwned<ClientConnection, TcpStream>,
    decoder: Decoder,
}
impl TlsConnection {
    pub fn connect(host: &str, port: u16, o: &ConnectionOptions) -> Result<Self> {
        Self::connect_with_options(host, port, o, &TlsOptions::default())
    }
    pub fn connect_with_options(
        host: &str,
        port: u16,
        o: &ConnectionOptions,
        tls: &TlsOptions,
    ) -> Result<Self> {
        let mut roots = RootCertStore::empty();
        let certs = if tls.ca_certs_der.is_empty() {
            rustls_native_certs::load_native_certs().certs
        } else {
            tls.ca_certs_der
                .iter()
                .cloned()
                .map(CertificateDer::from)
                .collect()
        };
        for cert in certs {
            roots
                .add(cert)
                .map_err(|_| Error::InvalidUrl("invalid native CA certificate".into()))?;
        }
        if roots.is_empty() {
            return Err(Error::InvalidUrl("no native CA roots found".into()));
        }
        let config = if let Some(key) = &tls.client_key_pkcs8_der {
            let chain = tls
                .client_cert_chain_der
                .iter()
                .cloned()
                .map(CertificateDer::from)
                .collect();
            ClientConfig::builder()
                .with_root_certificates(roots)
                .with_client_auth_cert(
                    chain,
                    PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.clone())),
                )
                .map_err(|e| Error::Protocol(format!("TLS client identity failed: {e}")))?
        } else {
            ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth()
        };
        let name = ServerName::try_from(host.to_string())
            .map_err(|_| Error::InvalidUrl("invalid TLS hostname".into()))?;
        let addr = (host, port)
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| Error::InvalidUrl("host has no address".into()))?;
        let tcp = TcpStream::connect_timeout(&addr, o.connect_timeout)?;
        tcp.set_read_timeout(o.command_timeout)?;
        tcp.set_write_timeout(o.command_timeout)?;
        Ok(Self {
            stream: StreamOwned::new(
                ClientConnection::new(Arc::new(config), name)
                    .map_err(|e| Error::Protocol(format!("TLS setup failed: {e}")))?,
                tcp,
            ),
            decoder: Decoder::new(Limits::default()),
        })
    }
    pub fn execute(&mut self, c: Command) -> Result<RedisValue> {
        self.stream.write_all(&crate::protocol::encode(&c.0))?;
        loop {
            let mut b = [0u8; 8192];
            let n = self.stream.read(&mut b)?;
            if n == 0 {
                return Err(Error::Protocol("connection closed".into()));
            }
            if let Some(v) = self.decoder.push(&b[..n])? {
                if let RedisValue::Error(e) = &v {
                    return Err(Error::Server(e.clone()));
                }
                return Ok(v);
            }
        }
    }
}
