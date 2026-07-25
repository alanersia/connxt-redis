use crate::ops::{Backoff, CircuitBreaker, Metrics, elapsed_micros};
use crate::{
    connection::Connection,
    error::{Error, Result},
    protocol::Command,
    types::{FromRedis, RedisValue, ToRedis},
};
use std::sync::Arc;
use std::{net::TcpStream, time::Duration};

#[derive(Debug, Clone)]
pub struct ConnectionOptions {
    pub connect_timeout: Duration,
    pub command_timeout: Option<Duration>,
    pub database: u32,
    pub username: Option<String>,
    pub password: Option<String>,
    pub keepalive: Option<Duration>,
}
impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            command_timeout: Some(Duration::from_secs(5)),
            database: 0,
            username: None,
            password: None,
            keepalive: Some(Duration::from_secs(30)),
        }
    }
}
#[derive(Debug, Clone)]
pub struct UrlConfig {
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub options: ConnectionOptions,
}
impl UrlConfig {
    pub fn parse(input: &str) -> Result<Self> {
        let (tls, rest) = if let Some(x) = input.strip_prefix("redis://") {
            (false, x)
        } else if let Some(x) = input.strip_prefix("rediss://") {
            (true, x)
        } else {
            return Err(Error::InvalidUrl(
                "scheme must be redis:// or rediss://".into(),
            ));
        };
        let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
        let (userpass, hostport) = authority
            .rsplit_once('@')
            .map_or((None, authority), |x| (Some(x.0), x.1));
        let (username, password) = userpass
            .map(|x| {
                let (u, p) = x.split_once(':').unwrap_or(("", x));
                (
                    if u.is_empty() {
                        None
                    } else {
                        Some(decode_component(u))
                    },
                    Some(decode_component(p)),
                )
            })
            .unwrap_or((None, None));
        let (host, port) = if let Some((h, p)) = hostport.rsplit_once(':') {
            (
                h.to_string(),
                p.parse()
                    .map_err(|_| Error::InvalidUrl("bad port".into()))?,
            )
        } else {
            (hostport.to_string(), 6379)
        };
        if host.is_empty() {
            return Err(Error::InvalidUrl("host is empty".into()));
        }
        let (db_text, query) = path.split_once('?').unwrap_or((path, ""));
        let db = db_text
            .parse()
            .map_err(|_| Error::InvalidUrl("bad database".into()))?;
        let mut options = ConnectionOptions {
            database: db,
            username,
            password,
            ..Default::default()
        };
        for item in query.split('&').filter(|x| !x.is_empty()) {
            let (key, value) = item.split_once('=').unwrap_or((item, ""));
            match key {
                "connect_timeout" => {
                    options.connect_timeout = Duration::from_millis(
                        value
                            .parse()
                            .map_err(|_| Error::InvalidUrl("bad connect_timeout".into()))?,
                    )
                }
                "timeout" | "command_timeout" => {
                    options.command_timeout = Some(Duration::from_millis(
                        value
                            .parse()
                            .map_err(|_| Error::InvalidUrl("bad timeout".into()))?,
                    ))
                }
                _ => {}
            }
        }
        Ok(Self {
            host,
            port,
            tls,
            options,
        })
    }
}

fn decode_component(input: &str) -> String {
    let mut out = Vec::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(h), Some(l)) = (hex(bytes[i + 1]), hex(bytes[i + 2]))
        {
            out.push(h * 16 + l);
            i += 3;
            continue;
        }
        out.push(if bytes[i] == b'+' { b' ' } else { bytes[i] });
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}
fn hex(v: u8) -> Option<u8> {
    match v {
        b'0'..=b'9' => Some(v - b'0'),
        b'a'..=b'f' => Some(v - b'a' + 10),
        b'A'..=b'F' => Some(v - b'A' + 10),
        _ => None,
    }
}
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub url: UrlConfig,
    pub options: ConnectionOptions,
    pub metrics: Arc<Metrics>,
}
impl ClientConfig {
    pub fn from_url(s: &str) -> Result<Self> {
        let u = UrlConfig::parse(s)?;
        Ok(Self {
            options: u.options.clone(),
            url: u,
            metrics: Arc::new(Metrics::default()),
        })
    }
}
pub struct Client {
    cfg: ClientConfig,
}
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_attempts: usize,
    pub delay: Duration,
}
impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            delay: Duration::ZERO,
        }
    }
}
pub struct Pipeline {
    client: ClientConfig,
    commands: Vec<Command>,
}
pub struct Transaction {
    client: ClientConfig,
    watched: Vec<Vec<u8>>,
    commands: Vec<Command>,
}
impl Client {
    pub fn open(cfg: ClientConfig) -> Result<Self> {
        Ok(Self { cfg })
    }
    pub fn connect(&self) -> Result<Connection> {
        if self.cfg.url.tls {
            return Err(Error::Unsupported(
                "rediss:// requires the tls feature; cleartext fallback is disabled",
            ));
        }
        Connection::connect(&self.cfg.url.host, self.cfg.url.port, &self.cfg.url.options)
    }
    pub fn execute<T: FromRedis>(&self, c: Command) -> Result<T> {
        let started = std::time::Instant::now();
        self.cfg
            .metrics
            .commands
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut x = self.connect()?;
        let result = x.execute(c);
        self.cfg.metrics.total_latency_micros.fetch_add(
            elapsed_micros(started),
            std::sync::atomic::Ordering::Relaxed,
        );
        if result.is_err() {
            self.cfg
                .metrics
                .errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        T::from_redis(result?)
    }
    pub fn command(&self, name: impl AsRef<[u8]>) -> Command {
        Command::new(name)
    }
    pub fn ping(&self) -> Result<String> {
        self.execute(Command::new("PING"))
    }
    pub fn hello(&self, version: u8) -> Result<RedisValue> {
        self.execute(Command::new("HELLO").arg(version as i64))
    }
    pub fn set(&self, key: impl ToRedis, val: impl ToRedis) -> Result<String> {
        self.execute(Command::new("SET").arg(key).arg(val))
    }
    pub fn get(&self, key: impl ToRedis) -> Result<Option<Vec<u8>>> {
        self.execute(Command::new("GET").arg(key))
    }
    #[cfg(feature = "tls")]
    pub fn connect_tls(&self) -> Result<crate::tls::TlsConnection> {
        if !self.cfg.url.tls {
            return Err(Error::InvalidUrl("connect_tls requires rediss://".into()));
        }
        crate::tls::TlsConnection::connect(&self.cfg.url.host, self.cfg.url.port, &self.cfg.options)
    }
    pub fn pipeline(&self) -> Pipeline {
        Pipeline {
            client: self.cfg.clone(),
            commands: Vec::new(),
        }
    }
    pub fn transaction(&self) -> Transaction {
        Transaction {
            client: self.cfg.clone(),
            watched: Vec::new(),
            commands: Vec::new(),
        }
    }
    pub fn publish(&self, channel: impl ToRedis, message: impl ToRedis) -> Result<i64> {
        self.execute(Command::new("PUBLISH").arg(channel).arg(message))
    }
    pub fn scan(
        &self,
        cursor: u64,
        pattern: Option<&str>,
        count: Option<usize>,
    ) -> Result<RedisValue> {
        let mut c = Command::new("SCAN").arg(cursor as i64);
        if let Some(p) = pattern {
            c = c.arg("MATCH").arg(p);
        }
        if let Some(n) = count {
            c = c.arg("COUNT").arg(n);
        }
        self.execute(c)
    }
    pub fn eval(&self, script: impl ToRedis, keys: &[&str], args: &[&str]) -> Result<RedisValue> {
        let c = Command::new("EVAL").arg(script).arg(keys.len());
        let c = c.args(keys.iter().copied()).args(args.iter().copied());
        self.execute(c)
    }
    pub fn xadd(
        &self,
        stream: impl ToRedis,
        id: impl ToRedis,
        fields: &[(&str, &str)],
    ) -> Result<String> {
        let mut c = Command::new("XADD").arg(stream).arg(id);
        for (k, v) in fields {
            c = c.arg(*k).arg(*v);
        }
        self.execute(c)
    }
    pub fn xread(
        &self,
        stream: impl ToRedis,
        id: impl ToRedis,
        count: Option<usize>,
    ) -> Result<RedisValue> {
        let mut c = Command::new("XREAD");
        if let Some(n) = count {
            c = c.arg("COUNT").arg(n);
        }
        self.execute(c.arg("STREAMS").arg(stream).arg(id))
    }
    pub fn xgroup_create(
        &self,
        stream: impl ToRedis,
        group: impl ToRedis,
        id: impl ToRedis,
    ) -> Result<String> {
        self.execute(
            Command::new("XGROUP")
                .arg("CREATE")
                .arg(stream)
                .arg(group)
                .arg(id),
        )
    }
    pub fn xreadgroup(
        &self,
        group: impl ToRedis,
        consumer: impl ToRedis,
        stream: impl ToRedis,
        id: impl ToRedis,
        count: Option<usize>,
    ) -> Result<RedisValue> {
        let mut c = Command::new("XREADGROUP")
            .arg("GROUP")
            .arg(group)
            .arg(consumer);
        if let Some(n) = count {
            c = c.arg("COUNT").arg(n);
        }
        self.execute(c.arg("STREAMS").arg(stream).arg(id))
    }
    pub fn xack(&self, stream: impl ToRedis, group: impl ToRedis, ids: &[&str]) -> Result<i64> {
        self.execute(
            Command::new("XACK")
                .arg(stream)
                .arg(group)
                .args(ids.iter().copied()),
        )
    }
    pub fn execute_retry<T: FromRedis>(
        &self,
        command: Command,
        policy: RetryPolicy,
        idempotent: bool,
    ) -> Result<T> {
        let attempts = if idempotent {
            policy.max_attempts.max(1)
        } else {
            1
        };
        let mut last = None;
        for n in 0..attempts {
            match self.execute(command.clone()) {
                Ok(v) => return Ok(v),
                Err(e) => {
                    last = Some(e);
                    if n + 1 < attempts {
                        std::thread::sleep(policy.delay);
                    }
                }
            }
        }
        Err(last.unwrap_or(Error::Unsupported("retry failed")))
    }
    pub fn execute_resilient<T: FromRedis>(
        &self,
        command: Command,
        policy: RetryPolicy,
        backoff: Backoff,
        breaker: &CircuitBreaker,
        idempotent: bool,
    ) -> Result<T> {
        if !breaker.allow() {
            return Err(Error::CircuitOpen);
        }
        let attempts = if idempotent {
            policy.max_attempts.max(1)
        } else {
            1
        };
        let mut last = None;
        for attempt in 0..attempts {
            match self.execute(command.clone()) {
                Ok(v) => {
                    breaker.success();
                    return Ok(v);
                }
                Err(e) => {
                    breaker.failure();
                    last = Some(e);
                    if attempt + 1 < attempts {
                        std::thread::sleep(backoff.delay(attempt));
                    }
                }
            }
        }
        Err(last.unwrap_or(Error::CircuitOpen))
    }
}
impl Pipeline {
    pub fn command(mut self, c: Command) -> Self {
        self.commands.push(c);
        self
    }
    pub fn execute(self) -> Result<Vec<RedisValue>> {
        let mut c = Connection::connect(
            &self.client.url.host,
            self.client.url.port,
            &self.client.options,
        )?;
        c.execute_many(&self.commands)
    }
}
impl Transaction {
    pub fn watch(mut self, key: impl ToRedis) -> Self {
        self.watched.push(key.encode_arg());
        self
    }
    pub fn command(mut self, c: Command) -> Self {
        self.commands.push(c);
        self
    }
    pub fn exec(self) -> Result<RedisValue> {
        let mut c = Connection::connect(
            &self.client.url.host,
            self.client.url.port,
            &self.client.options,
        )?;
        for k in self.watched {
            c.execute(Command::new("WATCH").arg(k))?;
        }
        c.execute(Command::new("MULTI"))?;
        for x in self.commands {
            c.execute(x)?;
        }
        c.execute(Command::new("EXEC"))
    }
    pub fn discard(self) -> Result<RedisValue> {
        let mut c = Connection::connect(
            &self.client.url.host,
            self.client.url.port,
            &self.client.options,
        )?;
        c.execute(Command::new("MULTI"))?;
        c.execute(Command::new("DISCARD"))
    }
}

pub(crate) fn tcp(host: &str, port: u16, o: &ConnectionOptions) -> Result<TcpStream> {
    use std::net::ToSocketAddrs;
    let addr = (host, port)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| Error::InvalidUrl("host has no address".into()))?;
    let s = TcpStream::connect_timeout(&addr, o.connect_timeout)?;
    s.set_read_timeout(o.command_timeout)?;
    s.set_write_timeout(o.command_timeout)?;
    if let Some(k) = o.keepalive {
        let _ = k;
    }
    Ok(s)
}
