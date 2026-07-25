use crate::{
    auth,
    client::{ConnectionOptions, tcp},
    error::{Error, Result},
    protocol::{
        Command,
        codec::{Decoder, Limits},
    },
    types::RedisValue,
};
use std::io::{Read, Write};
pub struct Connection {
    stream: std::net::TcpStream,
    decoder: Decoder,
}
impl Connection {
    pub fn connect(host: &str, port: u16, o: &ConnectionOptions) -> Result<Self> {
        let mut c = Self {
            stream: tcp(host, port, o)?,
            decoder: Decoder::new(Limits::default()),
        };
        if let Some(p) = o.password.as_deref() {
            auth::authenticate(&mut c, o.username.as_deref(), p)?
        }
        if o.database != 0 {
            c.execute(Command::new("SELECT").arg(o.database as i64))?;
        }
        Ok(c)
    }
    pub fn execute(&mut self, c: Command) -> Result<RedisValue> {
        self.stream.write_all(&crate::protocol::encode(&c.0))?;
        self.read_value()
    }
    pub fn execute_many(&mut self, commands: &[Command]) -> Result<Vec<RedisValue>> {
        for c in commands {
            self.stream.write_all(&crate::protocol::encode(&c.0))?;
        }
        commands.iter().map(|_| self.read_value()).collect()
    }
    pub fn hello(&mut self, version: u8) -> Result<RedisValue> {
        self.execute(Command::new("HELLO").arg(version as i64))
    }
    fn read_value(&mut self) -> Result<RedisValue> {
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
    pub fn read_message(&mut self) -> Result<RedisValue> {
        self.read_value()
    }
    pub fn is_valid(&mut self) -> bool {
        self.execute(Command::new("PING")).is_ok()
    }
}
