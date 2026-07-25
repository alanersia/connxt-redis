use std::{fmt, io};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Protocol(String),
    Server(String),
    InvalidUrl(String),
    Type(String),
    Timeout,
    PoolExhausted,
    CircuitOpen,
    Unsupported(&'static str),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => e.fmt(f),
            Self::Protocol(e) => write!(f, "protocol error: {e}"),
            Self::Server(e) => write!(f, "redis error: {e}"),
            Self::InvalidUrl(e) => write!(f, "invalid redis URL: {e}"),
            Self::Type(e) => write!(f, "type error: {e}"),
            Self::Timeout => f.write_str("operation timed out"),
            Self::PoolExhausted => f.write_str("pool acquisition timed out"),
            Self::CircuitOpen => f.write_str("circuit breaker is open"),
            Self::Unsupported(e) => write!(f, "unsupported: {e}"),
        }
    }
}
impl std::error::Error for Error {}
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
