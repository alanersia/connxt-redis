use crate::{Error, Result};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub enum RedisValue {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Vec<u8>),
    Array(Vec<RedisValue>),
    Map(BTreeMap<String, RedisValue>),
    Set(Vec<RedisValue>),
    Null,
    Double(f64),
    Boolean(bool),
    Push(Vec<RedisValue>),
}

pub trait ToRedis {
    fn encode_arg(&self) -> Vec<u8>;
}
impl ToRedis for String {
    fn encode_arg(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}
impl ToRedis for &str {
    fn encode_arg(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}
impl ToRedis for Vec<u8> {
    fn encode_arg(&self) -> Vec<u8> {
        self.clone()
    }
}
impl ToRedis for &[u8] {
    fn encode_arg(&self) -> Vec<u8> {
        self.to_vec()
    }
}
impl ToRedis for i64 {
    fn encode_arg(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }
}
impl ToRedis for usize {
    fn encode_arg(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }
}
impl ToRedis for bool {
    fn encode_arg(&self) -> Vec<u8> {
        if *self { b"1".to_vec() } else { b"0".to_vec() }
    }
}

pub trait FromRedis: Sized {
    fn from_redis(v: RedisValue) -> Result<Self>;
}
impl FromRedis for RedisValue {
    fn from_redis(v: RedisValue) -> Result<Self> {
        Ok(v)
    }
}
impl FromRedis for String {
    fn from_redis(v: RedisValue) -> Result<Self> {
        match v {
            RedisValue::Simple(s) | RedisValue::Error(s) => Ok(s),
            RedisValue::Bulk(b) => {
                String::from_utf8(b).map_err(|_| Error::Type("invalid UTF-8".into()))
            }
            _ => Err(Error::Type("expected string".into())),
        }
    }
}
impl FromRedis for Vec<u8> {
    fn from_redis(v: RedisValue) -> Result<Self> {
        match v {
            RedisValue::Bulk(b) => Ok(b),
            RedisValue::Simple(s) => Ok(s.into_bytes()),
            RedisValue::Null => Ok(Vec::new()),
            _ => Err(Error::Type("expected bytes".into())),
        }
    }
}
impl FromRedis for i64 {
    fn from_redis(v: RedisValue) -> Result<Self> {
        match v {
            RedisValue::Integer(n) => Ok(n),
            RedisValue::Bulk(b) => String::from_utf8_lossy(&b)
                .parse()
                .map_err(|_| Error::Type("expected integer".into())),
            _ => Err(Error::Type("expected integer".into())),
        }
    }
}
impl<T: FromRedis> FromRedis for Option<T> {
    fn from_redis(v: RedisValue) -> Result<Self> {
        if v == RedisValue::Null {
            Ok(None)
        } else {
            T::from_redis(v).map(Some)
        }
    }
}
