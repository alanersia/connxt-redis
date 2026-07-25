//! A small Redis-compatible client implemented directly over TCP.
#[cfg(feature = "tokio")]
pub mod async_client;
pub mod auth;
pub mod client;
pub mod connection;
pub mod error;
pub mod ops;
pub mod pool;
pub mod protocol;
pub mod pubsub;
pub mod sentinel;
#[cfg(feature = "tls")]
pub mod tls;
pub mod types;

pub use client::{Client, ClientConfig, ConnectionOptions, UrlConfig};
pub use connection::Connection;
pub use error::{Error, Result};
pub use types::{FromRedis, RedisValue, ToRedis};

#[cfg(test)]
mod tests {
    use super::{
        RedisValue,
        client::UrlConfig,
        protocol::{
            codec::{Decoder, Limits},
            encode,
        },
    };

    #[test]
    fn decodes_fragmented_nested_resp3() {
        let mut d = Decoder::new(Limits::default());
        assert!(matches!(d.push(b"*2\r\n$3\r\nfo"), Ok(None)));
        assert!(
            matches!(d.push(b"o\r\n:7\r\n"), Ok(Some(RedisValue::Array(v))) if v == vec![RedisValue::Bulk(b"foo".to_vec()), RedisValue::Integer(7)])
        );
    }

    #[test]
    fn encodes_command() {
        assert_eq!(
            encode(&[b"PING".to_vec(), b"x".to_vec()]),
            b"*2\r\n$4\r\nPING\r\n$1\r\nx\r\n"
        );
    }

    #[test]
    fn parses_auth_and_database() {
        let u = UrlConfig::parse("redis://:dev@127.0.0.1:6379/2").unwrap();
        assert_eq!(u.options.password.as_deref(), Some("dev"));
        assert_eq!(u.options.database, 2);
    }

    #[test]
    fn rejects_malformed_and_limited_frames() {
        let mut d = Decoder::new(Limits {
            max_frame: 32,
            max_depth: 1,
            max_items: 1,
        });
        assert!(d.push(b"%1\r\n+key\r\n").is_err());
        let mut d = Decoder::new(Limits {
            max_frame: 32,
            max_depth: 1,
            max_items: 1,
        });
        assert!(d.push(b"*2\r\n:1\r\n:2\r\n").is_err());
        let mut d = Decoder::new(Limits::default());
        assert!(matches!(d.push(b"_\r\n"), Ok(Some(RedisValue::Null))));
        assert!(matches!(d.push(b",1.5\r\n"), Ok(Some(RedisValue::Double(n))) if n == 1.5));
        assert!(
            matches!(d.push(b">1\r\n+event\r\n"), Ok(Some(RedisValue::Push(v))) if v == vec![RedisValue::Simple("event".into())])
        );
    }

    #[test]
    fn decodes_stream_response() {
        let mut d = Decoder::new(Limits::default());
        let frame = b"*1\r\n*2\r\n$10\r\nstreamtest\r\n*1\r\n*2\r\n$15\r\n1785000070950-0\r\n*2\r\n$5\r\nfield\r\n$5\r\nvalue\r\n";
        assert!(d.push(frame).is_ok(), "stream response should decode");
    }

    #[test]
    fn consumes_nested_frame_without_leaking_children() {
        let mut d = Decoder::new(Limits::default());
        assert!(matches!(
            d.push(b"*1\r\n+ok\r\n+next\r\n"),
            Ok(Some(RedisValue::Array(_)))
        ));
        assert!(matches!(d.push(b""), Ok(Some(RedisValue::Simple(v))) if v == "next"));
    }
}
