use crate::{Error, RedisValue, Result};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_frame: usize,
    pub max_depth: usize,
    pub max_items: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame: 64 * 1024 * 1024,
            max_depth: 128,
            max_items: 1_000_000,
        }
    }
}

pub fn encode(args: &[Vec<u8>]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

pub struct Decoder {
    buf: Vec<u8>,
    pub limits: Limits,
}
impl Decoder {
    pub fn new(limits: Limits) -> Self {
        Self {
            buf: Vec::new(),
            limits,
        }
    }
    pub fn push(&mut self, bytes: &[u8]) -> Result<Option<RedisValue>> {
        if self.buf.len() + bytes.len() > self.limits.max_frame {
            return Err(Error::Protocol("frame limit exceeded".into()));
        }
        self.buf.extend_from_slice(bytes);
        match parse(&self.buf, 0, 0, self.limits) {
            Ok(Some((v, n))) => {
                self.buf.drain(..n);
                Ok(Some(v))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

fn line(b: &[u8], from: usize) -> Option<(String, usize)> {
    let end = b[from..].windows(2).position(|x| x == b"\r\n")? + from;
    Some((String::from_utf8_lossy(&b[from..end]).into_owned(), end + 2))
}
fn parse(b: &[u8], at: usize, depth: usize, l: Limits) -> Result<Option<(RedisValue, usize)>> {
    if depth > l.max_depth {
        return Err(Error::Protocol("nesting limit exceeded".into()));
    }
    let Some(&t) = b.get(at) else { return Ok(None) };
    let (s, p) = match line(b, at + 1) {
        Some(x) => x,
        None => return Ok(None),
    };
    let complete = |v| Ok(Some((v, p)));
    match t {
        b'+' => complete(RedisValue::Simple(s)),
        b'-' => complete(RedisValue::Error(s)),
        b':' => complete(RedisValue::Integer(
            s.parse()
                .map_err(|_| Error::Protocol("bad integer".into()))?,
        )),
        b',' => complete(RedisValue::Double(
            s.parse()
                .map_err(|_| Error::Protocol("bad double".into()))?,
        )),
        b'#' => complete(RedisValue::Boolean(s == "t")),
        b'_' => complete(RedisValue::Null),
        b'$' | b'=' | b'!' => {
            let n: isize = s
                .parse()
                .map_err(|_| Error::Protocol("bad bulk length".into()))?;
            if n < 0 {
                return complete(RedisValue::Null);
            }
            let n = n as usize;
            if n > l.max_frame {
                return Err(Error::Protocol("bulk limit exceeded".into()));
            }
            if b.len() < p + n + 2 {
                return Ok(None);
            }
            if &b[p + n..p + n + 2] != b"\r\n" {
                return Err(Error::Protocol("bulk missing terminator".into()));
            }
            let v = b[p..p + n].to_vec();
            let value = if t == b'!' {
                RedisValue::Error(String::from_utf8_lossy(&v).into())
            } else {
                RedisValue::Bulk(v)
            };
            Ok(Some((value, p + n + 2)))
        }
        b'*' | b'%' | b'~' | b'>' => {
            let n: isize = s
                .parse()
                .map_err(|_| Error::Protocol("bad collection length".into()))?;
            if n < 0 {
                return complete(RedisValue::Null);
            }
            let n = n as usize;
            if n > l.max_items {
                return Err(Error::Protocol("item limit exceeded".into()));
            }
            let mut vals = Vec::with_capacity(n);
            let mut q = p;
            for _ in 0..n {
                let Some((v, next)) = parse(b, q, depth + 1, l)? else {
                    return Ok(None);
                };
                vals.push(v);
                q = next
            }
            if t == b'%' {
                if vals.len() % 2 != 0 {
                    return Err(Error::Protocol("map has an odd number of elements".into()));
                }
                let mut m = BTreeMap::new();
                let mut it = vals.into_iter();
                while let (Some(k), Some(v)) = (it.next(), it.next()) {
                    m.insert(String::from_redis(k)?, v);
                }
                Ok(Some((RedisValue::Map(m), q)))
            } else {
                Ok(Some((
                    match t {
                        b'~' => RedisValue::Set(vals),
                        b'>' => RedisValue::Push(vals),
                        _ => RedisValue::Array(vals),
                    },
                    q,
                )))
            }
        }
        _ => Err(Error::Protocol("unknown RESP type".into())),
    }
}
trait StringFrom {
    fn from_redis(v: RedisValue) -> Result<String>;
}
impl StringFrom for String {
    fn from_redis(v: RedisValue) -> Result<String> {
        match v {
            RedisValue::Simple(s) => Ok(s),
            RedisValue::Bulk(b) => {
                String::from_utf8(b).map_err(|_| Error::Protocol("map key is not UTF-8".into()))
            }
            _ => Err(Error::Protocol("map key is not a string".into())),
        }
    }
}
