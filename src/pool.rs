use crate::{
    client::ClientConfig,
    connection::Connection,
    error::{Error, Result},
};
use std::{
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};
struct State {
    idle: Vec<(Connection, Instant)>,
    total: usize,
    closed: bool,
}
pub struct Pool {
    cfg: ClientConfig,
    max: usize,
    wait: Duration,
    idle_timeout: Duration,
    state: Arc<(Mutex<State>, Condvar)>,
}
pub struct Pooled {
    conn: Option<Connection>,
    pool: Pool,
}
impl Pool {
    pub fn new(cfg: ClientConfig, max: usize, wait: Duration) -> Self {
        Self {
            cfg,
            max: max.max(1),
            wait,
            idle_timeout: Duration::from_secs(300),
            state: Arc::new((
                Mutex::new(State {
                    idle: Vec::new(),
                    total: 0,
                    closed: false,
                }),
                Condvar::new(),
            )),
        }
    }
    pub fn get(&self) -> Result<Pooled> {
        let until = Instant::now() + self.wait;
        let (lock, cv) = &*self.state;
        let mut s = lock.lock().unwrap();
        if s.closed {
            return Err(Error::Unsupported("pool is closed"));
        }
        loop {
            if let Some((mut c, idle_since)) = s.idle.pop() {
                if idle_since.elapsed() > self.idle_timeout {
                    s.total -= 1;
                    continue;
                }
                if c.is_valid() {
                    return Ok(Pooled {
                        conn: Some(c),
                        pool: self.clone(),
                    });
                }
                s.total -= 1;
            }
            if s.total < self.max {
                s.total += 1;
                drop(s);
                let conn =
                    Connection::connect(&self.cfg.url.host, self.cfg.url.port, &self.cfg.options);
                return match conn {
                    Ok(conn) => Ok(Pooled {
                        conn: Some(conn),
                        pool: self.clone(),
                    }),
                    Err(e) => {
                        s = lock.lock().unwrap();
                        s.total -= 1;
                        cv.notify_one();
                        Err(e)
                    }
                };
            }
            let left = until.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(Error::PoolExhausted);
            }
            s = cv.wait_timeout(s, left).unwrap().0;
        }
    }
    pub fn close(&self) {
        let (lock, cv) = &*self.state;
        let mut s = lock.lock().unwrap();
        s.closed = true;
        s.idle.clear();
        s.total = 0;
        cv.notify_all();
    }
}
impl Clone for Pool {
    fn clone(&self) -> Self {
        Self {
            cfg: self.cfg.clone(),
            max: self.max,
            wait: self.wait,
            idle_timeout: self.idle_timeout,
            state: self.state.clone(),
        }
    }
}
impl Pooled {
    pub fn connection(&mut self) -> Result<&mut Connection> {
        self.conn.as_mut().ok_or(Error::PoolExhausted)
    }
}
impl Drop for Pooled {
    fn drop(&mut self) {
        if let Some(mut c) = self.conn.take() {
            let (l, cv) = &*self.pool.state;
            let mut s = l.lock().unwrap();
            if s.closed {
                return;
            } else if c.is_valid() {
                s.idle.push((c, Instant::now()))
            } else {
                s.total -= 1
            }
            cv.notify_one();
        }
    }
}
