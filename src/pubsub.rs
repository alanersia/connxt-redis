use crate::{connection::Connection, error::Result, protocol::Command, types::RedisValue};
pub struct PubSub<'a> {
    connection: &'a mut Connection,
}
impl<'a> PubSub<'a> {
    pub fn new(connection: &'a mut Connection) -> Self {
        Self { connection }
    }
    pub fn subscribe(&mut self, channels: &[&str]) -> Result<Vec<RedisValue>> {
        self.connection
            .execute_many(&[Command::new("SUBSCRIBE").args(channels.iter().copied())])
    }
    pub fn psubscribe(&mut self, patterns: &[&str]) -> Result<Vec<RedisValue>> {
        self.connection
            .execute_many(&[Command::new("PSUBSCRIBE").args(patterns.iter().copied())])
    }
    pub fn next_message(&mut self) -> Result<RedisValue> {
        self.connection.read_message()
    }
}
