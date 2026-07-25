use crate::types::ToRedis;
#[derive(Debug, Clone)]
pub struct Command(pub Vec<Vec<u8>>);
impl Command {
    pub fn new(name: impl AsRef<[u8]>) -> Self {
        Self(vec![name.as_ref().to_ascii_uppercase()])
    }
    pub fn arg(mut self, v: impl ToRedis) -> Self {
        self.0.push(v.encode_arg());
        self
    }
    pub fn args<I, T>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: ToRedis,
    {
        self.0.extend(values.into_iter().map(|v| v.encode_arg()));
        self
    }
}
