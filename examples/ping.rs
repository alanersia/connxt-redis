use connxt_redis::{Client, ClientConfig};

fn main() -> connxt_redis::Result<()> {
    let url =
        std::env::var("REDIS_TEST_URL").unwrap_or_else(|_| "redis://:dev@127.0.0.1:6379/0".into());
    println!("{}", Client::open(ClientConfig::from_url(&url)?)?.ping()?);
    Ok(())
}
