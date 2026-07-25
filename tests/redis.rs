use connxt_redis::{Client, ClientConfig, RedisValue};
use std::time::Duration;

fn client() -> Option<Client> {
    let url = std::env::var("REDIS_TEST_URL").ok()?;
    Client::open(ClientConfig::from_url(&url).ok()?).ok()
}

#[test]
fn real_server_commands() {
    let Some(client) = client() else { return };
    assert_eq!(client.ping().unwrap(), "PONG");
    let key = format!("connxt-test:{}", std::process::id());
    assert_eq!(client.set(key.as_str(), "ok").unwrap(), "OK");
    assert_eq!(client.get(key.as_str()).unwrap(), Some(b"ok".to_vec()));
    assert!(matches!(
        client.scan(0, Some("connxt-test:*"), Some(10)).unwrap(),
        RedisValue::Array(_)
    ));
}

#[test]
fn real_server_workflows() {
    let Some(client) = client() else { return };
    let key = format!("connxt-workflow:{}", std::process::id());
    let values = client
        .pipeline()
        .command(client.command("SET").arg(key.as_str()).arg("1"))
        .command(client.command("INCR").arg(key.as_str()))
        .execute()
        .unwrap();
    assert_eq!(values.len(), 2);
    let stream = format!("connxt-stream:{}", std::process::id());
    let group = format!("g:{}", std::process::id());
    let id = client
        .xadd(stream.as_str(), "*", &[("field", "value")])
        .unwrap();
    client
        .xgroup_create(stream.as_str(), group.as_str(), "0")
        .unwrap();
    let result = client.xreadgroup(group.as_str(), "consumer", stream.as_str(), ">", Some(1));
    assert!(
        result.is_ok(),
        "xreadgroup failed after adding {id}: {result:?}"
    );
}

#[cfg(feature = "tokio")]
#[tokio::test]
async fn async_real_server_commands() {
    let Some(url) = std::env::var("REDIS_TEST_URL").ok() else {
        return;
    };
    let cfg = ClientConfig::from_url(&url).unwrap();
    let client = connxt_redis::async_client::AsyncClient::open(cfg);
    let pong: String = client
        .execute(connxt_redis::protocol::Command::new("PING"))
        .await
        .unwrap();
    assert_eq!(pong, "PONG");
}

#[test]
fn real_server_pubsub() {
    let Some(client) = client() else { return };
    let mut connection = client.connect().unwrap();
    let mut subscription = connxt_redis::pubsub::PubSub::new(&mut connection);
    let ack = subscription.subscribe(&["connxt-channel"]).unwrap();
    assert!(!ack.is_empty());
    let publisher = std::thread::spawn(move || {
        assert_eq!(client.publish("connxt-channel", "message").unwrap(), 1);
    });
    let message = subscription.next_message().unwrap();
    publisher.join().unwrap();
    assert!(matches!(message, RedisValue::Array(_)));
}

#[test]
fn pool_closes_cleanly() {
    let Some(url) = std::env::var("REDIS_TEST_URL").ok() else {
        return;
    };
    let cfg = ClientConfig::from_url(&url).unwrap();
    let pool = connxt_redis::pool::Pool::new(cfg, 2, Duration::from_millis(100));
    let handle = pool.get().unwrap();
    pool.close();
    drop(handle);
    assert!(pool.get().is_err());
}

#[test]
fn sentinel_discovery_real_server() {
    let Some(url) = std::env::var("SENTINEL_TEST_URL").ok() else {
        return;
    };
    let endpoint = connxt_redis::UrlConfig::parse(&url).unwrap();
    let master = connxt_redis::sentinel::discover(&[endpoint], "mymaster").unwrap();
    assert_eq!(master.port, 6379);
    assert_eq!(master.host, "172.30.0.2");
}
