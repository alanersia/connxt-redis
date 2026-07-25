use crate::{
    client::{Client, ClientConfig, UrlConfig},
    error::{Error, Result},
    protocol::Command,
    types::RedisValue,
};

pub fn discover(sentinels: &[UrlConfig], service: &str) -> Result<UrlConfig> {
    for endpoint in sentinels {
        let cfg = ClientConfig {
            url: endpoint.clone(),
            options: endpoint.options.clone(),
            metrics: std::sync::Arc::new(crate::ops::Metrics::default()),
        };
        let Ok(client) = Client::open(cfg) else {
            continue;
        };
        let Ok(RedisValue::Array(values)) = client.execute::<RedisValue>(
            Command::new("SENTINEL")
                .arg("get-master-addr-by-name")
                .arg(service),
        ) else {
            continue;
        };
        if values.len() < 2 {
            continue;
        }
        let host = match &values[0] {
            RedisValue::Bulk(v) => String::from_utf8_lossy(v).into_owned(),
            RedisValue::Simple(v) => v.clone(),
            _ => continue,
        };
        let port = match &values[1] {
            RedisValue::Bulk(v) => String::from_utf8_lossy(v).parse().ok(),
            RedisValue::Simple(v) => v.parse().ok(),
            _ => None,
        };
        if let Some(port) = port {
            return Ok(UrlConfig {
                host,
                port,
                tls: endpoint.tls,
                options: endpoint.options.clone(),
            });
        }
    }
    Err(Error::Unsupported("no Sentinel returned a master address"))
}
