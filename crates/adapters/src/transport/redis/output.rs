use anyhow::{Result as AnyResult, anyhow};
use feldera_adapterlib::transport::{AsyncErrorCallback, OutputEndpoint};
use feldera_types::transport::redis::RedisOutputConfig;
use redis::{ConnectionInfo, Pipeline};
use std::str::FromStr;
use tracing::{info_span, span::EnteredSpan};

/// Handles output to Redis.
///
/// This connector sets the output pair.
pub struct RedisOutputEndpoint {
    config: ConnectionInfo,
    pool: Option<r2d2::Pool<redis::Client>>,
    pipeline: Option<redis::Pipeline>,
}

impl RedisOutputEndpoint {
    pub fn new(config: RedisOutputConfig) -> AnyResult<Self> {
        Ok(Self {
            config: ConnectionInfo::from_str(&config.connection_string)
                .map_err(|e| anyhow!("error parsing Redis connection string: {e}"))?,
            pool: None,
            pipeline: None,
        })
    }

    pub fn span(&self) -> EnteredSpan {
        info_span!(
            "redis_output",
            ft = false,
            config = self.config.addr.to_string()
        )
        .entered()
    }
}

impl OutputEndpoint for RedisOutputEndpoint {
    fn connect(&mut self, _: AsyncErrorCallback) -> anyhow::Result<()> {
        let _guard = self.span();
        let client = redis::Client::open(self.config.clone())
            .map_err(|e| anyhow!("error connecting to the Redis server: {e}"))?;
        let pool = r2d2::Pool::builder()
            .build(client)
            .map_err(|e| anyhow!("error opening a connection pool to the Redis server: {e}"))?;

        self.pool = Some(pool);

        Ok(())
    }

    fn max_buffer_size_bytes(&self) -> usize {
        usize::MAX
    }

    // Creates a [`redis::Pipeline`] that is atomic, so that every batch is
    // committed as a transaction.
    fn batch_start(&mut self, _step: feldera_adapterlib::transport::Step) -> AnyResult<()> {
        let mut pipeline = Pipeline::new();
        pipeline.atomic();
        self.pipeline = Some(pipeline);
        Ok(())
    }

    fn push_buffer(&mut self, _: &[u8]) -> anyhow::Result<()> {
        anyhow::bail!("redis: invalid format selected for redis connector")
    }

    fn push_key(
        &mut self,
        key: Option<&[u8]>,
        val: Option<&[u8]>,
        _: &[(&str, Option<&[u8]>)],
    ) -> anyhow::Result<()> {
        let _guard = self.span();

        let key = key.ok_or(anyhow!("cannot push empty key to redis"))?;

        let pipeline = self.pipeline.as_mut().ok_or(anyhow!(
            "redis: trying to push data before pipeline is initialized: unreachable"
        ))?;

        if let Some(val) = val {
            pipeline.set(key, val);
        } else {
            pipeline.del(key);
        }

        Ok(())
    }

    // Executes the transaction.
    fn batch_end(&mut self) -> AnyResult<()> {
        let mut conn = self
            .pool
            .clone()
            .ok_or(anyhow!(
                "redis: trying to get connection from pool before the pool is initialized: unreachable"
            ))?
            .get().map_err(|e| anyhow!("redis: error trying to get a connection from redis connection pool: {e}"))?;

        let pipeline = std::mem::take(&mut self.pipeline);

        let pipeline = pipeline.ok_or(anyhow!(
            "redis: batch_end called before batch_start: unreachable"
        ))?;

        let count = pipeline.cmd_iter().count();

        pipeline
            .exec(&mut conn)
            .map_err(|e| anyhow!("redis: error committing Redis transaction; {count} uncommitted updates will be lost: {e}"))?;

        Ok(())
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }
}

// ── Connector registry ────────────────────────────────────────────────────────

use serde_json::Value as JsonValue;

fn redis_output_config_schema() -> JsonValue {
    JsonValue::Object(Default::default())
}

pub fn build_redis_output(
    config: &JsonValue,
    _endpoint_name: &str,
    _fault_tolerant: bool,
    _secrets_dir: &std::path::Path,
) -> AnyResult<Box<dyn OutputEndpoint>> {
    let config: RedisOutputConfig = serde_json::from_value(config.clone())?;
    Ok(Box::new(RedisOutputEndpoint::new(config)?))
}

#[linkme::distributed_slice(feldera_adapterlib_meta::CONNECTOR_METADATA_REGISTRY)]
static REDIS_OUTPUT_META: feldera_adapterlib_meta::ConnectorDescriptor =
    feldera_adapterlib_meta::ConnectorDescriptor {
        name: "redis_output",
        crate_name: env!("CARGO_CRATE_NAME"),
        direction: feldera_adapterlib_meta::Direction::Output,
        kind: feldera_adapterlib_meta::ConnectorKind::Regular,
        fault_tolerance: None,
        config_schema: redis_output_config_schema,
        default_format: None,
        flags: feldera_adapterlib_meta::ConnectorFlags::EMPTY,
    };

// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod registry_test {
    #[test]
    fn redis_output_descriptor() {
        let d = feldera_adapterlib::meta::descriptor_by_name("redis_output")
            .expect("redis_output descriptor not registered");
        assert!(d.direction.allows_output());
    }
}
