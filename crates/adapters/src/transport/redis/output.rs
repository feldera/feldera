use anyhow::{anyhow, Result as AnyResult};
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
            config: ConnectionInfo::from_str(&config.connection_string)?,
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
        let client = redis::Client::open(self.config.clone())?;
        let pool = r2d2::Pool::builder().build(client)?;

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
        unreachable!()
    }

    fn push_key(
        &mut self,
        key: Option<&[u8]>,
        val: Option<&[u8]>,
        _: &[(&str, Option<&[u8]>)],
    ) -> anyhow::Result<()> {
        let _guard = self.span();

        let Some(key) = key else {
            return Err(anyhow!("cannot push empty key to redis"));
        };

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
            .get()?;

        let pipeline = std::mem::take(&mut self.pipeline);

        pipeline
            .ok_or(anyhow!(
                "redis: batch_end called before batch_start: unreachable"
            ))?
            .exec(&mut conn)?;

        Ok(())
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }
}
