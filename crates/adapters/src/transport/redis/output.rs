use anyhow::{anyhow, Result as AnyResult};
use feldera_adapterlib::transport::{AsyncErrorCallback, OutputEndpoint};
use feldera_types::transport::redis::RedisOutputConfig;
use redis::{Commands, ConnectionInfo};
use std::str::FromStr;
use tracing::{info_span, span::EnteredSpan};

pub struct RedisOutputEndpoint {
    config: ConnectionInfo,
    pool: Option<r2d2::Pool<redis::Client>>,
}

impl RedisOutputEndpoint {
    pub fn new(config: RedisOutputConfig) -> AnyResult<Self> {
        Ok(Self {
            config: ConnectionInfo::from_str(&config.connection_string)?,
            pool: None,
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

        let mut conn = self
            .pool
            .clone()
            .ok_or(anyhow!(
                "redis: trying to push data before connection pool is initialized"
            ))?
            .get()?;

        if let Some(val) = val {
            let _: () = conn.set(key, val)?;
        } else {
            let _: () = conn.del(key)?;
        }

        Ok(())
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }

    fn batch_start(&mut self, _step: feldera_adapterlib::transport::Step) -> AnyResult<()> {
        // TODO: start transction
        Ok(())
    }

    fn batch_end(&mut self) -> AnyResult<()> {
        // TODO: end transction
        Ok(())
    }
}
