use anyhow::{Context, Error as AnyError, Result as AnyResult, anyhow};
use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::transport::{AsyncErrorCallback, OutputEndpoint};
use feldera_types::transport::s2::S2OutputConfig;
use s2_sdk::{
    S2, S2Stream,
    producer::{Producer, ProducerConfig},
    types::{
        AccountEndpoint, AppendRecord, BasinEndpoint, RetryConfig, S2Config, S2Endpoints,
    },
};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, info_span, span::EnteredSpan, Instrument};

pub struct S2OutputEndpoint {
    config: S2OutputConfig,
    s2_stream: Arc<S2Stream>,
    producer: Option<Producer>,
}

impl S2OutputEndpoint {
    pub fn new(config: S2OutputConfig) -> Result<Self, AnyError> {
        let s2_stream = TOKIO
            .block_on(
                async {
                    let retry_config = RetryConfig::new()
                        .with_max_attempts(NonZeroU32::new(10).unwrap())
                        .with_min_base_delay(Duration::from_millis(100))
                        .with_max_base_delay(Duration::from_secs(10));

                    let mut s2_config = S2Config::new(config.auth_token.clone())
                        .with_connection_timeout(Duration::from_secs(10))
                        .with_request_timeout(Duration::from_secs(30))
                        .with_retry(retry_config);

                    if let Some(ref endpoint) = config.endpoint {
                        let endpoints = S2Endpoints::new(
                            AccountEndpoint::new(endpoint)?,
                            BasinEndpoint::new(endpoint)?,
                        )?;
                        s2_config = s2_config.with_endpoints(endpoints);
                    }

                    let client = S2::new(s2_config)?;
                    let basin = client.basin(config.basin.parse().map_err(|e| anyhow!("{e}"))?);
                    Ok::<_, AnyError>(
                        basin.stream(config.stream.parse().map_err(|e| anyhow!("{e}"))?),
                    )
                }
                .instrument(info_span!(
                    "s2_output_init",
                    basin = %config.basin,
                    stream = %config.stream,
                )),
            )
            .map_err(|e| {
                error!(basin = %config.basin, stream = %config.stream, "S2 output init failed: {e:#}");
                e.context(format!(
                    "S2 output initialization failed for stream '{}' in basin '{}'",
                    config.stream, config.basin,
                ))
            })?;

        Ok(Self {
            config,
            s2_stream: Arc::new(s2_stream),
            producer: None,
        })
    }

    fn span(&self) -> EnteredSpan {
        info_span!(
            "s2_output",
            basin = %self.config.basin,
            stream = %self.config.stream,
        )
        .entered()
    }
}

impl OutputEndpoint for S2OutputEndpoint {
    fn connect(&mut self, _async_error_callback: AsyncErrorCallback) -> AnyResult<()> {
        let _guard = self.span();
        let producer = TOKIO.block_on(async {
            self.s2_stream.producer(ProducerConfig::new())
        });
        info!("S2 producer connected");
        self.producer = Some(producer);
        Ok(())
    }

    fn max_buffer_size_bytes(&self) -> usize {
        // S2 enforces a 1MB limit per AppendRecord.
        // Use a conservative limit to leave room for framing overhead.
        1_000_000
    }

    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
        let _guard = self.span();
        let producer = self
            .producer
            .as_ref()
            .ok_or_else(|| anyhow!("s2: push_buffer called before connect"))?;

        let record = AppendRecord::new(buffer.to_vec())
            .context("s2: failed to create AppendRecord from buffer")?;

        TOKIO
            .block_on(async {
                let ticket = producer
                    .submit(record)
                    .await
                    .map_err(|e| anyhow!("s2: failed to submit record: {e}"))?;
                ticket
                    .await
                    .map_err(|e| anyhow!("s2: failed to get ack for record: {e}"))?;
                Ok::<_, AnyError>(())
            })
            .map_err(|e| {
                error!("S2 push_buffer failed: {e:#}");
                e
            })
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        _val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        anyhow::bail!("s2: push_key is not supported; S2 is not a key-value store")
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }
}
