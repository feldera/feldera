use crate::{
    transport::{InputQueue, Step},
    InputConsumer, InputEndpoint, InputReader, Parser, PipelineState, TransportInputEndpoint,
};
use anyhow::{anyhow, bail, Error as AnyError, Result as AnyResult};
use chrono::DateTime;
use dbsp::circuit::tokio::TOKIO;
use feldera_types::{program_schema::Relation, transport::pubsub::PubSubInputConfig};
use futures::StreamExt;
use google_cloud_gax::conn::Environment;
use google_cloud_pubsub::{
    client::{google_cloud_auth::credentials::CredentialsFile, Client, ClientConfig},
    subscription::{SeekTo, Subscription},
};
use log::{debug, warn};
use std::{
    sync::Arc,
    thread,
    time::{Duration, SystemTime},
};
use tokio::{
    sync::watch::{channel, Receiver, Sender},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

pub struct PubSubInputEndpoint {
    config: Arc<PubSubInputConfig>,
}

impl PubSubInputEndpoint {
    pub fn new(config: PubSubInputConfig) -> Result<Self, AnyError> {
        // Validation.
        if config.snapshot.is_some() && config.timestamp.is_some() {
            bail!("'snapshot' and 'timestamp' config options cannot be set at the same time",);
        }
        Ok(Self {
            config: Arc::new(config),
        })
    }
}

impl InputEndpoint for PubSubInputEndpoint {
    fn is_fault_tolerant(&self) -> bool {
        false
    }
}

impl TransportInputEndpoint for PubSubInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        _start_step: Step,
        _schema: Relation,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(PubSubReader::new(
            self.config.clone(),
            consumer,
            parser,
        )?))
    }
}

struct PubSubReader {
    state_sender: Sender<PipelineState>,
    queue: Arc<InputQueue>,
}

impl PubSubReader {
    fn new(
        config: Arc<PubSubInputConfig>,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
    ) -> AnyResult<Self> {
        let (state_sender, state_receiver) = channel(PipelineState::Paused);
        let subscription = TOKIO.block_on(Self::subscribe(&config))?;
        let queue = Arc::new(InputQueue::new(consumer.clone()));
        thread::spawn({
            let queue = queue.clone();
            move || {
                let consumer_clone = consumer.clone();
                TOKIO.block_on(async {
                    Self::worker_task(subscription, consumer_clone, parser, queue, state_receiver)
                        .await
                        .unwrap_or_else(|e| consumer.error(true, e));
                })
            }
        });

        Ok(Self {
            state_sender,
            queue,
        })
    }

    async fn subscribe(config: &PubSubInputConfig) -> Result<Subscription, AnyError> {
        let client_config = pubsub_config(config).await?;

        let client = Client::new(client_config)
            .await
            .map_err(|e| anyhow!("error connecting to the Pub/Sub service: {e}"))?;

        debug!(
            "Pub/Sub input endpoint: connecting to subscription {}",
            &config.subscription
        );

        let subscription = client.subscription(&config.subscription);
        if !subscription.exists(None).await? {
            bail!("subscription '{}' does not exist", config.subscription);
        }

        if let Some(snapshot) = &config.snapshot {
            subscription
                .seek(SeekTo::Snapshot(snapshot.to_string()), None)
                .await
                .map_err(|e| anyhow!("error retrieving Pub/Sub snapshot '{snapshot}': {e}"))?;
        } else if let Some(timestamp) = &config.timestamp {
            let ts = DateTime::parse_from_rfc3339(timestamp)
                .map_err(|e| anyhow!("not a valid ISO 8601 date-time string '{timestamp}' {e}"))?;
            subscription
                .seek(SeekTo::Timestamp(SystemTime::from(ts)), None)
                .await
                .map_err(|e| anyhow!("error seeking to timestamp '{timestamp}': {e}"))?;
        }

        Ok(subscription)
    }

    async fn worker_task(
        subscription: Subscription,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        queue: Arc<InputQueue>,
        mut state_receiver: Receiver<PipelineState>,
    ) -> Result<(), AnyError> {
        let mut state = PipelineState::Paused;
        let mut cancel: Option<(CancellationToken, JoinHandle<()>)> = None;

        loop {
            // Wait for state change.
            if let Ok(s) = state_receiver.wait_for(|s| s != &state).await {
                state = *s;
            } else {
                warn!("Pub/Sub input endpoint: connector terminated unexpectedly, exiting worker task");
                return Ok(());
            };

            match state {
                PipelineState::Paused => {
                    if let Some((token, handle)) = cancel.take() {
                        token.cancel();
                        let _ = handle.await;
                    }
                }
                PipelineState::Running => {
                    // TODO: Do we need to tune the config (the parameter to subscribe)?
                    let mut stream = subscription
                        .subscribe(None)
                        .await
                        .map_err(|e| anyhow!("error subscribing to messages: {e}"))?;

                    let consumer = consumer.clone();
                    let mut parser = parser.fork();
                    let token = stream.cancellable();
                    let handle = tokio::spawn({
                        let queue = queue.clone();
                        async move {
                            // None if the stream is cancelled
                            while let Some(message) = stream.next().await {
                                queue.push(
                                    message.message.data.len(),
                                    parser.parse(&message.message.data),
                                );
                                message.ack().await.unwrap_or_else(|e| {
                                    consumer.error(
                                        false,
                                        anyhow!("gRPC error acknowledging Pub/Sub message: {e}"),
                                    )
                                });
                            }
                        }
                    });
                    cancel = Some((token, handle));
                }
                PipelineState::Terminated => {
                    if let Some((token, handle)) = cancel.take() {
                        token.cancel();
                        let _ = handle.await;
                    }
                    return Ok(());
                }
            }
        }
    }
}

impl InputReader for PubSubReader {
    fn start(&self, _step: Step) -> AnyResult<()> {
        self.state_sender.send(PipelineState::Running)?;
        Ok(())
    }

    fn pause(&self) -> AnyResult<()> {
        self.state_sender.send(PipelineState::Paused)?;
        Ok(())
    }

    fn disconnect(&self) {
        let _ = self.state_sender.send(PipelineState::Terminated);
    }

    fn flush(&self, n: usize) -> usize {
        self.queue.flush(n)
    }
}

/// Create `ClientConfig` from connector config.  Performs authentication when
/// `config.credentials` is specified.
async fn pubsub_config(config: &PubSubInputConfig) -> Result<ClientConfig, AnyError> {
    let mut client_config = ClientConfig::default();

    if let Some(project_id) = &config.project_id {
        client_config.project_id = Some(project_id.to_string());
    }

    if let Some(pool_size) = config.pool_size {
        client_config.pool_size = Some(pool_size as usize);
    }

    if let Some(endpoint) = &config.endpoint {
        client_config.endpoint = endpoint.to_string();
    }

    if let Some(connect_timeout_seconds) = config.connect_timeout_seconds {
        client_config.connection_option.connect_timeout =
            Some(Duration::from_secs(connect_timeout_seconds as u64));
    }

    if let Some(timeout_seconds) = config.timeout_seconds {
        client_config.connection_option.timeout = Some(Duration::from_secs(timeout_seconds as u64));
    }

    // Use credentials file if specified.
    // Otherwise, use application default credentials, unless emulator is configured.
    if let Some(credentials) = &config.credentials {
        let credentials_file = serde_json::from_str::<CredentialsFile>(credentials)
            .map_err(|e| anyhow!("error parsing credentials: {e}"))?;

        debug!("Pub/Sub input endpoint: authenticating using provided credentials");
        client_config = client_config
            .with_credentials(credentials_file)
            .await
            .map_err(|e| anyhow!("authentication error: {e}"))?;
    }
    if let Some(emulator) = &config.emulator {
        client_config.environment = Environment::Emulator(emulator.clone());
    } else {
        debug!("Pub/Sub input endpoint: authenticating using Application Default Credentials");

        client_config = client_config
            .with_auth()
            .await
            .map_err(|e| anyhow!("authentication error: {e}"))?;
    }

    Ok(client_config)
}
