use crate::{
    transport::{InputQueue, InputReaderCommand, NonFtInputReaderCommand},
    InputConsumer, InputEndpoint, InputReader, Parser, PipelineState, TransportInputEndpoint,
};
use anyhow::{anyhow, Error as AnyError, Result as AnyResult};
use async_nats::{self, jetstream};
use dbsp::circuit::tokio::TOKIO;
use feldera_types::{config::FtModel, program_schema::Relation, transport::nats::NatsInputConfig};
use futures::StreamExt;
use std::{num::NonZeroU64, sync::atomic::Ordering};
use std::{sync::Arc, thread};
use tokio::{
    select,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{info_span, Instrument};

mod atomic_option;
use atomic_option::AtomicOptionNonZeroU64;

pub struct NatsInputEndpoint {
    config: Arc<NatsInputConfig>,
}

impl NatsInputEndpoint {
    pub fn new(config: NatsInputConfig) -> Result<Self, AnyError> {
        Ok(Self {
            config: Arc::new(config),
        })
    }
}

impl InputEndpoint for NatsInputEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        None
    }
}

impl TransportInputEndpoint for NatsInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        _schema: Relation,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(NatsReader::new(
            self.config.clone(),
            consumer,
            parser,
        )?))
    }
}

struct NatsReader {
    state_sender: UnboundedSender<NonFtInputReaderCommand>,
}

impl NatsReader {
    fn new(
        config: Arc<NatsInputConfig>,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
    ) -> AnyResult<Self> {
        let span = info_span!("nats_input");
        let (state_sender, state_receiver) = unbounded_channel();
        let nats_connection =
            TOKIO.block_on(Self::connect_nats(&config).instrument(span.clone()))?;
        thread::spawn({
            move || {
                let consumer_clone = consumer.clone();
                TOKIO.block_on(async {
                    Self::worker_task(
                        config.clone(),
                        jetstream::new(nats_connection),
                        consumer_clone,
                        parser,
                        state_receiver,
                    )
                    .instrument(span)
                    .await
                    .unwrap_or_else(|e| consumer.error(true, e));
                })
            }
        });

        Ok(Self { state_sender })
    }

    async fn connect_nats(config: &NatsInputConfig) -> Result<async_nats::Client, AnyError> {
        let mut connect_options = async_nats::ConnectOptions::new();
        if let Some(credentials) = config.credentials.as_ref() {
            connect_options = connect_options.credentials(credentials)?;
        }

        let client = connect_options.connect(&config.server_url).await?;

        Ok(client)
    }

    async fn worker_task(
        config: Arc<NatsInputConfig>,
        jetstream: jetstream::Context,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        mut state_receiver: UnboundedReceiver<NonFtInputReaderCommand>,
    ) -> Result<(), AnyError> {
        let mut cancel: Option<(CancellationToken, JoinHandle<()>)> = None;
        let queue = Arc::new(InputQueue::new(consumer.clone()));
        let next_stream_sequence = Arc::new(AtomicOptionNonZeroU64::new(None));
        let mut nats_consumer_config = config.consumer_config.clone();

        while let Some(command) = state_receiver.recv().await {
            match command {
                NonFtInputReaderCommand::Queue => queue.queue(),
                NonFtInputReaderCommand::Transition(state) => match state {
                    PipelineState::Paused => {
                        if let Some((cancel_token, handle)) = cancel.take() {
                            cancel_token.cancel();
                            let _ = handle.await;
                        }
                    }
                    PipelineState::Running => {
                        if cancel.is_none() {
                            // Update the consumer config's StartSequence if this
                            // InputEndpoint has previously received messages, in
                            // order to continue where we left off.
                            if let Some(nss) = next_stream_sequence.load(Ordering::Acquire) {
                                nats_consumer_config.deliver_policy =
                                    jetstream::consumer::DeliverPolicy::ByStartSequence {
                                        start_sequence: nss.get(),
                                    };
                            }

                            let nats_stream_name = &config.stream_name;
                            let nats_consumer = jetstream
                                .create_consumer_strict_on_stream(
                                    nats_consumer_config.clone(),
                                    nats_stream_name,
                                )
                                .await?;

                            let mut nats_messages = nats_consumer.messages().await?;

                            let consumer = consumer.clone();
                            let mut parser = parser.fork();

                            let cancel_token = CancellationToken::new();
                            let cancel_token_copy = cancel_token.clone();

                            let handle = tokio::spawn({
                                let queue = queue.clone();
                                let next_stream_sequence = next_stream_sequence.clone();
                                async move {
                                    loop {
                                        select! {
                                            _ = cancel_token_copy.cancelled() => {
                                                break;
                                            }
                                            Some(result) = nats_messages.next() => {
                                                match result {
                                                    Ok(message) => {
                                                        let info = match message.info() {
                                                            Ok(info) => info,
                                                            Err(error) => {
                                                                consumer.error(false, anyhow!("Failed to get NATS message info: {error}"));
                                                                continue;
                                                            }
                                                        };
                                                        next_stream_sequence.store(NonZeroU64::new(info.stream_sequence + 1), Ordering::Release);
                                                        let data = &message.payload;
                                                        queue.push(parser.parse(&data), data.len());
                                                    }
                                                    Err(error) => {
                                                        consumer.error(false, anyhow!("NATS error: {error}"))
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            });
                            cancel = Some((cancel_token, handle));
                        }
                    }
                    PipelineState::Terminated => break,
                },
            }
        }
        if let Some((cancel_token, handle)) = cancel.take() {
            cancel_token.cancel();
            let _ = handle.await;
        }
        Ok(())
    }
}

impl InputReader for NatsReader {
    fn request(&self, command: InputReaderCommand) {
        let _ = self.state_sender.send(command.as_nonft().unwrap());
    }

    fn is_closed(&self) -> bool {
        self.state_sender.is_closed()
    }
}
