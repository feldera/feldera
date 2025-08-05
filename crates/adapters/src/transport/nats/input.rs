use crate::{
    transport::{InputQueue, InputReaderCommand, NonFtInputReaderCommand},
    InputConsumer, InputEndpoint, InputReader, Parser, PipelineState, TransportInputEndpoint,
};
use anyhow::{anyhow, Error as AnyError, Result as AnyResult};
use async_nats::{self, jetstream};
use dbsp::circuit::tokio::TOKIO;
use feldera_types::{
    config::FtModel,
    program_schema::Relation,
    transport::nats::{self as cfg, NatsInputConfig},
};
use futures::StreamExt;
use std::{num::NonZeroU64, sync::atomic::Ordering};
use std::sync::Arc;
use tokio::{
    select,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{info_span, Instrument};

mod atomic_option;
use atomic_option::AtomicOptionNonZeroU64;
mod config_utils;
use config_utils::{translate_connect_options, translate_consumer_options};

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
    command_sender: UnboundedSender<InputReaderCommand>,
}

impl NatsReader {
    fn new(
        config: Arc<NatsInputConfig>,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
    ) -> AnyResult<Self> {
        let span = info_span!("nats_input");
        let (command_sender, command_receiver) = unbounded_channel();
        let nats_connection = TOKIO
            .block_on(Self::connect_nats(&config.connection_config).instrument(span.clone()))?;

        let consumer_clone = consumer.clone();
        TOKIO.spawn(async move {
            Self::worker_task(
                config,
                jetstream::new(nats_connection),
                consumer_clone,
                parser,
                command_receiver,
            )
            .instrument(span)
            .await
            .unwrap_or_else(|e| consumer.error(true, e));
        });

        Ok(Self { command_sender })
    }

    async fn connect_nats(
        connection_config: &cfg::ConnectOptions,
    ) -> Result<async_nats::Client, AnyError> {
        let connect_options = translate_connect_options(&connection_config).await?;

        let client = connect_options
            .connect(&connection_config.server_url)
            .await?;

        Ok(client)
    }

    async fn worker_task(
        config: Arc<NatsInputConfig>,
        jetstream: jetstream::Context,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        mut command_receiver: UnboundedReceiver<NonFtInputReaderCommand>,
    ) -> Result<(), AnyError> {
        let mut cancel: Option<(CancellationToken, JoinHandle<()>)> = None;
        let queue = Arc::new(InputQueue::new(consumer.clone()));
        let next_stream_sequence = Arc::new(AtomicOptionNonZeroU64::new(None));
        let mut nats_consumer_config = translate_consumer_options(&config.consumer_config);

        while let Some(command) = command_receiver.recv().await {
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
        let _ = self.command_sender.send(command.as_nonft().unwrap());
    }

    fn is_closed(&self) -> bool {
        self.command_sender.is_closed()
    }
}
