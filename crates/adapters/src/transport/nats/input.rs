use crate::{
    transport::{InputQueue, InputReaderCommand, NonFtInputReaderCommand},
    InputConsumer, InputEndpoint, InputReader, Parser, PipelineState, TransportInputEndpoint,
};
use anyhow::{Error as AnyError, Result as AnyResult};
use async_nats::{self, Client};
use dbsp::circuit::tokio::TOKIO;
use feldera_types::{config::FtModel, program_schema::Relation, transport::nats::NatsInputConfig};
use futures::StreamExt;
use std::{sync::Arc, thread};
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tracing::{info_span, Instrument};

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
        let subscription = TOKIO.block_on(Self::subscribe(&config).instrument(span.clone()))?;
        thread::spawn({
            move || {
                let consumer_clone = consumer.clone();
                TOKIO.block_on(async {
                    Self::worker_task(subscription, consumer_clone, parser, state_receiver)
                        .instrument(span)
                        .await
                        .unwrap_or_else(|e| consumer.error(true, e));
                })
            }
        });

        Ok(Self { state_sender })
    }

    async fn subscribe(config: &NatsInputConfig) -> Result<async_nats::Client, AnyError> {
        let client = async_nats::connect("localhost").await?;

        Ok(client)
    }

    async fn worker_task(
        client: Client,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        mut state_receiver: UnboundedReceiver<NonFtInputReaderCommand>,
    ) -> Result<(), AnyError> {
        let mut cancel: Option<JoinHandle<()>> = None;
        let queue = Arc::new(InputQueue::new(consumer.clone()));
        while let Some(command) = state_receiver.recv().await {
            match command {
                NonFtInputReaderCommand::Queue => queue.queue(),
                NonFtInputReaderCommand::Transition(state) => {
                    match state {
                        PipelineState::Paused => {
                            if let Some(handle) = cancel.take() {
                                // TODO Stop the subscription
                                let _ = handle.await;
                            }
                        }
                        PipelineState::Running => {
                            if cancel.is_none() {
                                let mut subscription = client.subscribe("sub".to_string()).await?;
                                let mut parser = parser.fork();
                                let handle = tokio::spawn({
                                    let queue = queue.clone();
                                    async move {
                                        while let Some(message) = subscription.next().await {
                                            let data = message.payload;
                                            queue.push(parser.parse(&data), data.len());
                                        }
                                    }
                                });
                                cancel = Some(handle);
                            }
                        }
                        PipelineState::Terminated => break,
                    }
                }
            }
        }
        if let Some(handle) = cancel.take() {
            // TODO Stop the subscription
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
