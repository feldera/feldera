//! An input adapter that generates Nexmark event input data.

use feldera_adapterlib::format::BufferSize;
use feldera_adapterlib::transport::{parse_resume_info, InputReaderCommand, Resume};
use feldera_types::config::FtModel;
use std::cmp::min;
use std::collections::VecDeque;
use std::hash::Hasher;
use std::io::Cursor;
use std::mem;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::sync::{Barrier, Weak};
use std::thread::{self};
use tokio::sync::broadcast::{channel as broadcast_channel, Receiver as BroadcastReceiver};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use xxhash_rust::xxh3::Xxh3Default;

use crate::format::InputBuffer;
use crate::{InputConsumer, InputEndpoint, InputReader, Parser, TransportInputEndpoint};
use anyhow::{anyhow, Result as AnyResult};
use csv::{Writer as CsvWriter, WriterBuilder as CsvWriterBuilder};
use dbsp_nexmark::generator::RandomGenerator;
use dbsp_nexmark::model::Event;
use dbsp_nexmark::{config::GeneratorOptions, generator::config::Config as GeneratorConfig};
use enum_map::EnumMap;
use feldera_types::program_schema::Relation;
use feldera_types::transport::nexmark::{NexmarkInputConfig, NexmarkInputOptions, NexmarkTable};
use rand::rngs::SmallRng;
use serde::{Deserialize, Serialize};

use super::InputCommandReceiver;

pub(crate) struct NexmarkEndpoint {
    config: NexmarkInputConfig,
}

impl NexmarkEndpoint {
    pub(crate) fn new(config: NexmarkInputConfig) -> Self {
        Self { config }
    }
}

impl InputEndpoint for NexmarkEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        Some(FtModel::ExactlyOnce)
    }
}

impl TransportInputEndpoint for NexmarkEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        _schema: Relation,
        resume_info: Option<serde_json::Value>,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(InputGenerator::new(
            &self.config,
            consumer,
            parser,
            resume_info,
        )?))
    }
}

struct InputGenerator {
    table: NexmarkTable,
    consumer: Box<dyn InputConsumer>,
    inner: Arc<Inner>,
}

impl InputGenerator {
    pub fn new(
        config: &NexmarkInputConfig,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        resume_info: Option<serde_json::Value>,
    ) -> AnyResult<Self> {
        let resume_info = if config.table != NexmarkTable::Bid {
            None
        } else if let Some(resume_info) = resume_info {
            Some(parse_resume_info::<Metadata>(&resume_info)?)
        } else {
            None
        };
        let mut guard = INNER.lock().unwrap();
        let inner = guard.upgrade().unwrap_or_else(|| {
            let inner = Arc::new(Inner::new());
            *guard = Arc::downgrade(&inner);
            inner
        });
        drop(guard);

        inner.configure(config, consumer.clone(), parser, resume_info)?;

        Ok(Self {
            table: config.table,
            consumer,
            inner,
        })
    }
}

impl InputReader for InputGenerator {
    fn request(&self, command: InputReaderCommand) {
        match self.table {
            NexmarkTable::Bid => {
                let _ = self.inner.command_sender.send(command);
            }
            _ => match command {
                InputReaderCommand::Replay { .. } => self.consumer.replayed(BufferSize::empty(), 0),
                InputReaderCommand::Extend => (),
                InputReaderCommand::Pause => (),
                InputReaderCommand::Queue { .. } => {
                    self.consumer.extended(
                        BufferSize::empty(),
                        Some(Resume::Replay {
                            seek: serde_json::Value::Null,
                            replay: rmpv::Value::Nil,
                            hash: 0,
                        }),
                    );
                }
                InputReaderCommand::Disconnect => (),
            },
        }
    }

    fn is_closed(&self) -> bool {
        false
    }
}

#[derive(Serialize, Deserialize)]
struct Metadata {
    event_ids: Range<u64>,
}

static INNER: Mutex<Weak<Inner>> = Mutex::new(Weak::new());

struct InnerInit {
    /// Options, which can be set from any of the tables but only from one of them.
    options: Option<NexmarkInputOptions>,

    /// The per-table parsers.
    parsers: EnumMap<NexmarkTable, Option<Box<dyn Parser>>>,

    /// The per-table consumers.
    consumers: EnumMap<NexmarkTable, Option<Box<dyn InputConsumer>>>,

    command_receiver: UnboundedReceiver<InputReaderCommand>,
}

impl InnerInit {
    pub fn new(command_receiver: UnboundedReceiver<InputReaderCommand>) -> Self {
        Self {
            options: None,
            parsers: EnumMap::default(),
            consumers: EnumMap::default(),
            command_receiver,
        }
    }
    pub fn configure(
        &mut self,
        config: &NexmarkInputConfig,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
    ) -> AnyResult<()> {
        if self.parsers[config.table].is_some() {
            return Err(anyhow!(
                "more than one Nexmark input connector for {:?}",
                config.table
            ));
        }
        self.parsers[config.table] = Some(parser);
        self.consumers[config.table] = Some(consumer);

        if config.options.is_some() {
            if self.options.is_some() {
                return Err(anyhow!(
                    "can't configure Nexmark options from two different connectors"
                ));
            }
            self.options = config.options.clone();
        }
        Ok(())
    }
    pub fn fully_configured(&self) -> bool {
        self.parsers.values().all(|p| p.is_some())
    }
    pub fn start_worker(self, resume_info: Option<Metadata>) {
        assert!(self.fully_configured());

        let Self {
            options,
            consumers,
            parsers,
            command_receiver,
        } = self;

        let options = options.unwrap_or_default();
        let consumers = consumers.map(|_table, consumer| consumer.unwrap());
        let parsers = parsers.map(|_table, parser| parser.unwrap());

        thread::Builder::new()
            .name(String::from("nexmark"))
            .spawn({
                move || {
                    if let Err(error) = worker_thread(
                        options,
                        parsers,
                        consumers.clone(),
                        command_receiver,
                        resume_info,
                    ) {
                        for consumer in consumers.values() {
                            consumer.error(true, anyhow!("{error}"));
                        }
                    }
                }
            })
            .unwrap();
    }
}

enum InnerStatus {
    Initializing(InnerInit),
    Running,
}

struct Inner {
    status: Mutex<InnerStatus>,
    command_sender: UnboundedSender<InputReaderCommand>,
}

impl Inner {
    pub fn new() -> Self {
        let (command_sender, command_receiver) = unbounded_channel();
        Self {
            status: Mutex::new(InnerStatus::Initializing(InnerInit::new(command_receiver))),
            command_sender,
        }
    }
    pub fn configure(
        &self,
        config: &NexmarkInputConfig,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        resume_info: Option<Metadata>,
    ) -> AnyResult<()> {
        let mut status = self.status.lock().unwrap();
        match &mut *status {
            InnerStatus::Running => Err(anyhow!(
                "more than one Nexmark input connector for {:?}",
                config.table
            )),
            InnerStatus::Initializing(init) => {
                init.configure(config, consumer, parser)?;
                if init.fully_configured() {
                    let InnerStatus::Initializing(init) =
                        mem::replace(&mut *status, InnerStatus::Running)
                    else {
                        unreachable!()
                    };
                    init.start_worker(resume_info);
                }
                Ok(())
            }
        }
    }
}

/// Returns a CSV writer with our style for `inner`.
fn make_csv_writer(inner: Vec<u8>) -> CsvWriter<Cursor<Vec<u8>>> {
    CsvWriterBuilder::new()
        .has_headers(false)
        .from_writer(Cursor::new(inner))
}

/// Main thread of the Nexmark connector.
fn worker_thread(
    options: NexmarkInputOptions,
    parsers: EnumMap<NexmarkTable, Box<dyn Parser>>,
    consumers: EnumMap<NexmarkTable, Box<dyn InputConsumer>>,
    command_receiver: UnboundedReceiver<InputReaderCommand>,
    resume_info: Option<Metadata>,
) -> AnyResult<()> {
    let mut command_receiver = InputCommandReceiver::<Metadata, ()>::new(command_receiver);

    // Start all the generator threads.
    let barrier = Arc::new(Barrier::new(options.threads + 1));
    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let (bcast_sender, _) = broadcast_channel(4);
    let generators = (0..options.threads)
        .map(|index| {
            let options = options.clone();
            let parsers = EnumMap::from_fn(|table| parsers[table].fork());
            let consumer = consumers[NexmarkTable::Bid].clone();
            let queue = queue.clone();
            let barrier = barrier.clone();
            let bcast_receiver = bcast_sender.subscribe();
            thread::Builder::new()
                .name(format!("nexmark-{index}"))
                .spawn(move || {
                    generate_thread(
                        options,
                        parsers,
                        consumer,
                        queue,
                        index,
                        barrier,
                        bcast_receiver,
                    )
                })
                .unwrap()
        })
        .collect::<Vec<_>>();
    drop(parsers);

    let mut next_event_id = if let Some(metadata) = resume_info {
        metadata.event_ids.end
    } else {
        0
    };

    while let Some((Metadata { event_ids }, ())) = command_receiver.blocking_recv_replay()? {
        let _ = bcast_sender.send(event_ids.clone());
        barrier.wait();
        let mut total = BufferSize::empty();
        let mut hasher = Xxh3Default::new();
        for (_events, mut buffer) in queue.lock().unwrap().drain(..) {
            total += buffer.len();
            buffer.hash(&mut hasher);
            buffer.flush();
        }
        consumers[NexmarkTable::Bid].replayed(total, hasher.finish());
        next_event_id = event_ids.end;
    }

    let batch_size = options.threads as u64 * options.batch_size_per_thread;
    let mut running = false;
    let mut eoi = false;
    loop {
        let command = if running && !eoi {
            command_receiver.try_recv()?
        } else {
            Some(command_receiver.blocking_recv()?)
        };
        match command {
            Some(InputReaderCommand::Replay { .. }) => {
                unreachable!("{command:?} must be at the beginning of the command stream")
            }
            Some(InputReaderCommand::Extend) => running = true,
            Some(InputReaderCommand::Pause) => running = false,
            Some(InputReaderCommand::Queue { .. }) => {
                let mut total = BufferSize::empty();
                let mut hasher = consumers[NexmarkTable::Bid].hasher();
                let n = options.max_step_size_per_thread as usize * options.threads;
                let mut events: Option<Range<u64>> = None;
                while total.records < n {
                    let mut queue = queue.lock().unwrap();
                    if queue.is_empty() {
                        break;
                    }
                    for _ in 0..3 * options.threads {
                        let (range, mut buffer) = queue.pop_front().unwrap();
                        events = match events {
                            Some(events) => Some(events.start..range.end),
                            None => Some(range),
                        };
                        total += buffer.len();
                        if let Some(hasher) = hasher.as_mut() {
                            buffer.hash(hasher);
                        }
                        buffer.flush();
                    }
                }
                let resume = Resume::new_metadata_only(
                    serde_json::to_value(Metadata {
                        event_ids: events.unwrap_or_else(|| next_event_id..next_event_id),
                    })
                    .unwrap(),
                    hasher,
                );
                consumers[NexmarkTable::Bid].extended(total, Some(resume));
            }
            Some(InputReaderCommand::Disconnect) => break,
            None => (),
        }

        if running {
            if next_event_id < options.events {
                let end = min(next_event_id + batch_size, options.events);
                let _ = bcast_sender.send(next_event_id..end);
                next_event_id = end;
                barrier.wait();
            } else if !eoi {
                eoi = true;
                for consumer in consumers.values() {
                    consumer.eoi();
                }
            }
        }
    }

    //  Wait for generator threads to die.
    drop(bcast_sender);
    for handle in generators {
        handle.join().unwrap();
    }

    Ok(())
}

#[allow(clippy::type_complexity)]
fn generate_thread(
    options: NexmarkInputOptions,
    mut parsers: EnumMap<NexmarkTable, Box<dyn Parser>>,
    consumer: Box<dyn InputConsumer>,
    queue: Arc<Mutex<VecDeque<(Range<u64>, Option<Box<dyn InputBuffer>>)>>>,
    index: usize,
    barrier: Arc<Barrier>,
    mut command_receiver: BroadcastReceiver<Range<u64>>,
) {
    let generator_options = GeneratorOptions {
        max_events: options.events,
        num_event_generators: 1,
        ..GeneratorOptions::default()
    };
    let generator = RandomGenerator::<SmallRng>::new(GeneratorConfig::new(generator_options, 0, 0));

    while let Ok(events) = command_receiver.blocking_recv() {
        let mut writers = EnumMap::from_fn(|_| make_csv_writer(Vec::new()));

        let stride = options.threads as u64;
        let mut event_id = events.start / stride * stride + index as u64;
        if event_id < events.start {
            event_id += stride;
        }
        while event_id < events.end {
            match generator.generate(event_id) {
                Event::Person(person) => writers[NexmarkTable::Person].serialize(person).unwrap(),
                Event::Auction(auction) => {
                    writers[NexmarkTable::Auction].serialize(auction).unwrap()
                }
                Event::Bid(bid) => writers[NexmarkTable::Bid].serialize(bid).unwrap(),
            }
            event_id += stride;
        }

        // Parse the batch into per-table InputBuffers.
        let buffers = writers
            .into_iter()
            .map(|(table, writer)| {
                let data = writer.into_inner().unwrap().into_inner();
                let parser = &mut parsers[table];
                let (buffer, _errors) = parser.parse(data.as_slice());
                consumer.buffered(buffer.len());
                buffer
            })
            .collect::<Vec<_>>();
        queue
            .lock()
            .unwrap()
            .extend(buffers.into_iter().map(|buffer| (events.clone(), buffer)));
        barrier.wait();
    }
}
