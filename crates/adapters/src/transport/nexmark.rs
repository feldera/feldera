//! An input adapter that generates Nexmark event input data.

use std::collections::VecDeque;
use std::io::Cursor;
use std::mem;
use std::sync::atomic::AtomicUsize;
use std::sync::{atomic::Ordering, Arc, Mutex};
use std::sync::{Barrier, OnceLock, Weak};
use std::thread::{self, Thread};

use crate::format::{EmptyInputBuffer, InputBuffer};
use crate::transport::Step;
use crate::{
    InputConsumer, InputEndpoint, InputReader, Parser, PipelineState, TransportInputEndpoint,
};
use anyhow::{anyhow, Result as AnyResult};
use atomic::Atomic;
use csv::{Writer as CsvWriter, WriterBuilder as CsvWriterBuilder};
use dbsp_nexmark::generator::{NexmarkGenerator, NextEvent};
use dbsp_nexmark::model::Event;
use dbsp_nexmark::{config::GeneratorOptions, generator::config::Config as GeneratorConfig};
use enum_map::EnumMap;
use feldera_types::program_schema::Relation;
use feldera_types::transport::nexmark::{NexmarkInputConfig, NexmarkInputOptions, NexmarkTable};
use rand::rngs::ThreadRng;

pub(crate) struct NexmarkEndpoint {
    config: NexmarkInputConfig,
}

impl NexmarkEndpoint {
    pub(crate) fn new(config: NexmarkInputConfig) -> Self {
        Self { config }
    }
}

impl InputEndpoint for NexmarkEndpoint {
    fn is_fault_tolerant(&self) -> bool {
        false
    }
}

impl TransportInputEndpoint for NexmarkEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        _start_step: Step,
        _schema: Relation,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(InputGenerator::new(
            &self.config,
            consumer,
            parser,
        )?))
    }
}

struct InputGenerator {
    table: NexmarkTable,
    inner: Arc<Inner>,
}

impl InputGenerator {
    pub fn new(
        config: &NexmarkInputConfig,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
    ) -> AnyResult<Self> {
        let mut guard = INNER.lock().unwrap();
        let inner = guard.upgrade().unwrap_or_else(|| {
            let inner = Inner::new();
            *guard = Arc::downgrade(&inner);
            inner
        });
        drop(guard);

        inner.merge(config, consumer, parser)?;
        Ok(Self {
            table: config.table,
            inner,
        })
    }
}

impl InputReader for InputGenerator {
    fn start(&self, _step: Step) -> AnyResult<()> {
        self.inner.status[self.table].store(PipelineState::Running, Ordering::Release);
        let queued_batches = *self.inner.queued_batches.lock().unwrap();
        self.inner
            .consumable_batches
            .store(queued_batches, Ordering::Release);
        self.inner.unpark();
        Ok(())
    }

    fn pause(&self) -> AnyResult<()> {
        self.inner.status[self.table].store(PipelineState::Paused, Ordering::Release);
        Ok(())
    }

    fn disconnect(&self) {
        self.inner.status[self.table].store(PipelineState::Terminated, Ordering::Release);
        self.inner.unpark();
    }

    fn flush(&self, _n: usize) -> usize {
        // What batches are we permitted to flush?
        //
        // We need all of the tables to flush the events for the same event time
        // range in a given step. To do that, we assume that the controller will
        // call flush on each table exactly once per step, and we need each call
        // to flush to be able to consume the same batches. However, if we just
        // had each flush call look at the queue, then they might see different
        // batches because InputBuffers are queued asynchronously. Thus, we
        // introduce [Self::start], which is called once per step, as a
        // synchronization point, where it looks at the available batches and
        // saves it for `flush` to use here.
        let consumable_batches = self.inner.consumable_batches.load(Ordering::Acquire);
        let options = self.inner.options.get().unwrap();
        let max_batches = options
            .max_step_size_per_thread
            .div_ceil(options.batch_size_per_thread);

        let mut total = 0;
        for _ in 0..max_batches {
            // Find the earliest batch number and make sure that it's less than
            // `consumable_batches`.
            let mut buffers = Vec::with_capacity(options.threads);
            let mut queue = self.inner.queue.lock().unwrap();
            let batch_num = queue[self.table]
                .front()
                .map_or(usize::MAX, |front| front.0);
            if batch_num >= consumable_batches {
                break;
            }

            // Collect all the buffers in `batch_num` (there should be exactly
            // `n_threads` of them) and drop the lock.
            for _ in 0..options.threads {
                let (_batch_num, buffer) = queue[self.table].pop_front().unwrap();
                assert_eq!(_batch_num, batch_num);
                buffers.push(buffer);
            }
            drop(queue);

            // Flush it.
            for mut buffer in buffers {
                total += buffer.flush_all();
            }
        }
        total
    }
}

static INNER: Mutex<Weak<Inner>> = Mutex::new(Weak::new());

struct Inner {
    /// Status for each of the tables. We only run if all of them can run.
    status: EnumMap<NexmarkTable, Atomic<PipelineState>>,

    /// Options, which can be set from any of the tables but only from one of them.
    options: OnceLock<NexmarkInputOptions>,

    /// The per-table consumers and parsers.
    #[allow(clippy::type_complexity)]
    cps: Mutex<EnumMap<NexmarkTable, Option<(Box<dyn InputConsumer>, Box<dyn Parser>)>>>,

    #[allow(clippy::type_complexity)]
    queue: Mutex<EnumMap<NexmarkTable, VecDeque<(usize, Box<dyn InputBuffer>)>>>,

    consumable_batches: AtomicUsize,
    queued_batches: Mutex<usize>,

    /// The threads to wake up when we unpark.
    ///
    /// While we're waiting for the connectors for all the tables to be brought
    /// to the running state for the first time, this is only the worker
    /// thread. After that, we start the generator threads and they get
    /// populated here instead.
    threads: Mutex<Vec<Thread>>,
}

impl Inner {
    pub fn new() -> Arc<Self> {
        let inner = Arc::new(Self {
            status: EnumMap::from_fn(|_| Atomic::new(PipelineState::Paused)),
            options: OnceLock::new(),
            cps: Mutex::new(EnumMap::default()),
            queue: Mutex::new(EnumMap::from_fn(|_| VecDeque::new())),
            consumable_batches: AtomicUsize::new(0),
            queued_batches: Mutex::new(0),
            threads: Mutex::new(Vec::new()),
        });
        thread::Builder::new()
            .name(String::from("nexmark"))
            .spawn({
                let inner = Arc::clone(&inner);
                || inner.worker_thread()
            })
            .unwrap();
        inner
    }
    pub fn merge(
        &self,
        config: &NexmarkInputConfig,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
    ) -> AnyResult<()> {
        let mut tables = self.cps.lock().unwrap();
        if tables[config.table].is_some() {
            return Err(anyhow!(
                "more than one Nexmark input connector for {:?}",
                config.table
            ));
        }
        tables[config.table] = Some((consumer, parser));
        drop(tables);

        if let Some(options) = config.options.as_ref() {
            if self.options.set(options.clone()).is_err() {
                return Err(anyhow!(
                    "can't configure Nexmark options from two different connectors"
                ));
            }
        }

        Ok(())
    }

    pub fn unpark(&self) {
        for thread in self.threads.lock().unwrap().iter() {
            thread.unpark();
        }
    }

    /// Returns the pipeline's overall status based on the three underlying connectors:
    ///
    /// - We're terminated if any of the connectors are terminated.
    /// - Otherwise, we're paused if any of them are paused.
    /// - Otherwise, all of them are running, so we are running.
    ///
    /// To be honest, we could probably just use the status of the `bid`
    /// connector, because it's 92% of the records, and ignore the rest.
    fn status(&self) -> PipelineState {
        let mut state = PipelineState::Running;
        for (_table, status) in &self.status {
            let status = status.load(Ordering::Acquire);
            match status {
                PipelineState::Terminated => return PipelineState::Terminated,
                PipelineState::Paused => state = PipelineState::Paused,
                PipelineState::Running => (),
            }
        }
        state
    }

    /// Waits as long as the pipeline is paused, then returns `Ok` if we should
    /// run or `Err` if we should exit.
    fn wait_to_run(&self) -> Result<(), ()> {
        loop {
            match self.status() {
                PipelineState::Paused => thread::park(),
                PipelineState::Running => {
                    return Ok(());
                }
                PipelineState::Terminated => return Err(()),
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
    fn worker_thread(self: Arc<Self>) {
        // Wait until we're running.
        *self.threads.lock().unwrap() = vec![thread::current().clone()];
        if self.wait_to_run().is_err() {
            return;
        }

        // Grab the consumers. We know they're all there because `self.status()`
        // returned `PipelineStatus::Running`.
        let mut guard = self.cps.lock().unwrap();
        let mut cps = EnumMap::from_fn(|table| {
            let (consumer, parser) = guard[table].take().unwrap();
            (consumer, parser, Arc::new(AtomicUsize::new(0)))
        });
        drop(guard);

        // Start all the generator threads.
        let options = self.options.get_or_init(Default::default);
        let barrier = Arc::new(Barrier::new(options.threads));
        let generators = (0..options.threads)
            .map(|index| {
                let cps = EnumMap::from_fn(|table| {
                    let (consumer, parser, count) = &cps[table];
                    (consumer.clone(), parser.fork(), count.clone())
                });
                let barrier = barrier.clone();
                let inner = Arc::clone(&self);
                thread::Builder::new()
                    .name(format!("nexmark-{index}"))
                    .spawn(move || inner.generate_thread(cps, index, barrier))
                    .unwrap()
            })
            .collect::<Vec<_>>();
        drop(barrier);

        // Make sure all the generator threads can get unparked, and then unpark
        // them for the first time to avoid a missed wakeup.
        let threads = generators
            .iter()
            .map(|handle| handle.thread().clone())
            .collect::<Vec<_>>();
        *self.threads.lock().unwrap() = threads;
        self.unpark();

        // Let the generator threads run and wait for them to die.
        for handle in generators {
            handle.join().unwrap();
        }

        // Input is exhausted.
        for (_table, (consumer, parser, _count)) in cps.iter_mut() {
            parser.end_of_fragments();
            consumer.eoi();
        }
    }

    #[allow(clippy::type_complexity)]
    fn generate_thread(
        self: Arc<Self>,
        mut cps: EnumMap<NexmarkTable, (Box<dyn InputConsumer>, Box<dyn Parser>, Arc<AtomicUsize>)>,
        index: usize,
        barrier: Arc<Barrier>,
    ) {
        let options = self.options.get().unwrap();

        // Calculate the exact number of times to wait on `barrier`. If we wait
        // any fewer times than that, the other threads will get stuck (if we
        // wait more, we'll get stuck). It's harmless if it's greater than the
        // number of batches.
        let n_batches = options
            .events
            .div_ceil(options.batch_size_per_thread * options.threads as u64);

        let generator_options = GeneratorOptions {
            max_events: options.events,
            num_event_generators: options.threads,
            ..GeneratorOptions::default()
        };
        let mut generator = NexmarkGenerator::new(
            GeneratorConfig::new(generator_options, 0, 0, index),
            ThreadRng::default(),
            0,
        );

        let mut buffers = EnumMap::from_fn(|_| Vec::new());

        for i in 0..n_batches {
            // Wait until we're ready to run.
            if self.wait_to_run().is_err() {
                // Make sure we synchronize exactly `n_batches` times.
                for _ in i..n_batches {
                    barrier.wait();
                }
                return;
            }

            // Compose a batch into the writers.
            let mut writers =
                EnumMap::from_fn(|table| Self::make_csv_writer(mem::take(&mut buffers[table])));
            let mut n = 0;
            for NextEvent { event, .. } in &mut generator {
                match event {
                    Event::Person(person) => {
                        writers[NexmarkTable::Person].serialize(person).unwrap()
                    }
                    Event::Auction(auction) => {
                        writers[NexmarkTable::Auction].serialize(auction).unwrap()
                    }
                    Event::Bid(bid) => writers[NexmarkTable::Bid].serialize(bid).unwrap(),
                }
                n += 1;
                if n >= options.batch_size_per_thread {
                    break;
                }
            }

            // Parse the batch into per-table InputBuffers.
            let buffers = writers.map(|table, writer| {
                let data = writer.into_inner().unwrap().into_inner();
                let (_consumer, parser, count) = &mut cps[table];
                parser.input_chunk(data.as_slice());
                let buffer = parser.take().unwrap_or(Box::new(EmptyInputBuffer));
                count.fetch_add(buffer.len(), Ordering::SeqCst);
                buffer
            });

            // Append the batch to the queues.
            let mut queue = self.queue.lock().unwrap();
            for (table, buffer) in buffers {
                queue[table].push_back((i as usize, buffer))
            }
            drop(queue);

            // Synchronize with the other threads.
            if barrier.wait().is_leader() {
                let mut guard = self.queued_batches.lock().unwrap();
                for (_table, (consumer, _parser, count)) in &cps {
                    consumer.queued(0, count.swap(0, Ordering::SeqCst), Vec::new());
                }
                *guard += 1;
            }
        }
    }
}
