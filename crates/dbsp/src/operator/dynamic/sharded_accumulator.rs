use std::{
    borrow::Cow,
    collections::VecDeque,
    iter::repeat_n,
    ops::Range,
    panic::Location,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, OnceLock},
};

use feldera_samply::Span;
use itertools::{Itertools as _, zip_eq};
use rkyv::AlignedVec;
use size_of::{HumanBytes, SizeOf, TotalSize};

use crate::{
    Circuit, NumEntries, Runtime, Scope, Stream,
    circuit::{
        GlobalNodeId, OwnershipPreference, StepSize, WorkerLocation, WorkerLocations,
        circuit_builder::StreamId,
        metadata::{
            ALLOCATED_MEMORY_BYTES, BatchSizeStats, INPUT_BATCHES_STATS, MEMORY_ALLOCATIONS_COUNT,
            MetaItem, OUTPUT_BATCHES_STATS, OperatorLocation, OperatorMeta, SHARED_MEMORY_BYTES,
            SPINE_COUNT, STATE_RECORDS_COUNT, USED_MEMORY_BYTES,
        },
        operator_traits::{Operator, SinkOperator, SourceOperator},
    },
    circuit_cache_key,
    operator::{
        communication::{
            ExchangeClients, ExchangeDelivery, ExchangeDirectory, ExchangeId, pop_flushed,
        },
        dynamic::shard_batch,
    },
    trace::{Batch, BatchReader as _, Spine, Trace, deserialize_indexed_wset},
};

circuit_cache_key!(local StreamingExchangeCacheId<B: Batch>(ExchangeId => Arc<ShardedAccumulator<B>>));

circuit_cache_key!(ShardedAccumulatorId<C, B: Batch>(StreamId => Stream<C, Option<Spine<B>>>));

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: Batch,
{
    /// Implements a fused shard-accumulator operation, equivalent to
    /// `self.dyn_shard().dyn_accumulate()` but intended to be more efficient.
    #[track_caller]
    pub fn dyn_shard_accumulate(&self, factories: &B::Factories) -> Stream<C, Option<Spine<B>>>
    where
        B: Batch<Time = ()>,
    {
        if let Some(sharded) = self.get_sharded_version() {
            sharded.dyn_accumulate(factories)
        } else if Runtime::num_workers() == 1 {
            self.dyn_accumulate(factories)
        } else {
            self.circuit()
                .cache_get_or_insert_with(ShardedAccumulatorId::new(self.stream_id()), || {
                    let runtime = Runtime::runtime().unwrap();
                    match runtime.get_step_size() {
                        StepSize::Microsteps => {
                            let exchange_id: ExchangeId =
                                runtime.sequence_next().try_into().unwrap();
                            let exchange = ShardedAccumulator::<B>::with_runtime(
                                &runtime,
                                Runtime::worker_index(),
                                exchange_id,
                                factories,
                            );
                            self.circuit()
                                .add_exchange(
                                    ShardedAccumulatorSender::new(
                                        Some(Location::caller()),
                                        exchange.clone(),
                                    ),
                                    ShardedAccumulatorReceiver::new(
                                        Some(Location::caller()),
                                        exchange,
                                    ),
                                    self,
                                )
                                .mark_sharded()
                        }
                        StepSize::FullSteps => self.dyn_shard(factories).dyn_accumulate(factories),
                    }
                })
                .clone()
        }
    }
}

struct ShardedAccumulator<B>
where
    B: Batch,
{
    receiver_global_node_id: OnceLock<Arc<String>>,
    exchange_id: ExchangeId,

    /// The number of communicating peers.
    npeers: usize,

    factories: B::Factories,

    /// Range of worker IDs on the local host.
    local_workers: Range<usize>,

    /// The RPC clients to contact remote hosts.
    clients: Arc<ExchangeClients>,

    rxq: Vec<Mutex<Rxq<B>>>,
}

impl<B> ShardedAccumulator<B>
where
    B: Batch<Time = ()>,
{
    fn with_runtime(
        runtime: &Runtime,
        worker_index: usize,
        exchange_id: ExchangeId,
        factories: &B::Factories,
    ) -> Arc<Self> {
        // It's tempting to move the following calls to create the
        // `ExchangeDirectory` and `ExchangeClients` into
        // `ShardedAccumulator::new`, but don't do it: all three of these access
        // `runtime.local_store` and nesting them creates deadlocks at runtime.
        let directory = ExchangeDirectory::for_runtime(runtime);
        let clients = ExchangeClients::for_runtime(runtime);
        runtime
            .local_store()
            .entry(StreamingExchangeCacheId::new(exchange_id))
            .or_insert_with(|| {
                ShardedAccumulator::new(
                    runtime,
                    worker_index,
                    clients,
                    exchange_id,
                    &directory,
                    factories,
                )
            })
            .value()
            .clone()
    }

    /// Create a new streaming exchange operator for `npeers` communicating threads.
    fn new(
        runtime: &Runtime,
        worker_index: usize,
        clients: Arc<ExchangeClients>,
        exchange_id: ExchangeId,
        directory: &ExchangeDirectory,
        factories: &B::Factories,
    ) -> Arc<Self> {
        let layout = runtime.layout();
        let npeers = layout.n_workers();

        let exchange = Arc::new(Self {
            exchange_id,
            receiver_global_node_id: Default::default(),
            npeers,
            local_workers: layout.local_workers(),
            factories: factories.clone(),
            clients,
            rxq: layout
                .local_workers()
                .map(|_| Mutex::new(Rxq::new(runtime, worker_index, factories, npeers)))
                .collect(),
        });

        directory.insert(exchange_id, exchange.clone());

        exchange
    }

    fn rxq(&self, receiver: usize) -> MutexGuard<'_, Rxq<B>> {
        assert!(self.local_workers.contains(&receiver));
        self.rxq[receiver - self.local_workers.start]
            .lock()
            .unwrap()
    }

    /// Delivers `batch`, sent by local or remote `sender`, to local worker
    /// `receiver`.  If `flush` is true, this is the final batch in the
    /// transaction.  Returns true if the receiving spine has too many batches.
    fn deliver(
        &self,
        factories: &B::Factories,
        sender: usize,
        receiver: usize,
        batch: B,
        flush: bool,
    ) -> bool {
        // Spill the batch to disk, if we should, without taking the rxq lock.
        let batch = Spine::maybe_flush_batch(batch, factories, || (None, None));
        if flush || !batch.is_empty() {
            self.rxq(receiver).deliver(factories, sender, batch, flush)
        } else {
            false
        }
    }

    async fn send(self: &Arc<Self>, global_node_id: &Arc<String>, batch: B, flush: bool) {
        let sender = Runtime::worker_index();

        let runtime = Runtime::runtime().unwrap();
        let layout = runtime.layout();
        let mut builders = Vec::with_capacity(layout.n_workers());
        let mut batches = Vec::with_capacity(layout.n_workers());
        shard_batch(
            batch,
            &(0..self.npeers),
            &mut builders,
            &mut batches,
            &self.factories,
        );
        let worker_locations = WorkerLocations::for_layout(layout);
        let mut data = batches.into_iter();
        let mut remote_waiters = Vec::new();
        let mut local_waiters = Vec::new();
        let mut serialized_bytes = 0;
        for receivers in layout.all_hosts() {
            match worker_locations[receivers.start] {
                WorkerLocation::Local => {
                    for receiver in receivers.clone() {
                        let item = data
                            .next()
                            .expect("data should include one item per peer")
                            .into_plain()
                            .expect("local data should not be serialized");
                        if self.deliver(&self.factories, sender, receiver, item, flush)
                            && !flush
                            && let Some(waiter) = self
                                .rxq(receiver)
                                .spines
                                .back()
                                .unwrap()
                                .spine
                                .backpressure_waiter()
                        {
                            local_waiters.push((receiver, waiter));
                        }
                    }
                }
                WorkerLocation::Remote => {
                    let items = receivers
                        .clone()
                        .map(|_| {
                            let mut fbuf = data
                                .next()
                                .expect("data should include one item per peer")
                                .into_tx()
                                .expect("remote mailboxes should always be serialized");
                            fbuf.push(flush as u8);
                            fbuf
                        })
                        .inspect(|serialized| {
                            serialized_bytes += serialized.len();
                        })
                        .collect_vec();
                    let this = self.clone();
                    if let Some(waiter) = this.clients.connect(receivers.start).await.send(
                        global_node_id.clone(),
                        this.exchange_id,
                        sender,
                        items,
                    ) {
                        remote_waiters.push(waiter);
                    }
                }
            }
        }

        if !local_waiters.is_empty() {
            let _span = Span::new("local send wait")
                .with_category("Exchange")
                .with_tooltip(|| {
                    format!(
                        "{global_node_id} wait for batches to merge in {} receive queues (for workers {})",
                        local_waiters.len(),
                        local_waiters
                            .iter()
                            .map(|(receiver, _waiter)| receiver)
                            .format(", ")
                    )
                });
            for (_receiver, waiter) in local_waiters {
                waiter.await;
            }
        }
        if !remote_waiters.is_empty() {
            let _span = Span::new("remote send wait")
                .with_category("Exchange")
                .with_tooltip(|| {
                    format!(
                        "{global_node_id} wait for {} to drain from {} tx buffers",
                        HumanBytes::from(serialized_bytes),
                        remote_waiters.len()
                    )
                });
            for waiter in remote_waiters {
                waiter.wait().await;
            }
        }
    }

    fn receive(&self) -> Option<Spine<B>> {
        let receiver = Runtime::worker_index();
        self.rxq(receiver).receive()
    }
}

impl<B> ExchangeDelivery for ShardedAccumulator<B>
where
    B: Batch<Time = ()>,
{
    fn name(&self) -> &str {
        self.receiver_global_node_id.get().unwrap()
    }

    fn received<'a>(
        &'a self,
        sender: usize,
        data: Vec<AlignedVec>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            for (receiver, mut data) in zip_eq(self.local_workers.clone(), data) {
                let flush = pop_flushed(&mut data);
                let batch = deserialize_indexed_wset(&self.factories, &data);
                self.deliver(&self.factories, sender, receiver, batch, flush);
            }
        })
    }
}

/// Queues data to a [ShardedAccumulatorReceiver].
struct Rxq<B>
where
    B: Batch,
{
    /// The runtime in which we're embedded.
    runtime: Runtime,

    /// Our worker index within the runtime.
    worker_index: usize,

    /// Total number of worker threads in this circuit.
    npeers: usize,

    /// A deque of spines under construction for delivery to the circuit.  The
    /// first element will be delivered to the circuit next, the second element
    /// after that, and so on.
    ///
    /// In the common case, the deque has one element; it will never be empty.
    spines: VecDeque<RxqEntry<B>>,

    /// For each sender, the number of flushes it has sent.
    n_flushes: Vec<usize>,

    /// The number of entries that have been popped off `spines` and received by
    /// the circuit.
    n_received: usize,
}

/// A spine that a [ShardedAccumulatorReceiver] is building from batches
/// received from [ShardedAccumulatorSender]s.
struct RxqEntry<B>
where
    B: Batch,
{
    /// Number of senders that have not yet sent a flush notification.  This
    /// starts as the number of workers in the circuit and decrements with each
    /// flush.  When it reaches zero, every sender has sent a flush
    /// notification, so `spine` is ready to be delivered to the circuit.
    n_unflushed: usize,

    /// Spine under construction.
    spine: Spine<B>,
}

impl<B> RxqEntry<B>
where
    B: Batch,
{
    fn new(
        npeers: usize,
        runtime: &Runtime,
        worker_index: usize,
        factories: &B::Factories,
    ) -> Self {
        Self {
            n_unflushed: npeers,
            spine: Spine::with_runtime(runtime.clone(), worker_index, factories),
        }
    }
}

impl<B> Rxq<B>
where
    B: Batch,
{
    fn new(
        runtime: &Runtime,
        worker_index: usize,
        factories: &B::Factories,
        npeers: usize,
    ) -> Self {
        Self {
            runtime: runtime.clone(),
            worker_index,
            npeers,
            spines: VecDeque::from([RxqEntry::new(npeers, runtime, worker_index, factories)]),
            n_flushes: repeat_n(0, npeers).collect(),
            n_received: 0,
        }
    }

    fn deliver(
        &mut self,
        factories: &B::Factories,
        sender: usize,
        batch: Arc<B>,
        flush: bool,
    ) -> bool {
        let index = self.n_flushes[sender] - self.n_received;
        let entry = &mut self.spines[index];

        let should_block = entry.spine.insert_without_blocking(batch);

        if flush {
            entry.n_unflushed -= 1;
            self.n_flushes[sender] += 1;
            if index + 1 >= self.spines.len() {
                self.spines.push_back(RxqEntry::new(
                    self.npeers,
                    &self.runtime,
                    self.worker_index,
                    factories,
                ));
            }
        }

        should_block
    }

    fn receive(&mut self) -> Option<Spine<B>> {
        self.spines
            .pop_front_if(|entry| entry.n_unflushed == 0)
            .map(|entry| {
                self.n_received += 1;
                entry.spine
            })
    }
}

struct ShardedAccumulatorSender<B>
where
    B: Batch,
{
    location: OperatorLocation,
    global_node_id: Arc<String>,
    exchange: Arc<ShardedAccumulator<B>>,

    // Input batch sizes.
    input_batch_stats: BatchSizeStats,

    flushed: bool,
}

impl<B> ShardedAccumulatorSender<B>
where
    B: Batch,
{
    fn new(location: OperatorLocation, exchange: Arc<ShardedAccumulator<B>>) -> Self {
        Self {
            location,
            global_node_id: Arc::new(format!("ShardedAccumulatorSender {}", exchange.exchange_id)),
            exchange,
            input_batch_stats: BatchSizeStats::new(),
            flushed: false,
        }
    }
}

impl<B> Operator for ShardedAccumulatorSender<B>
where
    B: Batch,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("ShardedAccumulatorSender")
    }

    fn init(&mut self, global_id: &GlobalNodeId) {
        self.global_node_id = Arc::new(format!(
            "ShardedAccumulatorSender {}",
            global_id.node_identifier()
        ));
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        meta.extend(metadata! {
            INPUT_BATCHES_STATS => self.input_batch_stats.metadata(),
        });
    }

    fn location(&self) -> OperatorLocation {
        self.location
    }

    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }

    fn flush(&mut self) {
        self.flushed = true;
    }
}

impl<B> SinkOperator<B> for ShardedAccumulatorSender<B>
where
    B: Batch<Time = ()>,
{
    async fn eval(&mut self, batch: &B) {
        self.eval_owned(batch.clone()).await
    }

    async fn eval_owned(&mut self, batch: B) {
        self.input_batch_stats.add_batch(batch.num_entries_deep());
        self.exchange
            .send(&self.global_node_id, batch, self.flushed)
            .await;
        self.flushed = false;
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }
}

struct ShardedAccumulatorReceiver<B>
where
    B: Batch,
{
    exchange: Arc<ShardedAccumulator<B>>,
    output_batch_stats: BatchSizeStats,
    location: OperatorLocation,
    flushed: bool,
}

impl<B> ShardedAccumulatorReceiver<B>
where
    B: Batch,
{
    fn new(location: OperatorLocation, exchange: Arc<ShardedAccumulator<B>>) -> Self {
        Self {
            exchange,
            output_batch_stats: BatchSizeStats::new(),
            location,
            flushed: false,
        }
    }
}

impl<B> Operator for ShardedAccumulatorReceiver<B>
where
    B: Batch<Time = ()>,
{
    fn name(&self) -> std::borrow::Cow<'static, str> {
        Cow::Borrowed("ShardedAccumulatorReceiver")
    }

    fn init(&mut self, global_id: &GlobalNodeId) {
        let _ = self
            .exchange
            .receiver_global_node_id
            .set(Arc::new(global_id.node_identifier().to_string()));
    }

    fn location(&self) -> OperatorLocation {
        self.location
    }

    fn fixedpoint(&self, _scope: crate::circuit::Scope) -> bool {
        true
    }

    fn flush(&mut self) {
        self.flushed = false;
    }

    fn is_flush_complete(&self) -> bool {
        self.flushed
    }

    fn ready(&self) -> bool {
        // This operator does not fit well into the DBSP evaluation model.  It
        // only has anything useful to contribute when it has received a flush.
        // At any other time, it can immediately evaluate to `None`.  However:
        //
        // - If it only reports that it is ready when it has received a flush,
        //   then it will prevent the step from completing until it has.  This
        //   will deadlock because the flush will never be received (since
        //   generally it takes more than one step to flush).
        //
        // - If it does what it does here, and always reports that it is ready,
        //   then this could cause livelock, spinning uselessly with 100% CPU,
        //   if there's nothing for the circuit to do while data transmits.
        //
        // I don't know the right solution.
        true
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        let rxq = self.exchange.rxq(Runtime::worker_index());

        let mut total_size = 0;
        let mut bytes = TotalSize::zero();
        let mut n_spines = 0;
        for (index, spine) in rxq.spines.iter().map(|entry| &entry.spine).enumerate() {
            if index == 0 {
                spine.metadata(meta);
            }
            n_spines += 1;
            total_size += spine.num_entries_deep();
            bytes += spine.size_of();
        }

        meta.extend(metadata! {
            SPINE_COUNT =>  MetaItem::Count(n_spines),
            STATE_RECORDS_COUNT => MetaItem::Count(total_size),
            ALLOCATED_MEMORY_BYTES => MetaItem::bytes(bytes.total_bytes()),
            USED_MEMORY_BYTES => MetaItem::bytes(bytes.used_bytes()),
            MEMORY_ALLOCATIONS_COUNT => MetaItem::Count(bytes.distinct_allocations()),
            SHARED_MEMORY_BYTES => MetaItem::bytes(bytes.shared_bytes()),
            OUTPUT_BATCHES_STATS => self.output_batch_stats.metadata(),
        });
    }
}

impl<B> SourceOperator<Option<Spine<B>>> for ShardedAccumulatorReceiver<B>
where
    B: Batch<Time = ()>,
{
    async fn eval(&mut self) -> Option<Spine<B>> {
        let output = self.exchange.receive();
        if let Some(spine) = &output {
            self.output_batch_stats.add_batch(spine.len());
            spine.backpressure_wait().await;
            self.flushed = true;
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use crossbeam::thread;
    use itertools::Itertools;

    use crate::{
        DBSPHandle, OutputHandle, RootCircuit, ZSetHandle, ZWeight,
        circuit::{CircuitConfig, Layout, Runtime},
        dynamic::{Data, DowncastTrait, DynWeightTyped},
        trace::{BatchReader, Cursor, FallbackWSet, Spine},
        typed_batch::TypedBatch,
    };
    use std::{collections::BTreeMap, iter::zip, net::TcpListener};

    /// Number of rounds for streaming exchange.
    ///
    /// We do fewer rounds for streaming exchange because the `n`th round does
    /// `n` steps and exchanges `O(n**2)` data.
    const STREAMING_ROUNDS: usize = 64;

    fn test_circuit(workers: usize, hosts: usize) {
        let (mut dbsp_handles, input_handles, output_handles) = match hosts {
            0 => unreachable!(),
            1 => {
                let (dbsp_handle, (input_handle, output_handle)) =
                    Runtime::init_circuit(workers, circuit).expect("failed to start runtime");
                (vec![dbsp_handle], vec![input_handle], vec![output_handle])
            }
            _ => {
                assert!(workers >= hosts);

                // Bind some listening sockets.
                let exchange_listeners = (0..hosts)
                    .map(|_| {
                        TcpListener::bind("127.0.0.1:0")
                            .expect("should be able to bind a port on localhost")
                    })
                    .collect_vec();

                // Assemble the listening sockets' addresses into something we can pass
                // to `Layout::new_multihost`.
                let params = exchange_listeners
                    .iter()
                    .enumerate()
                    .map(|(index, listener)| {
                        (
                            listener
                                .local_addr()
                                .expect("should be able to get local address"),
                            workers / hosts + (index < workers % hosts) as usize,
                        )
                    })
                    .collect_vec();

                // Create the runtimes.
                let mut handles = Vec::with_capacity(params.len());
                for ((local_address, _), exchange_listener) in
                    zip(params.iter(), exchange_listeners)
                {
                    let cconf = CircuitConfig::from(
                        Layout::new_multihost(&params, *local_address).unwrap(),
                    )
                    .with_exchange_listener(exchange_listener);

                    let (dbsp_handle, (input_handle, output_handle)) =
                        Runtime::init_circuit(cconf, circuit).expect("failed to start runtime");
                    handles.push((dbsp_handle, input_handle, output_handle));
                }
                handles.into_iter().multiunzip()
            }
        };

        /// Executes `f` on all of the handles in `dbsp_handles` in parallel and
        /// waits for them to complete.
        fn for_each_host<F>(dbsp_handles: &mut [DBSPHandle], f: F)
        where
            F: Fn(&mut DBSPHandle) + Send + Sync + 'static,
        {
            thread::scope(|s| {
                dbsp_handles
                    .iter_mut()
                    .map(|h| s.spawn(|_| f(h)))
                    .collect_vec()
                    .into_iter()
                    .for_each(|h| h.join().unwrap())
            })
            .unwrap();
        }

        for round in 0..STREAMING_ROUNDS {
            for_each_host(&mut dbsp_handles, |h| h.start_transaction().unwrap());

            for i in 0..=round {
                input_handles[i % hosts].push(i, 1);
                for_each_host(&mut dbsp_handles, |h| {
                    h.step().unwrap();
                });
            }
            for_each_host(&mut dbsp_handles, |h| h.commit_transaction().unwrap());

            let mut results = BTreeMap::<usize, ZWeight>::new();
            for spine in output_handles
                .iter()
                .flat_map(|handle| handle.take_from_all())
            {
                let mut cursor = spine.cursor();
                while let Some(key) = cursor.get_key() {
                    let key = *unsafe { key.downcast() };
                    let weight = *unsafe { cursor.weight().downcast::<ZWeight>() };
                    *results.entry(key).or_default() += weight;
                    cursor.step_key();
                }
            }
            let results = results.into_iter().collect_vec();
            let expected = (0..=round).map(|i| (i, 1)).collect_vec();
            assert_eq!(&results, &expected);
        }
    }

    fn circuit(
        circuit: &mut RootCircuit,
    ) -> anyhow::Result<(
        ZSetHandle<usize>,
        OutputHandle<
            TypedBatch<
                usize,
                (),
                i64,
                Spine<FallbackWSet<dyn Data + 'static, DynWeightTyped<i64>>>,
            >,
        >,
    )> {
        let (input, input_handle) = circuit.add_input_zset::<usize>();
        let output_handle = input.shard_accumulate().latest_output();
        Ok((input_handle, output_handle))
    }

    // Create a circuit with `WORKERS` concurrent workers with the following
    // structure: `Generator - ExchangeSender -> ExchangeReceiver -> Inspect`.
    // `Generator` - yields sequential numbers 0, 1, 2, ...
    // `ExchangeSender` - sends each number to all peers.
    // `ExchangeReceiver` - combines all received numbers in a vector.
    // `Inspect` - validates the output of the receiver.
    #[test]
    fn sharded_accumulator_single_host() {
        for workers in [2, 16, 32] {
            test_circuit(workers, 1);
        }
    }

    #[test]
    fn sharded_accumulator_multihost() {
        for (workers, hosts) in [(2, 2), (4, 2), (8, 2), (3, 3), (4, 4), (16, 4)] {
            test_circuit(workers, hosts);
        }
    }
}
