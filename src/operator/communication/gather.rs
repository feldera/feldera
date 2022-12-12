use crate::{
    circuit::{
        metadata::OperatorLocation,
        operator_traits::{Operator, SinkOperator, SourceOperator},
        GlobalNodeId, LocalStoreMarker, OwnershipPreference, Scope,
    },
    circuit_cache_key,
    trace::{spine_fueled::Spine, Batch, Trace},
    Circuit, Runtime, Stream,
};
use crossbeam::{
    atomic::AtomicConsume,
    channel::{Receiver, Sender},
};
use once_cell::sync::OnceCell;
use std::{
    borrow::Cow,
    hash::{Hash, Hasher},
    marker::PhantomData,
    panic::Location,
    ptr,
    sync::{
        atomic::{AtomicPtr, Ordering},
        Arc,
    },
};
use typedmap::TypedMapKey;

// TODO: We could use `ArcSwap<Box<dyn Fn() + ...>>` with a default noop function
// to remove some more sync/checking overhead
type NotifyCallback = Arc<OnceCell<Box<dyn Fn() + Send + Sync + 'static>>>;

circuit_cache_key!(GatherId<C, D>((GlobalNodeId, usize) => Stream<C, D>));

#[repr(transparent)]
struct GatherStoreId<T> {
    id: usize,
    __type: PhantomData<T>,
}

impl<T> GatherStoreId<T> {
    const fn new(id: usize) -> Self {
        Self {
            id,
            __type: PhantomData,
        }
    }
}

unsafe impl<T> Sync for GatherStoreId<T> {}

impl<T> Hash for GatherStoreId<T> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.id.hash(state);
    }
}

impl<T> PartialEq for GatherStoreId<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T> Eq for GatherStoreId<T> {}

impl<T> TypedMapKey<LocalStoreMarker> for GatherStoreId<T>
where
    T: 'static,
{
    type Value = (Sender<T>, Receiver<T>, NotifyCallback);
}

impl<P, B> Stream<Circuit<P>, B>
where
    P: Clone + 'static,
    B: 'static,
{
    /// Collect all shards of a stream at the same worker.
    ///
    /// The output stream in `receiver_worker` will contain a union of all
    /// input batches across all workers. The output streams in all other
    /// workers will contain empty batches.
    #[track_caller]
    pub fn gather(&self, receiver_worker: usize) -> Stream<Circuit<P>, B>
    where
        // FIXME: Remove `Time = ()` restriction currently imposed by `.consolidate()`
        B: Batch<Time = ()> + Send,
    {
        let location = Location::caller();

        match Runtime::runtime() {
            None => self.clone(),
            Some(runtime) => {
                let workers = runtime.num_workers();
                assert!(receiver_worker < workers);

                if workers == 1 {
                    self.clone()
                } else {
                    self.circuit()
                        .cache_get_or_insert_with(
                            GatherId::new((self.origin_node_id().clone(), receiver_worker)),
                            move || {
                                let current_worker = Runtime::worker_index();
                                let gather_id = runtime.sequence_next(current_worker);

                                let (tx, rx, ready_callback) = runtime
                                    .local_store()
                                    .entry(GatherStoreId::new(gather_id))
                                    .or_insert_with(|| {
                                        let (tx, rx) = crossbeam::channel::bounded(workers);
                                        (tx, rx, Arc::new(OnceCell::new()))
                                    })
                                    .value()
                                    .clone();

                                let (producer, consumer) = OneshotSpsc::new().split();
                                let sender =
                                    GatherProducer::new(producer, ready_callback.clone(), location);
                                tx.send(consumer).unwrap();

                                let gather_trace = if current_worker == receiver_worker {
                                    let mut consumers = Vec::with_capacity(workers);
                                    for _ in 0..workers {
                                        consumers.push(rx.recv().unwrap());
                                    }

                                    self.circuit().add_exchange(
                                        sender,
                                        GatherConsumer::new(
                                            consumers.into_boxed_slice(),
                                            ready_callback,
                                            location,
                                        ),
                                        self,
                                    )
                                } else {
                                    self.circuit().add_exchange(
                                        sender,
                                        EmptyGatherConsumer::new(location),
                                        self,
                                    )
                                };

                                // Is `consolidate` always necessary? Some (all?) consumers may be
                                // happy working with traces.
                                gather_trace.consolidate()
                            },
                        )
                        .clone()
                }
            }
        }
    }
}

struct GatherProducer<T> {
    queue: Producer<T>,
    notify: NotifyCallback,
    location: &'static Location<'static>,
}

impl<T> GatherProducer<T> {
    const fn new(
        queue: Producer<T>,
        notify: NotifyCallback,
        location: &'static Location<'static>,
    ) -> Self {
        Self {
            queue,
            notify,
            location,
        }
    }

    fn notify(&self) {
        (self.notify.get().unwrap())();
    }
}

impl<T> Operator for GatherProducer<T>
where
    T: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("GatherProducer")
    }

    fn location(&self) -> OperatorLocation {
        Some(self.location)
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T> SinkOperator<T> for GatherProducer<T>
where
    T: Clone + Send + 'static,
{
    fn eval(&mut self, input: &T) {
        self.queue.push(input.clone());
        self.notify();
    }

    fn eval_owned(&mut self, input: T) {
        self.queue.push(input);
        self.notify();
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }
}

struct GatherConsumer<T> {
    queues: Box<[Consumer<T>]>,
    ready_callback: NotifyCallback,
    location: &'static Location<'static>,
}

impl<T> GatherConsumer<T> {
    const fn new(
        queues: Box<[Consumer<T>]>,
        ready_callback: NotifyCallback,
        location: &'static Location<'static>,
    ) -> Self {
        Self {
            queues,
            ready_callback,
            location,
        }
    }

    fn all_consumers_full(&self) -> bool {
        !self.queues.iter().any(Consumer::is_empty)
    }
}

impl<T: 'static> Operator for GatherConsumer<T> {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("GatherConsumer")
    }

    fn location(&self) -> OperatorLocation {
        Some(self.location)
    }

    fn is_async(&self) -> bool {
        true
    }

    fn register_ready_callback<F>(&mut self, callback: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        let _ = self.ready_callback.set(Box::new(callback));
    }

    fn ready(&self) -> bool {
        self.all_consumers_full()
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T> SourceOperator<Spine<T>> for GatherConsumer<T>
where
    T: Batch + 'static,
    Spine<T>: Trace<Batch = T>,
{
    fn eval(&mut self) -> Spine<T> {
        debug_assert!(self.all_consumers_full());

        let mut spine = Spine::new(None);
        for consumer in self.queues.iter_mut() {
            let batch = *consumer
                .pop()
                .expect("GatherConsumer popped from empty channel");
            spine.insert(batch);
        }

        spine
    }
}

/// The consumer half of the gather operator that's given to all
/// the workers who aren't the target of the gather, simply yields
/// an empty trace on each clock cycle
struct EmptyGatherConsumer<T> {
    location: &'static Location<'static>,
    __type: PhantomData<T>,
}

impl<T> EmptyGatherConsumer<T> {
    const fn new(location: &'static Location<'static>) -> Self {
        Self {
            location,
            __type: PhantomData,
        }
    }
}

impl<T: 'static> Operator for EmptyGatherConsumer<T> {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("GatherConsumer")
    }

    fn location(&self) -> OperatorLocation {
        Some(self.location)
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T> SourceOperator<Spine<T>> for EmptyGatherConsumer<T>
where
    T: Batch + 'static,
    Spine<T>: Trace<Batch = T>,
{
    fn eval(&mut self) -> Spine<T> {
        Default::default()
    }
}

/// A very specialized oneshot single producer, single consumer channel
///
/// # Safety
///
/// Requires external synchronization via circuit epochs, the [`Producer`]
/// associated with a channel should only be pushed to once every clock cycle.
///
struct OneshotSpsc<T> {
    inner: AtomicPtr<T>,
}

impl<T> OneshotSpsc<T> {
    /// Create a new oneshot channel
    const fn new() -> Self {
        Self {
            inner: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Split the oneshot channel into producer and consumer halves
    fn split(self) -> (Producer<T>, Consumer<T>)
    where
        T: Send,
    {
        let this = Arc::new(self);

        (
            Producer {
                queue: this.clone(),
            },
            Consumer { queue: this },
        )
    }

    /// Load the value currently pointed to by the channel
    fn load(&self) -> *mut T {
        // Use consume ordering for loads
        self.inner.load_consume()
    }
}

impl<T> Drop for OneshotSpsc<T> {
    fn drop(&mut self) {
        let node = self.load();

        // If the channel is dropped while it contains a value, drop the value
        if !node.is_null() {
            let _ = unsafe { Box::from_raw(node) };
        }
    }
}

struct Producer<T> {
    queue: Arc<OneshotSpsc<T>>,
}

impl<T> Producer<T> {
    fn push(&mut self, value: T) {
        // There shouldn't be any value stored within the channel when we're pushing
        if cfg!(debug_assertions) {
            let prev_node = self.queue.load();
            assert_eq!(prev_node, ptr::null_mut());
        }

        let node = Box::into_raw(Box::new(value));
        self.queue.inner.store(node, Ordering::Release);
    }
}

struct Consumer<T> {
    queue: Arc<OneshotSpsc<T>>,
}

impl<T> Consumer<T> {
    /// Returns `true` if the channel is currently empty
    fn is_empty(&self) -> bool {
        self.queue.load().is_null()
    }

    fn pop(&mut self) -> Option<Box<T>> {
        // Load the value currently stored in the channel
        let node = self.queue.load();
        if node.is_null() {
            return None;
        }

        self.queue.inner.store(ptr::null_mut(), Ordering::Relaxed);
        unsafe { Some(Box::from_raw(node)) }
    }
}
