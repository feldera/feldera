use crate::{
    circuit::{
        metadata::OperatorLocation,
        operator_traits::{Operator, SinkOperator, SourceOperator},
        GlobalNodeId, OwnershipPreference, Scope,
    },
    circuit_cache_key,
    trace::{merge_batches, Batch},
    Circuit, Runtime, Stream,
};
use arc_swap::ArcSwap;
use crossbeam::atomic::AtomicConsume;
use crossbeam_utils::CachePadded;
use std::{
    borrow::Cow,
    marker::PhantomData,
    mem::MaybeUninit,
    panic::Location,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

type NotifyCallback = dyn Fn() + Send + Sync + 'static;

circuit_cache_key!(GatherId<C, D>((GlobalNodeId, usize) => Stream<C, D>));
circuit_cache_key!(local GatherDataId<T>(usize => Arc<GatherData<T>>));

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: Send + 'static,
{
    /// See [`Stream::gather`].
    #[track_caller]
    pub fn dyn_gather(&self, factories: &B::Factories, receiver_worker: usize) -> Stream<C, B>
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

                                let gather = runtime
                                    .local_store()
                                    .entry(GatherDataId::new(gather_id))
                                    .or_insert_with(|| Arc::new(GatherData::new(workers, location)))
                                    .value()
                                    .clone();

                                // Safety: The current worker is unique
                                let producer =
                                    unsafe { GatherProducer::new(gather.clone(), current_worker) };

                                if current_worker == receiver_worker {
                                    self.circuit().add_exchange(
                                        producer,
                                        GatherConsumer::new(gather, factories),
                                        self,
                                    )
                                } else {
                                    self.circuit().add_exchange(
                                        producer,
                                        EmptyGatherConsumer::new(location, factories),
                                        self,
                                    )
                                }
                            },
                        )
                        .clone()
                }
            }
        }
    }
}

struct GatherData<T> {
    is_valid: Box<[CachePadded<AtomicBool>]>,
    values: Box<[CachePadded<MaybeUninit<T>>]>,
    notify: ArcSwap<Box<NotifyCallback>>,
    location: &'static Location<'static>,
}

impl<T> GatherData<T> {
    fn new(length: usize, location: &'static Location<'static>) -> Self {
        fn noop_notify() {
            if cfg!(debug_assertions) {
                panic!("a notification callback was never set on a gather node");
            }
        }

        let is_valid = (0..length)
            .map(|_| CachePadded::new(AtomicBool::new(false)))
            .collect();

        let mut values = Vec::with_capacity(length);
        // Safety: `CachePadded<MaybeUninit<T>>` is valid to initialize as uninit
        #[allow(clippy::uninit_vec)]
        unsafe {
            values.set_len(length);
        }

        Self {
            is_valid,
            values: values.into_boxed_slice(),
            notify: ArcSwap::new(Arc::new(Box::new(noop_notify))),
            location,
        }
    }

    /// Return the total number of workers
    const fn workers(&self) -> usize {
        self.is_valid.len()
    }

    // Sets the notify callback for this gather operator
    fn set_notify(&self, notify: Box<NotifyCallback>) {
        self.notify.store(Arc::new(notify));
    }

    /// Returns `true` if all channels are filled
    ///
    /// # Safety
    ///
    /// The calling thread must be the gathering thread
    unsafe fn all_channels_ready(&self) -> bool {
        self.is_valid.iter().all(|is_valid| is_valid.load_consume())
    }

    /// Pushes a value to a channel
    ///
    /// # Safety
    ///
    /// `worker` must be a valid and unique channel index
    unsafe fn push(&self, worker: usize, value: T) {
        if cfg!(debug_assertions) {
            assert!(worker < self.values.len());

            // There shouldn't be any value stored within the channel when we're pushing
            let currently_filled = self.is_valid[worker].load_consume();
            assert!(!currently_filled);
        }

        unsafe {
            // Write the value to the slot
            (*(self.values.as_ptr().add(worker) as *mut CachePadded<MaybeUninit<T>>)).write(value);

            // Mark the slot as valid
            self.is_valid
                .get_unchecked(worker)
                .store(true, Ordering::Release);
        }

        // Notify any subscriber
        (self.notify.load())();
    }

    /// Pops a value from a channel
    ///
    /// # Safety
    ///
    /// - `worker` must be valid channel index
    /// - This must only be called from the gather thread
    /// - `worker`'s channel must be initialized
    unsafe fn pop(&self, worker: usize) -> T {
        debug_assert!(worker < self.values.len());

        unsafe {
            let slot_is_valid = self.is_valid.get_unchecked(worker);

            // Load the value currently stored in the channel (and synchronize against
            // previous writes)
            let is_valid = slot_is_valid.load_consume();
            debug_assert!(is_valid);

            // Read the value from the channel
            let value = self.values.get_unchecked(worker).assume_init_read();

            // Set the slot to be invalid
            slot_is_valid.store(false, Ordering::Relaxed);

            value
        }
    }
}

impl<T> Drop for GatherData<T> {
    fn drop(&mut self) {
        if cfg!(debug_assertions) && !std::thread::panicking() {
            assert!(
                !self.is_valid.iter().any(|is_valid| is_valid.load_consume()),
                "dropped a GatherData with values stored in its channel",
            );
        }

        assert!(self.is_valid.len() == self.values.len());
        for idx in 0..self.is_valid.len() {
            if self.is_valid[idx].load_consume() {
                // Safety: The value is initialized
                unsafe { self.values[idx].assume_init_drop() };
            }
        }
    }
}

unsafe impl<T: Send> Send for GatherData<T> {}
unsafe impl<T: Send> Sync for GatherData<T> {}

struct GatherProducer<T> {
    gather: Arc<GatherData<T>>,
    worker: usize,
}

impl<T> GatherProducer<T> {
    /// Create a new `GatherProducer`
    ///
    /// # Safety
    ///
    /// `worker` must be inbounds and unique within `gather`'s channels
    const unsafe fn new(gather: Arc<GatherData<T>>, worker: usize) -> Self {
        Self { gather, worker }
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
        Some(self.gather.location)
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
        // Safety: `worker` is guaranteed to be a valid & unique worker index
        unsafe { self.gather.push(self.worker, input.clone()) }
    }

    fn eval_owned(&mut self, input: T) {
        // Safety: `worker` is guaranteed to be a valid & unique worker index
        unsafe { self.gather.push(self.worker, input) }
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }
}

struct GatherConsumer<T: Batch> {
    factories: T::Factories,
    gather: Arc<GatherData<T>>,
}

impl<T: Batch> GatherConsumer<T> {
    fn new(gather: Arc<GatherData<T>>, factories: &T::Factories) -> Self {
        Self {
            gather,
            factories: factories.clone(),
        }
    }
}

impl<T: Batch + 'static> Operator for GatherConsumer<T> {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("GatherConsumer")
    }

    fn location(&self) -> OperatorLocation {
        Some(self.gather.location)
    }

    fn is_async(&self) -> bool {
        true
    }

    fn register_ready_callback<F>(&mut self, callback: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.gather.set_notify(Box::new(callback));
    }

    fn ready(&self) -> bool {
        // Safety: This is the gather thread
        unsafe { self.gather.all_channels_ready() }
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T> SourceOperator<T> for GatherConsumer<T>
where
    T: Batch<Time = ()> + 'static,
{
    fn eval(&mut self) -> T {
        // Safety: This is the gather thread
        debug_assert!(unsafe { self.gather.all_channels_ready() });

        merge_batches(
            &self.factories,
            (0..self.gather.workers()).map(|worker| unsafe { self.gather.pop(worker) }),
        )
    }
}

/// The consumer half of the gather operator that's given to all
/// the workers who aren't the target of the gather, simply yields
/// an empty trace on each clock cycle
struct EmptyGatherConsumer<T: Batch> {
    factories: T::Factories,
    location: &'static Location<'static>,
    __type: PhantomData<T>,
}

impl<T: Batch> EmptyGatherConsumer<T> {
    fn new(location: &'static Location<'static>, factories: &T::Factories) -> Self {
        Self {
            factories: factories.clone(),
            location,
            __type: PhantomData,
        }
    }
}

impl<T: Batch + 'static> Operator for EmptyGatherConsumer<T> {
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

impl<T> SourceOperator<T> for EmptyGatherConsumer<T>
where
    T: Batch<Time = ()> + 'static,
{
    fn eval(&mut self) -> T {
        T::dyn_empty(&self.factories, ())
    }
}
