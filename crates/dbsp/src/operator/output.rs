use super::Mailbox;
use crate::{
    circuit::{
        operator_traits::{BinarySinkOperator, Operator, SinkOperator},
        LocalStoreMarker, OwnershipPreference, RootCircuit, Scope,
    },
    trace::BatchReaderFactories,
    Batch, Circuit, Runtime, Stream,
};
use std::{
    borrow::Cow,
    fmt::Debug,
    hash::{Hash, Hasher},
    intrinsics::transmute,
    marker::PhantomData,
    sync::Arc,
};
use typedmap::TypedMapKey;

impl<T> Stream<RootCircuit, T>
where
    T: Debug + Clone + Send + 'static,
{
    /// Create an output handle that makes the contents of `self` available
    /// outside the circuit.
    ///
    /// This API makes the result of the computation performed by the circuit
    /// available to the outside world.  At each clock cycle, the contents
    /// of the stream is buffered inside the handle and can be read using
    /// the [`OutputHandle`] API.
    pub fn output(&self) -> OutputHandle<T> {
        let (output, output_handle) = Output::new();
        self.circuit().add_sink(output, self);
        output_handle
    }

    /// Create an output handle that makes the contents of `self` available
    /// outside the circuit on demand.
    ///
    /// This operator is similar to [`output`](`Self::output`), but it only
    /// produces the output conditionally, when the value in the `guard` stream
    /// is `true`.  When `guard` is false, the output mailbox remains empty at
    /// the end of the clock cycle, and [`OutputHandle::take_from_worker`] will
    /// return `None`.  This operator can be used to output a large collection,
    /// such as an integral of a stream, on demand.
    pub fn output_guarded(&self, guard: &Stream<RootCircuit, bool>) -> OutputHandle<T> {
        let (output, output_handle) = OutputGuarded::new();
        self.circuit().add_binary_sink(output, self, guard);
        output_handle
    }
}

/// `TypedMapKey` entry used to share `OutputHandle` objects across workers in a
/// runtime. The first worker to create the handle will store it in the map,
/// subsequent workers will get a clone of the same handle.
struct OutputId<T> {
    id: usize,
    _marker: PhantomData<T>,
}

unsafe impl<T> Sync for OutputId<T> {}

// Implement `Hash`, `Eq` manually to avoid `T: Hash` type bound.
impl<T> Hash for OutputId<T> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.id.hash(state);
    }
}

impl<T> PartialEq for OutputId<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T> Eq for OutputId<T> {}

impl<T> OutputId<T> {
    fn new(id: usize) -> Self {
        Self {
            id,
            _marker: PhantomData,
        }
    }
}

impl<T> TypedMapKey<LocalStoreMarker> for OutputId<T>
where
    T: 'static,
{
    type Value = OutputHandle<T>;
}

struct OutputHandleInternal<T> {
    mailbox: Vec<Mailbox<Option<T>>>,
}

impl<T: Clone> OutputHandleInternal<T> {
    fn new(num_workers: usize) -> Self {
        assert_ne!(num_workers, 0);

        let mut mailbox = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            mailbox.push(Mailbox::new(Arc::new(|| None)));
        }

        Self { mailbox }
    }

    fn take_from_worker(&self, worker: usize) -> Option<T> {
        self.mailbox[worker].take()
    }

    fn peek_from_worker<F, O: 'static>(&self, worker: usize, func: F) -> O
    where
        F: Fn(&Option<T>) -> O,
    {
        self.mailbox[worker].map(func)
    }

    fn mailbox(&self, worker: usize) -> &Mailbox<Option<T>> {
        &self.mailbox[worker]
    }
}

/// A handle used to read data from a stream from outside the circuit.
///
/// Internally, the handle manages an array of mailboxes, one for
/// each worker thread.  At the end of each clock cycle, the worker
/// writes the current contents of the stream to the mailbox.
///
/// Once the clock cycle ends (i.e., the
/// [`DBSPHandle::step`](`crate::DBSPHandle::step`) method
/// returns), each mailbox contains a single value -- a copy of
/// stream contents at the current clock cycle.
///
/// The client retrieves values produced by individual workers using
/// the [`take_from_worker`](`OutputHandle::take_from_worker`) method.
/// Alternatively they can retrieve values from all mailboxes at once
/// using [`take_from_all`](`OutputHandle::take_from_all`).
/// If the stream carries relational data, the
/// [`consolidate`](`OutputHandle::consolidate`) method can be used
/// to combine output batches produced by all workes into a single
/// batch.
///
/// Reading from a mailbox using any of these methods removes the value
/// leaving the mailbox empty.  If the value is not read, it gets
/// overwritten at the next clock cycle (i.e., during the next call to
/// `step`).
#[derive(Clone)]
pub struct OutputHandle<T>(Arc<OutputHandleInternal<T>>);

impl<T> OutputHandle<T>
where
    T: Send + Clone + 'static,
{
    fn new() -> Self {
        match Runtime::runtime() {
            None => Self(Arc::new(OutputHandleInternal::new(1))),
            Some(runtime) => {
                let output_id = runtime.sequence_next(Runtime::worker_index());

                runtime
                    .local_store()
                    .entry(OutputId::new(output_id))
                    .or_insert_with(|| {
                        Self(Arc::new(OutputHandleInternal::new(runtime.num_workers())))
                    })
                    .value()
                    .clone()
            }
        }
    }

    fn mailbox(&self, worker: usize) -> &Mailbox<Option<T>> {
        self.0.mailbox(worker)
    }

    /// The number of mailboxes that contain values that haven't been retrieved
    /// yet.
    pub fn num_nonempty_mailboxes(&self) -> usize {
        let num_workers = self.0.mailbox.len();
        let mut non_empty = 0;

        for worker in 0..num_workers {
            non_empty += self.peek_from_worker(worker, Option::is_some) as usize;
        }

        non_empty
    }

    pub fn peek_from_worker<F, O: 'static>(&self, worker: usize, func: F) -> O
    where
        F: Fn(&Option<T>) -> O,
    {
        self.0.peek_from_worker(worker, func)
    }

    /// Read the value produced by `worker` worker thread during the last
    /// clock cycle.
    ///
    /// This method is invoked between two consecutive
    /// [`DBSPHandle::step`](`crate::DBSPHandle::step`)
    /// calls to retrieve the value written to the stream during the last
    /// clock cycle, if any.  It removes the value from the
    /// mailbox, so subsequent calls will return `None`.
    ///
    /// Invoking this method in the middle of a clock cycle, i.e., during
    /// the execution of `DBSPHandle::step`, may return either `None` or
    /// `Some`, depending of whether a value has already been pushed to
    /// the stream.
    pub fn take_from_worker(&self, worker: usize) -> Option<T> {
        self.0.take_from_worker(worker)
    }

    /// Read values produced by all worker threads during the last
    /// clock cycle.
    ///
    /// This is equivalent to calling
    /// [`take_from_worker`](`Self::take_from_worker`) for each
    /// worker thread in order and storing all none-`None`
    /// results in a vector.
    pub fn take_from_all(&self) -> Vec<T> {
        let num_workers = self.0.mailbox.len();
        let mut res = Vec::with_capacity(num_workers);

        for worker in 0..num_workers {
            if let Some(v) = self.take_from_worker(worker) {
                res.push(v);
            }
        }
        res
    }
}

impl<T> OutputHandle<T>
where
    T: Batch<Time = ()>,
    T::InnerBatch: Send,
{
    /// Read batches produced by all worker threads during the last
    /// clock cycle and consolidate them into a single batch.
    ///
    /// This method is used in the common case when the `OutputHandle` is
    /// attached to a stream that carries [`Batch`](`crate::trace::Batch`)es
    /// of updates to relational data.  Semantically, each `Batch` consists
    /// of `(key, value, weight)` tuples.  Depending on the structure of the
    /// circuit, the same `key` or `(key, value)` pair can occur in batches
    /// produced by multiple workers.  This method retrieves batches
    /// produced by all workers and consolidates them into a single batch
    /// where each `(key, value)` pair occurs exactly once.
    ///
    /// Internally, `consolidate` calls `take_from_worker` to retrieve batches
    /// from individual worker threads.  See
    /// [`take_from_worker`](`Self::take_from_worker`) documentation for the
    /// exact semantics of this method.  In particular, note that repeated calls
    /// to `take_from_worker` return `None`. `consolidate` skips `None` results
    /// when computing the consolidated batch.
    pub fn consolidate(&self) -> T {
        let factories = BatchReaderFactories::new::<T::Key, T::Val, T::R>();
        let handle: &OutputHandle<T::Inner> = unsafe { transmute(self) };
        T::from_inner(handle.dyn_consolidate(&factories))
    }
}

/// Sink operator that stores the contents of its input stream in
/// an `OutputHandle`.
struct Output<T> {
    mailbox: Mailbox<Option<T>>,
}

impl<T> Output<T>
where
    T: Clone + Send + 'static,
{
    fn new() -> (Self, OutputHandle<T>) {
        let handle = OutputHandle::new();
        let mailbox = handle.mailbox(Runtime::worker_index()).clone();

        let output = Self { mailbox };

        (output, handle)
    }
}

impl<T> Operator for Output<T>
where
    T: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Output")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T> SinkOperator<T> for Output<T>
where
    T: Debug + Clone + 'static,
{
    fn eval(&mut self, val: &T) {
        self.mailbox.set(Some(val.clone()));
    }

    fn eval_owned(&mut self, val: T) {
        self.mailbox.set(Some(val));
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }
}

struct OutputGuarded<T> {
    mailbox: Mailbox<Option<T>>,
}

impl<T> OutputGuarded<T>
where
    T: Clone + Send + 'static,
{
    fn new() -> (Self, OutputHandle<T>) {
        let handle = OutputHandle::new();
        let mailbox = handle.mailbox(Runtime::worker_index()).clone();

        let output = Self { mailbox };

        (output, handle)
    }
}

impl<T> Operator for OutputGuarded<T>
where
    T: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("OutputGuarded")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T> BinarySinkOperator<T, bool> for OutputGuarded<T>
where
    T: Clone + 'static,
{
    fn eval<'a>(&mut self, val: Cow<'a, T>, guard: Cow<'a, bool>) {
        if *guard {
            self.mailbox.set(Some(val.into_owned()));
        }
    }

    fn input_preference(&self) -> (OwnershipPreference, OwnershipPreference) {
        (
            OwnershipPreference::PREFER_OWNED,
            OwnershipPreference::INDIFFERENT,
        )
    }
}

#[cfg(test)]
mod test {
    use crate::{typed_batch::OrdZSet, utils::Tup2, Runtime};

    #[test]
    fn test_output_handle() {
        let (mut dbsp, (input, output)) = Runtime::init_circuit(4, |circuit| {
            let (zset, zset_handle) = circuit.add_input_zset::<u64>();
            let zset_output = zset.output();

            Ok((zset_handle, zset_output))
        })
        .unwrap();

        let inputs = vec![
            vec![Tup2(1, 1), Tup2(2, 1), Tup2(3, 1), Tup2(4, 1), Tup2(5, 1)],
            vec![
                Tup2(1, -1),
                Tup2(2, -1),
                Tup2(3, -1),
                Tup2(4, -1),
                Tup2(5, -1),
            ],
        ];

        for mut input_vec in inputs {
            let input_tuples = input_vec
                .iter()
                .map(|Tup2(k, w)| Tup2(Tup2(*k, ()), *w))
                .collect::<Vec<_>>();

            let expected_output = OrdZSet::from_tuples((), input_tuples);

            input.append(&mut input_vec);
            dbsp.step().unwrap();
            let output = output.consolidate();
            assert_eq!(output, expected_output);
        }

        dbsp.kill().unwrap();
    }

    #[test]
    fn test_guarded_output_handle() {
        let (mut dbsp, (input, guard, output)) = Runtime::init_circuit(4, |circuit| {
            let (zset, zset_handle) = circuit.add_input_zset::<u64>();
            let (guard, guard_handle) = circuit.add_input_stream::<bool>();
            let zset_output = zset.output_guarded(&guard);

            Ok((zset_handle, guard_handle, zset_output))
        })
        .unwrap();

        let inputs = vec![
            vec![Tup2(1, 1), Tup2(2, 1), Tup2(3, 1), Tup2(4, 1), Tup2(5, 1)],
            vec![
                Tup2(1, -1),
                Tup2(2, -1),
                Tup2(3, -1),
                Tup2(4, -1),
                Tup2(5, -1),
            ],
        ];

        for mut input_vec in inputs {
            let input_tuples = input_vec
                .iter()
                .map(|Tup2(k, w)| Tup2(Tup2(*k, ()), *w))
                .collect::<Vec<_>>();

            let expected_output = OrdZSet::from_tuples((), input_tuples);

            input.append(&mut input_vec.clone());
            guard.set_for_all(false);
            dbsp.step().unwrap();
            let output1 = output.consolidate();
            assert_eq!(output1, OrdZSet::empty(()));

            input.append(&mut input_vec);
            guard.set_for_all(true);
            dbsp.step().unwrap();
            let output2 = output.consolidate();

            assert_eq!(output2, expected_output);
        }

        dbsp.kill().unwrap();
    }
}
