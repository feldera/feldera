//! z^-1 operator delays its input by one timestamp.

use crate::Runtime;
use crate::circuit::circuit_builder::StreamId;
use crate::circuit::metadata::{MEMORY_ALLOCATIONS_COUNT, STATE_RECORDS_COUNT};
use crate::{
    Error, NumEntries,
    algebra::HasZero,
    circuit::checkpointer::Checkpoint,
    circuit::{
        Circuit, ExportId, ExportStream, FeedbackConnector, GlobalNodeId, OwnershipPreference,
        Scope, Stream,
        metadata::{
            ALLOCATED_MEMORY_BYTES, MetaItem, OperatorMeta, SHARED_MEMORY_BYTES, USED_MEMORY_BYTES,
        },
        operator_traits::{Operator, StrictOperator, StrictUnaryOperator, UnaryOperator},
    },
    circuit_cache_key,
    storage::file::to_bytes,
};
use feldera_storage::{FileCommitter, StoragePath};
use size_of::SizeOf;
use std::sync::Arc;
use std::{borrow::Cow, mem::replace};

use super::require_persistent_id;

circuit_cache_key!(DelayedId<C, D>(StreamId => Stream<C, D>));

/// Like [`FeedbackConnector`] but specialized for [`Z1`] feedback operator.
///
/// Use this API instead of the low-level [`Circuit::add_feedback`] API to
/// create feedback loops with `Z1` operator.  In addition to being more
/// concise, this API takes advantage of [caching](`crate::circuit::cache`).
pub struct DelayedFeedback<C, D>
where
    C: Circuit,
{
    feedback: FeedbackConnector<C, D, D, Z1<D>>,
    output: Stream<C, D>,
    export: Stream<C::Parent, D>,
}

impl<C, D> DelayedFeedback<C, D>
where
    C: Circuit,
    D: Checkpoint + Eq + SizeOf + NumEntries + Clone + HasZero + 'static,
{
    /// Create a feedback loop with `Z1` operator.  Use [`Self::connect`] to
    /// close the loop.
    pub fn new(circuit: &C) -> Self {
        let (ExportStream { local, export }, feedback) =
            circuit.add_feedback_with_export(Z1::new(D::zero()));

        Self {
            feedback,
            output: local,
            export,
        }
    }
}

impl<C, D> DelayedFeedback<C, D>
where
    C: Circuit,
    D: Checkpoint + Eq + SizeOf + NumEntries + Clone + 'static,
{
    /// Create a feedback loop with `Z1` operator.  Use [`Self::connect`] to
    /// close the loop.
    pub fn with_default(circuit: &C, default: D) -> Self {
        let (ExportStream { local, export }, feedback) =
            circuit.add_feedback_with_export(Z1::new(default));

        Self {
            feedback,
            output: local,
            export,
        }
    }

    /// Output stream of the `Z1` operator.
    pub fn stream(&self) -> &Stream<C, D> {
        &self.output
    }

    /// Connect `input` stream to the input of the `Z1` operator.
    pub fn connect(self, input: &Stream<C, D>) {
        let Self {
            feedback,
            output,
            export,
        } = self;
        let circuit = output.circuit().clone();

        feedback.connect_with_preference(input, OwnershipPreference::STRONGLY_PREFER_OWNED);
        circuit.cache_insert(DelayedId::new(input.stream_id()), output);
        circuit.cache_insert(ExportId::new(input.stream_id()), export);
    }
}

impl<C, D> Stream<C, D>
where
    C: Circuit,
{
    /// Applies [`Z1`] operator to `self`.
    #[track_caller]
    pub fn delay(&self) -> Stream<C, D>
    where
        D: Checkpoint + Eq + SizeOf + NumEntries + Clone + HasZero + 'static,
    {
        self.circuit()
            .cache_get_or_insert_with(DelayedId::new(self.stream_id()), || {
                let delay_pid = self.get_persistent_id().map(|pid| format!("{pid}.delay"));

                self.circuit()
                    .add_unary_operator(Z1::new(D::zero()), self)
                    .set_persistent_id(delay_pid.as_deref())
            })
            .clone()
    }

    #[track_caller]
    pub fn delay_with_initial_value(&self, initial: D) -> Stream<C, D>
    where
        D: Checkpoint + Eq + SizeOf + NumEntries + Clone + 'static,
    {
        self.circuit()
            .cache_get_or_insert_with(DelayedId::new(self.stream_id()), move || {
                let delay_pid = self.get_persistent_id().map(|pid| format!("{pid}.delay"));

                self.circuit()
                    .add_unary_operator(Z1::new(initial.clone()), self)
                    .set_persistent_id(delay_pid.as_deref())
            })
            .clone()
    }
}

/// z^-1 operator delays its input by one timestamp.
///
/// The operator outputs a user-defined "zero" value in the first timestamp
/// after [clock_start](`Z1::clock_start`).  For all subsequent timestamps, it
/// outputs the value received as input at the previous timestamp.  The zero
/// value is typically the neutral element of a monoid (e.g., 0 for addition
/// or 1 for multiplication).
///
/// It is a [strict
/// operator](`crate::circuit::operator_traits::StrictOperator`).
///
/// # Examples
///
/// ```text
/// time | input | output
/// ---------------------
///   0  |   5   |   0
///   1  |   6   |   5
///   2  |   7   |   6
///   3  |   8   |   7
///         ...
/// ```
pub struct Z1<T> {
    zero: T,
    // For error reporting,
    global_id: GlobalNodeId,
    empty_output: bool,
    values: T,
}

#[derive(rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub struct CommittedZ1 {
    values: Vec<u8>,
}

impl<T> TryFrom<&Z1<T>> for CommittedZ1
where
    T: Checkpoint + Clone,
{
    type Error = Error;

    fn try_from(z1: &Z1<T>) -> Result<CommittedZ1, Error> {
        Ok(CommittedZ1 {
            values: z1.values.checkpoint()?,
        })
    }
}

impl<T> Z1<T>
where
    T: Checkpoint + Clone,
{
    pub fn new(zero: T) -> Self {
        Self {
            empty_output: false,
            global_id: GlobalNodeId::root(),
            zero: zero.clone(),
            values: zero,
        }
    }

    /// Return the absolute path of the file for this operator.
    ///
    /// # Arguments
    /// - `cid`: The checkpoint id.
    /// - `persistent_id`: The persistent id that identifies the spine within
    ///   the circuit for a given checkpoint.
    fn checkpoint_file<P: AsRef<str>>(base: &StoragePath, persistent_id: P) -> StoragePath {
        base.child(format!("z1-{}.dat", persistent_id.as_ref()))
    }
}

impl<T> Operator for Z1<T>
where
    T: Checkpoint + SizeOf + NumEntries + Clone + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Z^-1")
    }

    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {
        self.empty_output = false;
        self.values = self.zero.clone();
    }

    fn init(&mut self, global_id: &GlobalNodeId) {
        self.global_id = global_id.clone();
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        let bytes = self.values.size_of();
        meta.extend(metadata! {
            STATE_RECORDS_COUNT => MetaItem::Count(self.values.num_entries_deep()),
            ALLOCATED_MEMORY_BYTES => MetaItem::bytes(bytes.total_bytes()),
            USED_MEMORY_BYTES => MetaItem::bytes(bytes.used_bytes()),
            MEMORY_ALLOCATIONS_COUNT => MetaItem::Count(bytes.distinct_allocations()),
            SHARED_MEMORY_BYTES => MetaItem::bytes(bytes.shared_bytes()),
        });
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        if scope == 0 {
            self.values.num_entries_shallow() == 0 && self.empty_output
        } else {
            true
        }
    }

    fn checkpoint(
        &mut self,
        base: &StoragePath,
        persistent_id: Option<&str>,
        files: &mut Vec<Arc<dyn FileCommitter>>,
    ) -> Result<(), Error> {
        let persistent_id = require_persistent_id(persistent_id, &self.global_id)?;

        let committed: CommittedZ1 = (self as &Self).try_into()?;
        let as_bytes = to_bytes(&committed).expect("Serializing CommittedZ1 should work.");
        files.push(
            Runtime::storage_backend()
                .unwrap()
                .write(&Self::checkpoint_file(base, persistent_id), as_bytes)?,
        );
        Ok(())
    }

    fn restore(&mut self, base: &StoragePath, persistent_id: Option<&str>) -> Result<(), Error> {
        let persistent_id = require_persistent_id(persistent_id, &self.global_id)?;

        let z1_path = Self::checkpoint_file(base, persistent_id);
        let content = Runtime::storage_backend().unwrap().read(&z1_path)?;
        let committed = unsafe { rkyv::archived_root::<CommittedZ1>(&content) };

        let mut values = self.zero.clone();
        values.restore(committed.values.as_slice())?;
        self.empty_output = false;
        self.values = values;
        Ok(())
    }

    fn clear_state(&mut self) -> Result<(), Error> {
        self.empty_output = false;
        self.values = self.zero.clone();
        Ok(())
    }
}

impl<T> UnaryOperator<T, T> for Z1<T>
where
    T: Checkpoint + SizeOf + NumEntries + Clone + 'static,
{
    async fn eval(&mut self, i: &T) -> T {
        replace(&mut self.values, i.clone())
    }

    async fn eval_owned(&mut self, i: T) -> T {
        replace(&mut self.values, i)
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }
}

impl<T> StrictOperator<T> for Z1<T>
where
    T: Checkpoint + SizeOf + NumEntries + Clone + 'static,
{
    fn get_output(&mut self) -> T {
        self.empty_output = self.values.num_entries_shallow() == 0;
        replace(&mut self.values, self.zero.clone())
    }

    fn get_final_output(&mut self) -> T {
        self.get_output()
    }
}

impl<T> StrictUnaryOperator<T, T> for Z1<T>
where
    T: Checkpoint + SizeOf + NumEntries + Clone + 'static,
{
    async fn eval_strict(&mut self, i: &T) {
        self.values = i.clone();
    }

    async fn eval_strict_owned(&mut self, i: T) {
        self.values = i;
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }
}

#[cfg(test)]
mod test {
    use crate::{
        circuit::operator_traits::{Operator, StrictOperator, StrictUnaryOperator, UnaryOperator},
        operator::Z1,
    };

    #[tokio::test]
    async fn z1_test() {
        let mut z1 = Z1::new(0);

        let expected_result = vec![0, 1, 2, 0, 4, 5];

        // Test `UnaryOperator` API.
        let mut res = Vec::new();
        z1.clock_start(0);
        res.push(z1.eval(&1).await);
        res.push(z1.eval(&2).await);
        res.push(z1.eval(&3).await);
        z1.clock_end(0);

        z1.clock_start(0);
        res.push(z1.eval_owned(4).await);
        res.push(z1.eval_owned(5).await);
        res.push(z1.eval_owned(6).await);
        z1.clock_end(0);

        assert_eq!(res, expected_result);

        // Test `StrictUnaryOperator` API.
        let mut res = Vec::new();
        z1.clock_start(0);
        res.push(z1.get_output());
        z1.eval_strict(&1).await;
        res.push(z1.get_output());
        z1.eval_strict(&2).await;
        res.push(z1.get_output());
        z1.eval_strict(&3).await;
        z1.clock_end(0);

        z1.clock_start(0);
        res.push(z1.get_output());
        z1.eval_strict_owned(4).await;
        res.push(z1.get_output());
        z1.eval_strict_owned(5).await;
        res.push(z1.get_output());
        z1.eval_strict_owned(6).await;
        z1.clock_end(0);

        assert_eq!(res, expected_result);
    }
}
