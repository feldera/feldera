//! The feedback operator of a [`RecursiveVar`](super::RecursiveVar).

use std::{borrow::Cow, mem::replace};

use size_of::SizeOf;

use crate::{
    Error, NumEntries, Scope,
    circuit::{
        GlobalNodeId, OwnershipPreference,
        metadata::{
            ALLOCATED_MEMORY_BYTES, MEMORY_ALLOCATIONS_COUNT, MetaItem, OperatorMeta,
            SHARED_MEMORY_BYTES, STATE_RECORDS_COUNT, USED_MEMORY_BYTES,
        },
        operator_traits::{Operator, OperatorName, StrictOperator, StrictUnaryOperator},
    },
    trace::{Batch, BatchReader, Spine, Trace, TraceRole},
};

/// A strict operator that accumulates input batches in a spine and outputs them
/// at the start of the next transaction.
///
/// Used to delay outputs produced by the current iteration of a recursive scope
/// until the next iteration.
///
/// Each iteration of a recursive scope is one transaction of its circuit. The
/// operator collects the batches it receives during an iteration into a spine
/// and outputs that spine, whole, at the first step of the next iteration;
/// later steps output `None`.
pub(super) struct IterationDelay<B>
where
    B: Batch,
{
    factories: B::Factories,
    name: OperatorName,

    /// Batches received during the current iteration, for the next one.
    pending: Spine<B>,

    /// Batches received during the previous iteration, until the first step
    /// of the current iteration outputs them.
    ready: Option<Spine<B>>,

    /// Whether the spine output by the current iteration was empty.
    empty_output: bool,
}

impl<B> IterationDelay<B>
where
    B: Batch,
{
    /// Creates an iteration delay.
    ///
    /// # Arguments
    ///
    /// * `factories` - factories of the batches the operator receives.
    ///
    /// # Returns
    ///
    /// An operator whose output in the first iteration is an empty spine.
    pub(super) fn new(factories: &B::Factories) -> Self {
        let name = OperatorName::new("IterationDelay");
        Self {
            factories: factories.clone(),
            pending: Spine::new(factories, name.get(), TraceRole::Accumulator),
            name,
            ready: None,
            empty_output: false,
        }
    }

    /// Creates an empty spine to collect an iteration's input into.
    ///
    /// # Returns
    ///
    /// An empty spine named after this operator.
    fn new_spine(&self) -> Spine<B> {
        Spine::new(&self.factories, self.name.get(), TraceRole::Accumulator)
    }

    /// Drops the batches the operator holds.
    fn clear(&mut self) {
        if !self.pending.is_empty() {
            self.pending = self.new_spine();
        }
        self.ready = None;
        self.empty_output = false;
    }
}

impl<B> Operator for IterationDelay<B>
where
    B: Batch,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("IterationDelay")
    }

    fn init(&mut self, global_id: &GlobalNodeId) {
        self.name.init(global_id);
        self.pending.set_name(self.name.get());
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        let bytes = self.pending.size_of();
        meta.extend(metadata! {
            STATE_RECORDS_COUNT => MetaItem::Count(self.pending.num_entries_deep()),
            ALLOCATED_MEMORY_BYTES => MetaItem::bytes(bytes.total_bytes()),
            USED_MEMORY_BYTES => MetaItem::bytes(bytes.used_bytes()),
            MEMORY_ALLOCATIONS_COUNT => MetaItem::Count(bytes.distinct_allocations()),
            SHARED_MEMORY_BYTES => MetaItem::bytes(bytes.shared_bytes()),
        });
    }

    fn clock_start(&mut self, _scope: Scope) {
        debug_assert!(self.pending.is_empty());
        debug_assert!(self.ready.is_none());
    }

    fn clock_end(&mut self, _scope: Scope) {
        self.clear();
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        if scope == 0 {
            // Like `Z1`: the next iteration's input is empty, and so was the
            // current one's.
            self.pending.is_empty() && self.empty_output
        } else {
            true
        }
    }

    fn clear_state(&mut self) -> Result<(), Error> {
        self.clear();
        Ok(())
    }

    fn start_transaction(&mut self) {
        // Both halves of the feedback node forward this call, and the first
        // one already moved the batches: moving them again would replace
        // them with an empty spine.
        if self.ready.is_none() {
            let pending = self.new_spine();
            self.ready = Some(replace(&mut self.pending, pending));
        }
    }
}

impl<B> StrictOperator<Option<Spine<B>>> for IterationDelay<B>
where
    B: Batch,
{
    fn get_output(&mut self) -> Option<Spine<B>> {
        let output = self.ready.take();
        if let Some(spine) = &output {
            self.empty_output = spine.is_empty();
        }
        output
    }

    fn get_final_output(&mut self) -> Option<Spine<B>> {
        self.get_output()
    }
}

impl<B> StrictUnaryOperator<B, Option<Spine<B>>> for IterationDelay<B>
where
    B: Batch,
{
    async fn eval_strict(&mut self, batch: &B) {
        self.eval_strict_owned(batch.clone()).await
    }

    async fn eval_strict_owned(&mut self, batch: B) {
        if !batch.is_empty() {
            self.pending.insert(batch).await;
        }
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }

    fn flush_input(&mut self) {}

    fn is_flush_input_complete(&self) -> bool {
        true
    }
}
