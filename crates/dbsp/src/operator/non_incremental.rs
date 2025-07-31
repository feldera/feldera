use crate::{
    circuit::{
        circuit_builder::{CircuitBase, NonIterativeCircuit},
        operator_traits::{ImportOperator, Operator},
        runtime::Consensus,
        schedule::{DynamicScheduler, Executor, FlushProgress, Scheduler},
        OwnershipPreference,
    },
    operator::Generator,
    trace::{
        Batch as DynBatch, BatchReader as _, BatchReaderFactories, Spine as DynSpine, Trace as _,
    },
    typed_batch::{Spine, TypedBatch},
    Batch, ChildCircuit, Circuit, SchedulerError, Scope, Stream, Timestamp,
};
use impl_trait_for_tuples::impl_for_tuples;
use std::{borrow::Cow, cell::RefCell, collections::BTreeSet, future::Future, pin::Pin, rc::Rc};

pub trait NonIncrementalInputStreams<C>
where
    C: Circuit,
{
    type Imported;

    fn import(&self, child_circuit: &mut NonIterativeCircuit<C>) -> Self::Imported;
}

impl<B, C> NonIncrementalInputStreams<C> for Stream<C, B>
where
    C: Circuit,
    B: Batch,
{
    type Imported = Stream<NonIterativeCircuit<C>, TypedBatch<B::Key, B::Val, B::R, B::Inner>>;

    fn import(&self, child_circuit: &mut NonIterativeCircuit<C>) -> Self::Imported {
        let accumulated = child_circuit
            .import_stream(
                ImportAccumulator::new(&BatchReaderFactories::new::<B::Key, B::Val, B::R>()),
                &self.try_sharded_version().inner(),
            )
            .typed()
            .apply_owned(|spine: Spine<B>| spine.consolidate());
        accumulated.mark_sharded_if(self);
        accumulated
    }
}

#[allow(clippy::unused_unit)]
#[impl_for_tuples(12)]
#[tuple_types_custom_trait_bound(NonIncrementalInputStreams<C>)]
impl<C> NonIncrementalInputStreams<C> for Tuple
where
    C: Circuit,
{
    for_tuples!( type Imported = ( #( Tuple::Imported ),* ); );

    fn import(&self, child_circuit: &mut NonIterativeCircuit<C>) -> Self::Imported {
        (for_tuples!( #( self.Tuple.import(child_circuit) ),* ))
    }
}

struct NonIterativeExecutor {
    scheduler: DynamicScheduler,
    flush: RefCell<bool>,
    flush_consensus: Consensus,
}

impl NonIterativeExecutor {
    pub fn new() -> Self {
        Self {
            scheduler: DynamicScheduler::new(),
            flush: RefCell::new(false),
            flush_consensus: Consensus::new(),
        }
    }
}

impl<C> Executor<C> for NonIterativeExecutor
where
    C: Circuit,
{
    fn prepare(
        &mut self,
        circuit: &C,
        nodes: Option<&BTreeSet<crate::circuit::NodeId>>,
    ) -> Result<(), SchedulerError> {
        self.scheduler.prepare(circuit, nodes)
    }

    fn start_step<'a>(
        &'a self,
        circuit: &'a C,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchedulerError>> + 'a>> {
        Box::pin(async { self.scheduler.start_step(circuit).await })
    }

    fn microstep<'a>(
        &'a self,
        _circuit: &'a C,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchedulerError>> + 'a>> {
        todo!()
    }

    fn step<'a>(
        &'a self,
        circuit: &'a C,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchedulerError>> + 'a>> {
        let circuit = circuit.clone();
        Box::pin(async move {
            let local = *self.flush.borrow();
            if self.flush_consensus.check(local).await? {
                *self.flush.borrow_mut() = false;
                self.scheduler.step(&circuit).await?;
            }
            Ok(())
        })
    }

    fn flush(&self) -> Result<(), SchedulerError> {
        *self.flush.borrow_mut() = true;
        Ok(())
    }

    fn is_flush_complete(&self) -> bool {
        !*self.flush.borrow()
    }

    fn flush_progress(&self) -> FlushProgress {
        FlushProgress::new()
    }
}

struct ImportAccumulator<B>
where
    B: DynBatch,
{
    factories: B::Factories,
    spine: DynSpine<B>,
}

impl<B> ImportAccumulator<B>
where
    B: DynBatch,
{
    pub fn new(factories: &B::Factories) -> Self {
        Self {
            factories: factories.clone(),
            spine: DynSpine::<B>::new(factories),
        }
    }
}

impl<B> Operator for ImportAccumulator<B>
where
    B: DynBatch,
{
    fn name(&self) -> std::borrow::Cow<'static, str> {
        Cow::Borrowed("ImportAccumulator")
    }

    fn fixedpoint(&self, _scope: crate::circuit::Scope) -> bool {
        self.spine.is_empty()
    }
}

impl<B> ImportOperator<B, DynSpine<B>> for ImportAccumulator<B>
where
    B: DynBatch,
{
    fn import(&mut self, val: &B) {
        self.spine.insert(val.clone())
    }

    fn import_owned(&mut self, val: B) {
        self.spine.insert(val)
    }

    async fn eval(&mut self) -> DynSpine<B> {
        let mut spine = DynSpine::<B>::new(&self.factories);
        std::mem::swap(&mut self.spine, &mut spine);
        spine
    }
}

impl<P, T> ChildCircuit<P, T>
where
    P: 'static,
    T: Timestamp,
    Self: Circuit,
{
    #[track_caller]
    pub fn non_incremental<F, I, O>(
        &self,
        input_streams: &I,
        f: F,
    ) -> Result<Stream<Self, O>, SchedulerError>
    where
        F: FnOnce(
            &NonIterativeCircuit<Self>,
            &I::Imported,
        ) -> Result<Stream<NonIterativeCircuit<Self>, O>, SchedulerError>,
        I: NonIncrementalInputStreams<Self>,
        O: Clone + Default + std::fmt::Debug + 'static,
    {
        let output_value: Rc<RefCell<O>> = Rc::new(RefCell::new(O::default()));
        let output_value_clone = output_value.clone();

        let subcircuit_node_id = self.non_iterative_subcircuit(move |circuit| {
            let accumulated = input_streams.import(circuit);

            let result = f(circuit, &accumulated)?;
            result.apply(move |batch| *output_value_clone.borrow_mut() = batch.clone());

            let mut executor = NonIterativeExecutor::new();

            // if Runtime::worker_index() == 0 {
            //     circuit.to_dot_file(
            //         |node| {
            //             Some(crate::utils::DotNodeAttributes::new().with_label(&format!(
            //                 "{}-{}-{}",
            //                 node.local_id(),
            //                 node.name(),
            //                 node.persistent_id().unwrap_or_default()
            //             )))
            //         },
            //         |edge| {
            //             let style = if edge.is_dependency() {
            //                 Some("dotted".to_string())
            //             } else {
            //                 None
            //             };
            //             let label = if let Some(stream) = &edge.stream {
            //                 Some(format!("consumers: {}", stream.num_consumers()))
            //             } else {
            //                 None
            //             };
            //             Some(
            //                 crate::utils::DotEdgeAttributes::new(edge.stream_id())
            //                     .with_style(style)
            //                     .with_label(label),
            //             )
            //         },
            //         "commit.dot",
            //     );
            //     println!("circuit written to commit.dot");
            // }

            executor.prepare(circuit, None)?;

            Ok((circuit.node_id(), executor))
        })?;

        let output = self.add_source(Generator::new(move || {
            std::mem::take(&mut *output_value.borrow_mut())
        }));

        self.add_dependency(subcircuit_node_id, output.local_node_id());

        Ok(output)
    }
}

impl<C, D> Stream<C, D>
where
    D: Clone + 'static,
    C: Circuit,
{
    /// Import `self` from the parent circuit to `subcircuit` via the `Delta0NonIterative`
    /// operator.
    #[track_caller]
    pub fn delta0_non_iterative<CC>(&self, subcircuit: &CC) -> Stream<CC, D>
    where
        CC: Circuit<Parent = C>,
    {
        let delta =
            subcircuit.import_stream(Delta0NonIterative::new(), &self.try_sharded_version());
        delta.mark_sharded_if(self);

        delta
    }
}

pub struct Delta0NonIterative<D> {
    val: Option<D>,
    fixedpoint: bool,
}

impl<D> Delta0NonIterative<D> {
    pub fn new() -> Self {
        Self {
            val: None,
            fixedpoint: false,
        }
    }
}

impl<D> Default for Delta0NonIterative<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D> Operator for Delta0NonIterative<D>
where
    D: Clone + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("delta0")
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        if scope == 0 {
            // Output becomes stable (all zeros) after the first clock cycle.
            self.fixedpoint
        } else {
            // Delta0 does not maintain any state across epochs.
            true
        }
    }
}

impl<D> ImportOperator<D, D> for Delta0NonIterative<D>
where
    D: Clone + 'static,
{
    fn import(&mut self, val: &D) {
        self.val = Some(val.clone());
        self.fixedpoint = false;
    }

    fn import_owned(&mut self, val: D) {
        self.val = Some(val);
        self.fixedpoint = false;
    }

    async fn eval(&mut self) -> D {
        if self.val.is_none() {
            self.fixedpoint = true;
        }
        self.val.take().unwrap()
    }

    /// Ownership preference on the operator's input stream
    /// (see [`OwnershipPreference`]).
    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }
}

#[cfg(test)]
mod test {
    use crate::{typed_batch::SpineSnapshot, OrdZSet, Runtime};

    #[test]
    fn test_non_incremental() {
        let (mut dbsp, (input_handle, output_handle)) = Runtime::init_circuit(4, |circuit| {
            let (input_stream, input_handle) = circuit.add_input_zset::<i64>();

            let differentiated_input = circuit
                .non_incremental(&input_stream, |_child_circuit, input_stream| {
                    Ok(input_stream.differentiate())
                })
                .unwrap();

            let output_handle = differentiated_input.accumulate_output();

            Ok((input_handle, output_handle))
        })
        .unwrap();

        dbsp.start_transaction().unwrap();

        input_handle.push(5, 1);
        dbsp.step().unwrap();

        input_handle.push(5, 1);
        dbsp.step().unwrap();

        dbsp.commit_transaction().unwrap();
        let output = SpineSnapshot::<OrdZSet<i64>>::concat(&output_handle.take_from_all())
            .iter()
            .collect::<Vec<_>>();

        debug_assert_eq!(output, vec![(5, (), 2)]);

        dbsp.start_transaction().unwrap();

        input_handle.push(5, 1);
        dbsp.step().unwrap();

        input_handle.push(2, 1);
        dbsp.step().unwrap();

        dbsp.commit_transaction().unwrap();
        let output = SpineSnapshot::<OrdZSet<i64>>::concat(&output_handle.take_from_all())
            .iter()
            .collect::<Vec<_>>();
        debug_assert_eq!(output, vec![(2, (), 1), (5, (), -1)]);
    }
}
