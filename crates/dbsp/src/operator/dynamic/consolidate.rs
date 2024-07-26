//! Operator that consolidates a trace into a single batch.

use minitrace::trace;
use std::{borrow::Cow, marker::PhantomData};

use crate::{
    circuit::{
        operator_traits::{Operator, UnaryOperator},
        Circuit, GlobalNodeId, OwnershipPreference, Scope, Stream,
    },
    circuit_cache_key,
    trace::{Batch, BatchReader, Trace},
};

circuit_cache_key!(ConsolidateId<C, D>(GlobalNodeId => Stream<C, D>));

impl<C, T> Stream<C, T>
where
    C: Circuit,
    T: Trace<Time = ()> + Clone,
{
    /// See [`Stream::consolidate`].
    pub fn dyn_consolidate(
        &self,
        factories: &<T::Batch as BatchReader>::Factories,
    ) -> Stream<C, T::Batch> {
        self.circuit()
            .cache_get_or_insert_with(ConsolidateId::new(self.origin_node_id().clone()), || {
                let consolidated = self.circuit().add_unary_operator_with_preference(
                    Consolidate::new(factories),
                    &self.try_sharded_version(),
                    OwnershipPreference::STRONGLY_PREFER_OWNED,
                );
                consolidated.mark_sharded_if(self);

                consolidated
            })
            .clone()
    }
}

pub struct Consolidate<T: Trace> {
    factories: <T::Batch as BatchReader>::Factories,
    _type: PhantomData<T>,
}

impl<T: Trace> Consolidate<T> {
    pub fn new(factories: &<T::Batch as BatchReader>::Factories) -> Self {
        Self {
            factories: factories.clone(),
            _type: PhantomData,
        }
    }
}

impl<T> Operator for Consolidate<T>
where
    T: Trace + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("Consolidate")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T> UnaryOperator<T, T::Batch> for Consolidate<T>
where
    T: Trace<Time = ()>,
{
    #[trace]
    fn eval(&mut self, _i: &T) -> T::Batch {
        unimplemented!()
    }

    #[trace]
    fn eval_owned(&mut self, i: T) -> T::Batch {
        i.consolidate()
            .unwrap_or_else(|| T::Batch::dyn_empty(&self.factories))
    }
}
