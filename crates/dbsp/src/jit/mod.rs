//! JIT operator for DBSP circuits.

use crate::{
    circuit::{
        metadata::OperatorLocation,
        operator_traits::{Operator, UnaryOperator},
        Circuit, Stream,
    },
    Scope,
};
use jit::{JitFunction, RawJitBatch};
use std::{borrow::Cow, panic::Location};

/// Operator that forwards a `RawJitBatch` into JIT compiled code.
pub struct JitInvokeOperator {
    func: JitFunction,
    name: Cow<'static, str>,
    location: &'static Location<'static>,
}

impl JitInvokeOperator {
    fn new(
        func: JitFunction,
        name: Cow<'static, str>,
        location: &'static Location<'static>,
    ) -> Self {
        Self {
            func,
            name,
            location,
        }
    }
}

impl Operator for JitInvokeOperator {
    fn name(&self) -> Cow<'static, str> {
        self.name.clone()
    }

    fn location(&self) -> OperatorLocation {
        Some(self.location)
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl UnaryOperator<RawJitBatch, RawJitBatch> for JitInvokeOperator {
    async fn eval(&mut self, input: &RawJitBatch) -> RawJitBatch {
        self.func.invoke(*input)
    }
}

impl<C> Stream<C, RawJitBatch>
where
    C: Circuit,
{
    /// Insert a JIT invocation operator that mutates the pointer batch in place.
    #[track_caller]
    pub fn invoke_jit<N>(&self, name: N, func: JitFunction) -> Stream<C, RawJitBatch>
    where
        N: Into<Cow<'static, str>>,
    {
        self.circuit().add_unary_operator(
            JitInvokeOperator::new(func, name.into(), Location::caller()),
            self,
        )
    }
}