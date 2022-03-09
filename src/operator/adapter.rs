//! Adapter operators wrap around regular operators and decapsulate
//! inputs wrapped in shared references.
//!
//! Adapters in this module allow existing operators such as `Plus<T>`
//! to be applied to streams that carry data of type `Ref<T>`, where
//! `Ref` is any type that implements trait [`SharedRef`](`crate::SharedRef`).
//! The adapter tries to extract an owned value from `Ref` if possible
//! and borrows the value otherwise before invoking the inner operator.
//! We provide these generic adapters so we do not need to generalize the
//! implementation of individual operators to handle shared references.
//!
//! For good measure, the adapter also optionally wraps the output of
//! the inner operator in `Ref` or converts it to any other type that
//! implements `From<T>`.  This isn't strictly necessary, as the same
//! can be achieved by chaining the operator with a simple transformer
//! operator.

use std::{borrow::Cow, marker::PhantomData};

use crate::{
    circuit::{
        operator_traits::{BinaryOperator, Operator, UnaryOperator},
        OwnershipPreference, Scope,
    },
    SharedRef,
};

/// Unary operator adapter unwraps input values wrapped in a shared reference.
/// See [module-level documentation](`crate::operator::adapter`) for details.
pub struct UnaryOperatorAdapter<O, Op> {
    op: Op,
    _types: PhantomData<O>,
}

impl<O, Op> UnaryOperatorAdapter<O, Op> {
    pub fn new(op: Op) -> Self {
        Self {
            op,
            _types: PhantomData,
        }
    }
}

impl<O, Op> Operator for UnaryOperatorAdapter<O, Op>
where
    Op: Operator,
    O: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        self.op.name()
    }
    fn clock_start(&mut self, scope: Scope) {
        self.op.clock_start(scope);
    }
    fn clock_end(&mut self, scope: Scope) {
        self.op.clock_end(scope);
    }
    fn is_async(&self) -> bool {
        self.op.is_async()
    }
    fn ready(&self) -> bool {
        self.op.ready()
    }
    fn register_ready_callback<F>(&mut self, cb: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.op.register_ready_callback(cb);
    }
}

impl<RI, RO, O, Op> UnaryOperator<RI, RO> for UnaryOperatorAdapter<O, Op>
where
    RI: SharedRef,
    Op: UnaryOperator<<RI as SharedRef>::Target, O>,
    RO: From<O>,
    O: 'static,
{
    fn eval(&mut self, input: &RI) -> RO {
        self.op.eval(input.borrow()).into()
    }

    fn eval_owned(&mut self, input: RI) -> RO {
        match input.try_into_owned() {
            Ok(v) => self.op.eval_owned(v),
            Err(v) => self.op.eval(v.borrow()),
        }
        .into()
    }

    fn input_preference(&self) -> OwnershipPreference {
        self.op.input_preference()
    }
}

/// Binary operator adapter unwraps input values of types
/// `I1` and `I2` wrapped in shared references.  See
/// [module-level documentation](`crate::operator::adapter`) for details.
pub struct BinaryOperatorAdapter<O, Op> {
    op: Op,
    _types: PhantomData<O>,
}

impl<O, Op> BinaryOperatorAdapter<O, Op> {
    pub fn new(op: Op) -> Self {
        Self {
            op,
            _types: PhantomData,
        }
    }
}

impl<O, Op> Operator for BinaryOperatorAdapter<O, Op>
where
    Op: Operator,
    O: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        self.op.name()
    }
    fn clock_start(&mut self, scope: Scope) {
        self.op.clock_start(scope);
    }
    fn clock_end(&mut self, scope: Scope) {
        self.op.clock_end(scope);
    }
    fn is_async(&self) -> bool {
        self.op.is_async()
    }
    fn ready(&self) -> bool {
        self.op.ready()
    }
    fn register_ready_callback<F>(&mut self, cb: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.op.register_ready_callback(cb);
    }
}

impl<RI1, RI2, RO, O, Op> BinaryOperator<RI1, RI2, RO> for BinaryOperatorAdapter<O, Op>
where
    RI1: SharedRef,
    RI2: SharedRef,
    Op: BinaryOperator<<RI1 as SharedRef>::Target, <RI2 as SharedRef>::Target, O>,
    RO: From<O>,
    O: 'static,
{
    fn eval(&mut self, left: &RI1, right: &RI2) -> RO {
        self.op.eval(left.borrow(), right.borrow()).into()
    }

    fn eval_owned(&mut self, left: RI1, right: RI2) -> RO {
        match (left.try_into_owned(), right.try_into_owned()) {
            (Ok(v1), Ok(v2)) => self.op.eval_owned(v1, v2),
            (Ok(v1), Err(v2)) => self.op.eval_owned_and_ref(v1, v2.borrow()),
            (Err(v1), Ok(v2)) => self.op.eval_ref_and_owned(v1.borrow(), v2),
            (Err(v1), Err(v2)) => self.op.eval(v1.borrow(), v2.borrow()),
        }
        .into()
    }

    fn eval_owned_and_ref(&mut self, left: RI1, right: &RI2) -> RO {
        match left.try_into_owned() {
            Ok(v) => self.op.eval_owned_and_ref(v, right.borrow()),
            Err(v) => self.op.eval(v.borrow(), right.borrow()),
        }
        .into()
    }

    fn eval_ref_and_owned(&mut self, left: &RI1, right: RI2) -> RO {
        match right.try_into_owned() {
            Ok(v) => self.op.eval_ref_and_owned(left.borrow(), v),
            Err(v) => self.op.eval(left.borrow(), v.borrow()),
        }
        .into()
    }

    fn input_preference(&self) -> (OwnershipPreference, OwnershipPreference) {
        self.op.input_preference()
    }
}
