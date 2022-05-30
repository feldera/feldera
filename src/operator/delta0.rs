//! `delta_0` operator.

use crate::{
    algebra::HasZero,
    circuit::{
        operator_traits::{Data, ImportOperator, Operator},
        Circuit, OwnershipPreference, Scope, Stream,
    },
};
use std::borrow::Cow;

impl<P, D> Stream<Circuit<P>, D>
where
    D: HasZero + Clone + 'static,
    P: Clone + 'static,
{
    /// Import `self` from the parent circuit to `subcircuit` via the `Delta0`
    /// operator.
    ///
    /// See [`Delta0`] operator documentation.
    pub fn delta0(&self, subcircuit: &Circuit<Circuit<P>>) -> Stream<Circuit<Circuit<P>>, D> {
        subcircuit.import_stream(Delta0::new(), self)
    }

    /// Like [`Self::delta0`], but overrides the ownership
    /// preference on the input stream with `input_preference`.
    pub fn delta0_with_preference(
        &self,
        circuit: &Circuit<Circuit<P>>,
        input_preference: OwnershipPreference,
    ) -> Stream<Circuit<Circuit<P>>, D> {
        circuit.import_stream_with_preference(Delta0::new(), self, input_preference)
    }
}

/// `delta_0` DBSP operator.
///
/// `delta_0` is an
/// [import operator](`crate::circuit::operator_traits::ImportOperator`), i.e.,
/// an operator that makes the contents of a parent stream available
/// inside the child circuit.  At the first nested clock cycle, it reads and
/// outputs a value from the parent stream.  During subsequent nested clock
/// cycles, it outputs zero.
///
/// # Examples
///
/// Given a parent stream
///
/// ```text
/// [1, 2, 3, 4, ...]
/// ```
///
/// the `delta_0` operator produces the following nested stream (each row in
/// the matrix represents a nested clock epoch):
///
/// ```text
/// ┌           ┐
/// │1 0 0 0 ...│
/// │2 0 0 0 ...│
/// │3 0 0 0 ...|
/// |4 0 0 0 ...|
/// └           ┘
/// ```
pub struct Delta0<D> {
    val: Option<D>,
    fixedpoint: bool,
}

impl<D> Delta0<D> {
    pub fn new() -> Self {
        Self {
            val: None,
            fixedpoint: false,
        }
    }
}

impl<D> Default for Delta0<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D> Operator for Delta0<D>
where
    D: Data,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("delta0")
    }
    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}
    fn fixedpoint(&self) -> bool {
        self.fixedpoint
    }
}

impl<D> ImportOperator<D, D> for Delta0<D>
where
    D: HasZero + Clone + 'static,
{
    fn import(&mut self, val: &D) {
        self.val = Some(val.clone());
        self.fixedpoint = false;
    }
    fn import_owned(&mut self, val: D) {
        self.val = Some(val);
        self.fixedpoint = false;
    }

    fn eval(&mut self) -> D {
        if self.val.is_none() {
            self.fixedpoint = true;
        }
        self.val.take().unwrap_or_else(D::zero)
    }

    /// Ownership preference on the operator's input stream
    /// (see [`OwnershipPreference`]).
    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }
}
