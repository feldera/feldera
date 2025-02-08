//! `delta_0` operator.

use crate::{
    algebra::HasZero,
    circuit::{
        operator_traits::{Data, ImportOperator, Operator},
        Circuit, OwnershipPreference, Scope, Stream,
    },
};
use std::borrow::Cow;

impl<C, D> Stream<C, D>
where
    D: HasZero + Clone + 'static,
    C: Circuit,
{
    /// Import `self` from the parent circuit to `subcircuit` via the `Delta0`
    /// operator.
    ///
    /// See [`Delta0`] operator documentation.
    #[track_caller]
    pub fn delta0<CC>(&self, subcircuit: &CC) -> Stream<CC, D>
    where
        CC: Circuit<Parent = C>,
    {
        let delta = subcircuit.import_stream(Delta0::new(), &self.try_sharded_version());
        delta.mark_sharded_if(self);

        delta
    }

    /// Like [`Self::delta0`], but overrides the ownership
    /// preference on the input stream with `input_preference`.
    #[track_caller]
    pub fn delta0_with_preference<CC>(
        &self,
        subcircuit: &CC,
        input_preference: OwnershipPreference,
    ) -> Stream<CC, D>
    where
        CC: Circuit<Parent = C>,
    {
        let delta = subcircuit.import_stream_with_preference(
            Delta0::new(),
            &self.try_sharded_version(),
            input_preference,
        );
        delta.mark_sharded_if(self);

        delta
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

    async fn eval(&mut self) -> D {
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
