use crate::{
    algebra::{HasZero, MulByRef, ZSet},
    circuit::GlobalNodeId,
    circuit::{
        operator_traits::{BinaryOperator, Data, Operator},
        Scope,
    },
    circuit_cache_key,
    trace::{Batch, BatchReader, Builder, Cursor},
    Circuit, Stream,
};
use std::{
    borrow::Cow,
    cmp::{min, Ordering},
    hash::Hash,
    marker::PhantomData,
};

circuit_cache_key!(SemijoinId<C, D>((GlobalNodeId, GlobalNodeId) => Stream<C, D>));

impl<S, Pairs> Stream<Circuit<S>, Pairs>
where
    S: Clone + 'static,
{
    /// Semijoin two streams of batches.
    ///
    /// The operator takes two streams of batches indexed with the same key type
    /// (`Pairs::Key = Keys::Key`) and outputs a stream obtained by joining each
    /// pair of inputs.
    ///
    /// Input streams will typically be produced by [`Stream::index()`] or
    /// [`Stream::index_with()`]
    ///
    /// #### Type arguments
    ///
    /// * `Pairs` - batch type in the first input stream.
    /// * `Keys` - batch type in the second input stream.
    /// * `Out` - output Z-set type.
    pub fn semijoin_stream<Keys, Out>(
        &self,
        keys: &Stream<Circuit<S>, Keys>,
    ) -> Stream<Circuit<S>, Out>
    where
        // TODO: Associated type bounds (rust/#52662) really simplify things
        // TODO: Allow non-unit timestamps
        Pairs: Batch<Time = ()> + Data + Send,
        Pairs::Key: Hash + Ord + Clone,
        Pairs::Val: Ord + Clone,
        Keys: Batch<Key = Pairs::Key, Val = (), Time = ()> + Data + Send,
        // TODO: Should this be `IndexedZSet<Key = Pairs::Key, Val = Pairs::Val>`?
        Out: ZSet<Key = (Pairs::Key, Pairs::Val)> + 'static,
        Out::R: HasZero,
        Pairs::R: MulByRef<Keys::R, Output = Out::R>,
    {
        self.circuit()
            .cache_get_or_insert_with(
                SemijoinId::new((self.origin_node_id().clone(), keys.origin_node_id().clone())),
                move || {
                    self.circuit().add_binary_operator(
                        SemiJoinStream::new(),
                        &self.shard(),
                        &keys.shard(),
                    )
                },
            )
            .clone()
    }
}

/// Semijoin two streams of batches, see [`Stream::semijoin_stream`]
pub struct SemiJoinStream<Pairs, Keys, Out> {
    _types: PhantomData<(Pairs, Keys, Out)>,
}

impl<Pairs, Keys, Out> SemiJoinStream<Pairs, Keys, Out> {
    pub const fn new() -> Self {
        Self {
            _types: PhantomData,
        }
    }
}

impl<Pairs, Keys, Out> Operator for SemiJoinStream<Pairs, Keys, Out>
where
    Pairs: 'static,
    Keys: 'static,
    Out: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("SemiJoinStream")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<Pairs, Keys, Out> BinaryOperator<Pairs, Keys, Out> for SemiJoinStream<Pairs, Keys, Out>
where
    Pairs: BatchReader<Time = ()> + 'static,
    Pairs::Key: Clone + Ord,
    Pairs::Val: Clone,
    Keys: BatchReader<Key = Pairs::Key, Val = (), Time = ()> + 'static,
    Out: ZSet<Key = (Pairs::Key, Pairs::Val)> + 'static,
    Out::R: HasZero,
    Pairs::R: MulByRef<Keys::R, Output = Out::R>,
{
    fn eval(&mut self, pairs: &Pairs, keys: &Keys) -> Out {
        let mut pair_cursor = pairs.cursor();
        let mut key_cursor = keys.cursor();

        // Choose capacity heuristically.
        let mut builder = Out::Builder::with_capacity((), min(pairs.len(), keys.len()));

        // While both keys are valid
        // TODO: Is there a better way to iterate here? `keys_cursor` is the
        //       thing really driving this, so can we just use it as the
        //       source of iteration to do the least work possible?
        while key_cursor.key_valid() && pair_cursor.key_valid() {
            match key_cursor.key().cmp(pair_cursor.key()) {
                // Match up both the cursors
                Ordering::Less => key_cursor.seek_key(pair_cursor.key()),
                Ordering::Greater => pair_cursor.seek_key(key_cursor.key()),

                Ordering::Equal => {
                    // TODO: Can the value of `()` ever be invalid? Do we need an `if
                    // key_cursor.val_valid()` check?
                    let key_weight = key_cursor.weight();
                    while pair_cursor.val_valid() {
                        // Get the weight of the output kv pair by multiplying them together
                        let pair_weight = pair_cursor.weight();
                        let kv_weight = pair_weight.mul_by_ref(&key_weight);

                        // Add to our output batch
                        builder.push((
                            Out::item_from(
                                (pair_cursor.key().clone(), pair_cursor.val().clone()),
                                (),
                            ),
                            kv_weight,
                        ));
                        pair_cursor.step_val();
                    }

                    pair_cursor.step_key();
                    key_cursor.step_key();
                }
            }
        }

        // Create the output stream
        builder.done()
    }
}
