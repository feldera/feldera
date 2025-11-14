use crate::{
    algebra::{IndexedZSet, IndexedZSetReader, MulByRef, ZSet, ZSetReader},
    circuit::{
        circuit_builder::StreamId,
        operator_traits::{BinaryOperator, Operator},
        Scope,
    },
    circuit_cache_key,
    dynamic::{DynPair, DynUnit, Erase},
    trace::{Batch, BatchReaderFactories, Builder, Cursor},
    utils::Tup2,
    Circuit, DBData, Stream, ZWeight,
};
use minitrace::trace;
use std::{
    borrow::Cow,
    cmp::{min, Ordering},
    marker::PhantomData,
    ops::Deref,
};

circuit_cache_key!(SemijoinId<C, D>((StreamId, StreamId) => Stream<C, D>));

pub struct SemijoinStreamFactories<Pairs: IndexedZSetReader, Keys: ZSetReader, Out: ZSet> {
    pairs_factories: Pairs::Factories,
    keys_factories: Keys::Factories,
    output_factories: Out::Factories,
}

impl<Pairs, Keys, Out> SemijoinStreamFactories<Pairs, Keys, Out>
where
    Pairs: IndexedZSetReader,
    Keys: ZSetReader<Key = Pairs::Key>,
    Out: ZSet<Key = DynPair<Pairs::Key, Pairs::Val>>,
{
    pub fn new<KType, VType>() -> Self
    where
        KType: DBData + Erase<Pairs::Key>,
        VType: DBData + Erase<Pairs::Val>,
    {
        Self {
            pairs_factories: BatchReaderFactories::new::<KType, VType, ZWeight>(),
            keys_factories: BatchReaderFactories::new::<KType, (), ZWeight>(),
            output_factories: BatchReaderFactories::new::<Tup2<KType, VType>, (), ZWeight>(),
        }
    }
}

impl<C, Pairs> Stream<C, Pairs>
where
    C: Circuit,
{
    /// See [`Stream::semijoin_stream`].
    pub fn dyn_semijoin_stream<Keys, Out>(
        &self,
        factories: &SemijoinStreamFactories<Pairs, Keys, Out>,
        keys: &Stream<C, Keys>,
    ) -> Stream<C, Out>
    where
        // TODO: Associated type bounds (rust/#52662) really simplify things
        // TODO: Allow non-unit timestamps
        Pairs: IndexedZSet + Send,
        Keys: ZSet<Key = Pairs::Key> + Send,
        // TODO: Should this be `IndexedZSet<Key = Pairs::Key, Val = Pairs::Val>`?
        Out: ZSet<Key = DynPair<Pairs::Key, Pairs::Val>>,
    {
        self.circuit()
            .cache_get_or_insert_with(
                SemijoinId::new((self.stream_id(), keys.stream_id())),
                move || {
                    self.circuit()
                        .add_binary_operator(
                            SemiJoinStream::new(&factories.output_factories),
                            &self.dyn_shard(&factories.pairs_factories),
                            &keys.dyn_shard(&factories.keys_factories),
                        )
                        // This is valid because both of the input streams are sharded. Since this
                        // operator doesn't transform the keys of the inputs
                        // any, the stream they produce is automatically
                        // sharded by the same metric that the inputs are
                        .mark_sharded()
                },
            )
            .clone()
    }
}

/// Semijoin two streams of batches, see [`Stream::dyn_semijoin_stream`]
pub struct SemiJoinStream<Pairs, Keys, Out: Batch> {
    output_factories: Out::Factories,
    _types: PhantomData<(Pairs, Keys, Out)>,
}

impl<Pairs, Keys, Out: Batch> SemiJoinStream<Pairs, Keys, Out> {
    pub fn new(output_factories: &Out::Factories) -> Self {
        Self {
            output_factories: output_factories.clone(),
            _types: PhantomData,
        }
    }
}

impl<Pairs, Keys, Out> Operator for SemiJoinStream<Pairs, Keys, Out>
where
    Pairs: 'static,
    Keys: 'static,
    Out: Batch,
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
    Pairs: IndexedZSetReader,
    Keys: IndexedZSetReader<Key = Pairs::Key, Val = DynUnit>,
    Out: ZSet<Key = DynPair<Pairs::Key, Pairs::Val>>,
{
    #[trace]
    async fn eval(&mut self, pairs: &Pairs, keys: &Keys) -> Out {
        let mut pair_cursor = pairs.cursor();
        let mut key_cursor = keys.cursor();

        let mut item = self.output_factories.key_factory().default_box();

        // Choose capacity heuristically.
        let mut builder = Out::Builder::with_capacity(
            &self.output_factories,
            min(pairs.key_count(), keys.key_count()),
            min(pairs.len(), keys.len()),
        );

        // While both keys are valid
        while key_cursor.key_valid() && pair_cursor.key_valid() {
            match key_cursor.key().cmp(pair_cursor.key()) {
                // Match up both the cursors
                Ordering::Less => key_cursor.seek_key(pair_cursor.key()),
                Ordering::Greater => pair_cursor.seek_key(key_cursor.key()),

                Ordering::Equal => {
                    // TODO: Can the value of `()` ever be invalid? Do we need an `if
                    // key_cursor.val_valid()` check?
                    let key_weight = key_cursor.weight().deref();
                    while pair_cursor.val_valid() {
                        // Get the weight of the output kv pair by multiplying them together
                        let pair_weight = pair_cursor.weight().deref();
                        let mut kv_weight = pair_weight.mul_by_ref(key_weight);

                        item.from_refs(pair_cursor.key(), pair_cursor.val());

                        // Add to our output batch
                        builder.push_val_diff_mut(().erase_mut(), kv_weight.erase_mut());
                        builder.push_key_mut(item.as_mut());
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

    // #[trace] fn eval_owned(&mut self, pairs: Pairs, keys: Keys) -> Out {
    //     // Choose capacity heuristically.
    //     let mut builder = Out::Builder::with_capacity((), min(pairs.len(),
    // keys.len()));

    //     let mut pairs = pairs.consumer();
    //     let mut keys = keys.consumer();

    //     // While both keys are valid
    //     while keys.key_valid() && pairs.key_valid() {
    //         match keys.peek_key().cmp(pairs.peek_key()) {
    //             // Match up both the cursors
    //             Ordering::Less => keys.seek_key(pairs.peek_key()),
    //             Ordering::Greater => pairs.seek_key(keys.peek_key()),

    //             Ordering::Equal => {
    //                 // Get the key's weight
    //                 let (_, mut key_value) = keys.next_key();
    //                 debug_assert!(key_value.value_valid());
    //                 let ((), key_weight, ()) = key_value.next_value();

    //                 // TODO: We could specialize for when pairs has a single
    // value to add the                 // weights by value and to not clone
    // pair_key

    //                 let (pair_key, mut pair_values) = pairs.next_key();
    //                 while pair_values.value_valid() {
    //                     // Get the weight of the output kv pair by multiplying
    // them together                     let (pair_value, pair_weight, ()) =
    // pair_values.next_value();                     let kv_weight =
    // pair_weight.mul_by_ref(&key_weight);

    //                     // Add to our output batch
    //                     builder.push((
    //                         Out::item_from((pair_key.clone(), pair_value), ()),
    //                         kv_weight,
    //                     ));
    //                 }
    //             }
    //         }
    //     }

    //     // Create the output stream
    //     builder.done()
    // }

    // #[trace] fn eval_owned_and_ref(&mut self, pairs: Pairs, keys: &Keys) -> Out {
    //     // Choose capacity heuristically.
    //     let mut builder = Out::Builder::with_capacity((), min(pairs.len(),
    // keys.len()));

    //     let mut pairs = pairs.consumer();
    //     let mut keys = keys.cursor();

    //     // While both keys are valid
    //     while keys.key_valid() && pairs.key_valid() {
    //         match keys.key().cmp(pairs.peek_key()) {
    //             // Match up both the cursors
    //             Ordering::Less => keys.seek_key(pairs.peek_key()),
    //             Ordering::Greater => pairs.seek_key(keys.key()),

    //             Ordering::Equal => {
    //                 // Get the key's weight and its weight
    //                 let key_weight = keys.weight();

    //                 let (pair_key, mut pair_values) = pairs.next_key();
    //                 while pair_values.value_valid() {
    //                     // Get the weight of the output kv pair by multiplying
    // them together                     let (pair_value, pair_weight, ()) =
    // pair_values.next_value();                     let kv_weight =
    // pair_weight.mul_by_ref(&key_weight);

    //                     // Add to our output batch
    //                     builder.push((
    //                         Out::item_from((pair_key.clone(), pair_value), ()),
    //                         kv_weight,
    //                     ));
    //                 }
    //             }
    //         }
    //     }

    //     // Create the output stream
    //     builder.done()
    // }

    // fn input_preference(&self) -> (OwnershipPreference, OwnershipPreference) {
    //     // We get bigger gains from pairs being owned than from keys being owned
    //     (
    //         OwnershipPreference::WEAKLY_PREFER_OWNED,
    //         OwnershipPreference::PREFER_OWNED,
    //     )
    // }
}
