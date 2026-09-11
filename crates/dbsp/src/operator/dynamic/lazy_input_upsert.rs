//! Input upsert that defers its integral lookups to the end of a transaction.
//!
//! [`RootCircuit::dyn_add_lazy_input_map`] offers the same interface as
//! [`RootCircuit::dyn_add_input_map`](crate::RootCircuit::dyn_add_input_map) and
//! the same semantics, but pays for the integral differently.  The eager
//! operator resolves every update against the integral in the step the update
//! arrives, one random probe per key per step.  This one stamps each update with
//! the step it arrived in, accumulates the stamped updates for the whole
//! transaction, and resolves them all in a single ordered scan when the
//! transaction commits.
//!
//! The stamp is what makes that possible.  A Z-set has no order, so without it
//! two updates to one key inside a transaction would be an unordered pair and
//! "which update wins" would have no answer.  The stamp is the last sort
//! component of the value, so records sharing a value stay contiguous within a
//! key, which is what lets the projection that removes it consolidate a run in
//! one pass.

use crate::{
    DBData, ZWeight,
    algebra::{IndexedZSet, OrdIndexedZSet, OrdIndexedZSetFactories},
    circuit::{
        OwnershipPreference, Scope,
        metadata::{BatchSizeStats, INPUT_BATCHES_STATS, OperatorMeta},
        operator_traits::{Operator, UnaryOperator},
    },
    dynamic::{DataTrait, DowncastTrait, DynData, DynPair, DynPairs, Erase, Factory, WithFactory},
    operator::dynamic::input_upsert::{DynUpdate, Update, UpdateRef},
    trace::{BatchReader, BatchReaderFactories, Builder, Spine, Trace, TraceRole},
    utils::Tup2,
};
use std::{borrow::Cow, marker::PhantomData, sync::Arc};

/// The stamped form of `B`: the same keys, with each value paired with the
/// `u32` step it arrived in.
pub type Stamped<B> =
    OrdIndexedZSet<<B as BatchReader>::Key, DynPair<<B as BatchReader>::Val, DynData>>;

/// Factories for [`RootCircuit::dyn_add_lazy_input_map`].
pub struct AddLazyInputMapFactories<B, U>
where
    B: IndexedZSet,
    U: DataTrait + ?Sized,
{
    /// The unstamped batches the operator emits.
    pub batch_factories: B::Factories,

    /// The stamped batches the accumulator holds.  Built with a projection, so
    /// a stamped batch can present itself as an unstamped one.
    pub stamped_factories: <Stamped<B> as BatchReader>::Factories,

    input_pair_factory: &'static dyn Factory<DynPair<B::Key, DynUpdate<B::Val, U>>>,
    input_pairs_factory: &'static dyn Factory<DynPairs<B::Key, DynUpdate<B::Val, U>>>,

    /// A `(value, stamp)` pair, built once per surviving update.
    stamped_val_factory: &'static dyn Factory<DynPair<B::Val, DynData>>,
}

impl<K, V, U> AddLazyInputMapFactories<OrdIndexedZSet<K, V>, U>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    U: DataTrait + ?Sized,
{
    pub fn new<KType, VType, UType>() -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<V>,
        UType: DBData + Erase<U>,
        Tup2<KType, Update<VType, UType>>: DBData,
        Tup2<VType, u32>: DBData + Erase<DynPair<V, DynData>>,
    {
        Self {
            // Built with a projection, so the batches this operator emits can
            // be stamped batches presenting themselves as unstamped ones.
            batch_factories: OrdIndexedZSetFactories::with_projection::<KType, VType, ZWeight>(),
            stamped_factories: BatchReaderFactories::new::<KType, Tup2<VType, u32>, ZWeight>(),
            input_pair_factory: WithFactory::<Tup2<KType, Update<VType, UType>>>::FACTORY,
            input_pairs_factory: WithFactory::<
                crate::dynamic::LeanVec<Tup2<KType, Update<VType, UType>>>,
            >::FACTORY,
            stamped_val_factory: WithFactory::<Tup2<VType, u32>>::FACTORY,
        }
    }
}

impl<B, U> Clone for AddLazyInputMapFactories<B, U>
where
    B: IndexedZSet,
    U: DataTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            batch_factories: self.batch_factories.clone(),
            stamped_factories: self.stamped_factories.clone(),
            input_pair_factory: self.input_pair_factory,
            input_pairs_factory: self.input_pairs_factory,
            stamped_val_factory: self.stamped_val_factory,
        }
    }
}

/// Presents a spine of stamped batches as a spine of unstamped ones.
///
/// Each batch is rewrapped rather than copied: the stamped and unstamped
/// spellings describe the same records, and the projection happens in the
/// cursor.  The resulting spine starts from the batch list alone, so whatever
/// merges the stamped spine had in flight are discarded; preserving them is a
/// separate change.
pub async fn remove_stamp<K, V>(
    factories: &OrdIndexedZSetFactories<K, V>,
    stamped: Vec<Arc<Stamped<OrdIndexedZSet<K, V>>>>,
    name: Arc<String>,
) -> Spine<OrdIndexedZSet<K, V>>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    let mut spine =
        <Spine<OrdIndexedZSet<K, V>> as Trace>::new(factories, name, TraceRole::Integral);
    for batch in stamped {
        spine
            .insert(OrdIndexedZSet::project_batch(factories, &batch))
            .await;
    }
    spine
}

/// Turns each step's updates into a stamped indexed Z-set.
///
/// Per step and per key exactly one update survives -- the last one, in arrival
/// order -- and it becomes one record:
///
/// * `Insert(v)` becomes `(key, (v, stamp))` with weight `+1`.
/// * `Delete` becomes `(key, (v, stamp))` with weight `-1`.  The value is the
///   value factory's default, and which value it is does not matter: the
///   downstream scan retracts the record it belongs to, so the two annihilate
///   and only the retraction of the integral's value survives.  Within a
///   transaction the delta therefore carries a record no reader should read on
///   its own, which is sound because only the committed total is observable.
///
/// The stamp is the step's index within the transaction, so it orders a key's
/// updates across the whole transaction.
pub struct Stamp<K, V, U, B>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    U: DataTrait + ?Sized,
    B: IndexedZSet<Key = K, Val = V>,
{
    factories: AddLazyInputMapFactories<B, U>,

    /// Steps elapsed in this transaction, and so the stamp the next step's
    /// updates carry.
    stamp: u32,

    input_batch_stats: BatchSizeStats,
    phantom: PhantomData<fn(&K, &V, &U)>,
}

impl<K, V, U, B> Stamp<K, V, U, B>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    U: DataTrait + ?Sized,
    B: IndexedZSet<Key = K, Val = V>,
{
    pub fn new(factories: &AddLazyInputMapFactories<B, U>) -> Self {
        Self {
            factories: factories.clone(),
            stamp: 0,
            input_batch_stats: BatchSizeStats::new(),
            phantom: PhantomData,
        }
    }
}

impl<K, V, U, B> Operator for Stamp<K, V, U, B>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    U: DataTrait + ?Sized,
    B: IndexedZSet<Key = K, Val = V>,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("Stamp")
    }

    /// The stamp orders updates within one transaction and nothing beyond it,
    /// so it starts over with each transaction.
    fn start_transaction(&mut self) {
        self.stamp = 0;
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        meta.extend(metadata! {
            INPUT_BATCHES_STATS => self.input_batch_stats.metadata(),
        });
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<K, V, U, B> UnaryOperator<Vec<Box<DynPairs<K, DynUpdate<V, U>>>>, Stamped<B>>
    for Stamp<K, V, U, B>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    U: DataTrait + ?Sized,
    B: IndexedZSet<Key = K, Val = V>,
{
    async fn eval(&mut self, _updates: &Vec<Box<DynPairs<K, DynUpdate<V, U>>>>) -> Stamped<B> {
        // The operator sorts its input in place, so it cannot take it by
        // reference.  Nothing else reads the input stream, so a correctly built
        // circuit never asks it to.
        panic!("Stamp::eval(): cannot accept updates by reference")
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::STRONGLY_PREFER_OWNED
    }

    async fn eval_owned(
        &mut self,
        mut updates: Vec<Box<DynPairs<K, DynUpdate<V, U>>>>,
    ) -> Stamped<B> {
        let stamp = self.stamp;
        // A transaction of four billion steps is not reachable in practice, but
        // wrapping here would silently reorder a key's updates.
        self.stamp = self
            .stamp
            .checked_add(1)
            .expect("a transaction cannot hold more steps than a stamp can count");

        // The merge below walks the vectors in key order and needs a key's
        // updates in the order the client wrote them, and nothing upstream
        // promises either: the client appends in whatever order it likes, a key
        // as often as it likes.  The sort establishes both, being stable and
        // comparing keys alone.
        //
        // The dedup only trims work.  A key's updates within one step share a
        // stamp, so the merge keeps the last of them either way; dropping the
        // rest here keeps them out of it.
        updates.retain_mut(|pairs| {
            pairs.sort_by_key();
            pairs.dedup_by_key_keep_last();
            !pairs.is_empty()
        });

        // The vectors are in append order, so an n-way merge visits a key's
        // updates in that order too: `min_by` returns the first among equal
        // keys.
        let mut sources: Vec<(&DynPairs<K, DynUpdate<V, U>>, usize)> =
            updates.iter().map(|pairs| (&**pairs, 0)).collect();
        let n_updates: usize = sources.iter().map(|(pairs, _)| pairs.len()).sum();
        self.input_batch_stats.add_batch(n_updates);

        let mut builder = <Stamped<B> as crate::trace::Batch>::Builder::with_capacity(
            &self.factories.stamped_factories,
            n_updates,
            n_updates,
        );

        // The surviving update for the key under construction.  The key is
        // cloned into one box that lasts the whole call, since the merge only
        // ever needs the key it is currently on.
        let mut cur_key = self.factories.batch_factories.key_factory().default_box();
        let mut have_key = false;
        let mut surviving_val = self.factories.batch_factories.val_factory().default_box();
        let mut surviving_weight: ZWeight = 0;

        let mut stamped_val = self.factories.stamped_val_factory.default_box();
        let default_val = self.factories.batch_factories.val_factory().default_box();

        loop {
            let next = sources
                .iter()
                .map(|(pairs, index)| pairs.index(*index))
                .enumerate()
                .min_by(|(_, a), (_, b)| a.fst().cmp(b.fst()));

            let Some((source, pair)) = next else { break };
            sources[source].1 += 1;
            if sources[source].1 >= sources[source].0.len() {
                sources.remove(source);
            }

            let (key, update) = pair.split();

            if !have_key || &*cur_key != key {
                if have_key {
                    push_stamped::<K, V, B>(
                        &mut builder,
                        &mut stamped_val,
                        &cur_key,
                        &mut surviving_val,
                        surviving_weight,
                        stamp,
                    );
                }
                key.clone_to(&mut cur_key);
                have_key = true;
            }

            // The last update for a key wins, so each one simply overwrites the
            // one before it.
            match update.get() {
                UpdateRef::Insert(val) => {
                    val.clone_to(&mut surviving_val);
                    surviving_weight = 1;
                }
                UpdateRef::Delete => {
                    // Any value works here; see the type's documentation.
                    default_val.clone_to(&mut surviving_val);
                    surviving_weight = -1;
                }
                UpdateRef::Update(_) => panic!(
                    "dyn_add_lazy_input_map does not support `Update` commands, only `Insert` and `Delete`"
                ),
            }
        }

        if have_key {
            push_stamped::<K, V, B>(
                &mut builder,
                &mut stamped_val,
                &cur_key,
                &mut surviving_val,
                surviving_weight,
                stamp,
            );
        }

        builder.done()
    }
}

/// Writes one `(key, (value, stamp))` record with the given weight.
fn push_stamped<K, V, B>(
    builder: &mut <Stamped<B> as crate::trace::Batch>::Builder,
    stamped_val: &mut Box<DynPair<V, DynData>>,
    key: &K,
    val: &mut Box<V>,
    weight: ZWeight,
    stamp: u32,
) where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    B: IndexedZSet<Key = K, Val = V>,
{
    let (value_half, stamp_half) = stamped_val.split_mut();
    val.move_to(value_half);
    *unsafe { stamp_half.downcast_mut::<u32>() } = stamp;

    let mut weight = weight;
    builder.push_val_diff_mut(&mut **stamped_val, weight.erase_mut());
    builder.push_key(key);
}

#[cfg(test)]
mod stamp_tests {
    use super::*;
    use crate::dynamic::LeanVec;
    use crate::trace::{Cursor, test::run_in_circuit_with_storage};
    use feldera_storage::tokio::TOKIO;

    type Key = DynData;
    type Val = DynData;
    type Upd = DynData;
    type Batched = OrdIndexedZSet<Key, Val>;

    /// `Some(v)` is an insert, `None` a delete.
    type Command = (i32, Option<i32>);

    fn factories() -> AddLazyInputMapFactories<Batched, Upd> {
        AddLazyInputMapFactories::new::<i32, i32, i32>()
    }

    /// One worker's commands for a step, in the order the client wrote them.
    /// The operator sorts them, so they need no order here.
    fn commands(commands: &[Command]) -> Box<DynPairs<Key, DynUpdate<Val, Upd>>> {
        let pairs: Vec<Tup2<i32, Update<i32, i32>>> = commands
            .iter()
            .map(|&(key, value)| {
                Tup2(
                    key,
                    match value {
                        Some(value) => Update::Insert(value),
                        None => Update::Delete,
                    },
                )
            })
            .collect();
        Box::new(LeanVec::from(pairs)).erase_box()
    }

    /// `(key, value, stamp, weight)` for every record a step emits.
    fn step(
        operator: &mut Stamp<Key, Val, Upd, Batched>,
        vectors: &[&[Command]],
    ) -> Vec<(i32, i32, u32, ZWeight)> {
        let input: Vec<Box<DynPairs<Key, DynUpdate<Val, Upd>>>> =
            vectors.iter().map(|v| commands(v)).collect();
        let batch = TOKIO.block_on(operator.eval_owned(input));

        let mut out = Vec::new();
        let mut cursor = batch.cursor();
        while cursor.key_valid() {
            while cursor.val_valid() {
                let Tup2(value, stamp) = *unsafe { cursor.val().downcast::<Tup2<i32, u32>>() };
                out.push((
                    *unsafe { cursor.key().downcast::<i32>() },
                    value,
                    stamp,
                    **cursor.weight(),
                ));
                cursor.step_val();
            }
            cursor.step_key();
        }
        out
    }

    /// An insert becomes a `+1` record and a delete a `-1` record, both carrying
    /// the step's stamp.
    #[test]
    fn an_insert_and_a_delete_become_one_record_each() {
        run_in_circuit_with_storage(|| {
            let factories = factories();
            let mut operator = Stamp::new(&factories);
            let records = step(&mut operator, &[&[(1, Some(10)), (2, None)]]);

            assert_eq!(records.len(), 2, "one record per key: {records:?}");
            assert_eq!(records[0], (1, 10, 0, 1));
            // The delete's value is a default and carries no meaning; only its
            // key, stamp and weight do.
            let (key, _value, stamp, weight) = records[1];
            assert_eq!((key, stamp, weight), (2, 0, -1));
        });
    }

    /// The map a set of vectors describes: the last update to a key across all
    /// of them wins, and a delete travels as the value type's default.
    fn merged(vectors: &[&[Command]]) -> Vec<(i32, i32, u32, ZWeight)> {
        let mut map: std::collections::BTreeMap<i32, (i32, ZWeight)> =
            std::collections::BTreeMap::new();
        for vector in vectors {
            for &(key, value) in vector.iter() {
                map.insert(key, value.map_or((0, -1), |value| (value, 1)));
            }
        }
        map.into_iter()
            .map(|(key, (value, weight))| (key, value, 0, weight))
            .collect()
    }

    /// Checks the merge against the model on vectors written out by hand.
    fn merges_to_the_model(vectors: &[&[Command]]) {
        let factories = factories();
        let mut operator = Stamp::new(&factories);
        assert_eq!(
            step(&mut operator, vectors),
            merged(vectors),
            "the merge disagrees with the model on {vectors:?}"
        );
    }

    /// A key written by every vector takes the last vector's update.
    #[test]
    fn every_vector_writes_one_key() {
        run_in_circuit_with_storage(|| {
            merges_to_the_model(&[&[(1, Some(10))], &[(1, Some(20))], &[(1, Some(30))]]);
            merges_to_the_model(&[&[(1, Some(10))], &[(1, None)], &[(1, Some(30))]]);
            merges_to_the_model(&[&[(1, Some(10))], &[(1, Some(20))], &[(1, None)]]);
            merges_to_the_model(&[&[(1, None)], &[(1, None)], &[(1, Some(30))]]);
        });
    }

    /// A key skipped by a middle vector still takes the last update written to
    /// it, not the one from the vector before the gap.
    #[test]
    fn a_key_skips_a_vector() {
        run_in_circuit_with_storage(|| {
            merges_to_the_model(&[&[(1, Some(10))], &[(2, Some(20))], &[(1, Some(30))]]);
            merges_to_the_model(&[
                &[(1, Some(10)), (2, Some(11))],
                &[(2, Some(20))],
                &[(1, Some(30))],
            ]);
        });
    }

    /// Vectors of different lengths, so the merge runs out of one while others
    /// still have keys.
    ///
    /// Exhausting a source drops it from the list the merge scans, which shifts
    /// the positions of every source after it.
    #[test]
    fn a_vector_runs_out_before_the_others() {
        run_in_circuit_with_storage(|| {
            // The first source empties first, then the second.
            merges_to_the_model(&[
                &[(1, Some(10))],
                &[(1, Some(20)), (2, Some(21))],
                &[(1, Some(30)), (2, Some(31)), (3, Some(32))],
            ]);
            // The last source empties first.
            merges_to_the_model(&[
                &[(1, Some(10)), (2, Some(11)), (3, Some(12))],
                &[(1, Some(20)), (2, Some(21))],
                &[(1, Some(30))],
            ]);
            // A middle source empties first, so the survivors close the gap it
            // leaves behind.
            merges_to_the_model(&[
                &[(1, Some(10)), (5, Some(11))],
                &[(1, Some(20))],
                &[(1, Some(30)), (5, Some(31))],
            ]);
        });
    }

    /// Keys interleaved across vectors, so the merge alternates sources.
    #[test]
    fn keys_interleave_across_vectors() {
        run_in_circuit_with_storage(|| {
            merges_to_the_model(&[
                &[(1, Some(10)), (4, Some(13))],
                &[(2, Some(21)), (3, Some(22))],
            ]);
            merges_to_the_model(&[
                &[(1, Some(10)), (3, Some(12))],
                &[(2, Some(21)), (4, Some(23))],
                &[(1, Some(30)), (2, Some(31)), (3, Some(32)), (4, Some(33))],
            ]);
        });
    }

    /// Empty vectors among the others, which the merge drops before it starts.
    #[test]
    fn empty_vectors_among_the_others() {
        run_in_circuit_with_storage(|| {
            merges_to_the_model(&[&[], &[(1, Some(10))], &[]]);
            merges_to_the_model(&[&[], &[], &[]]);
            merges_to_the_model(&[&[(1, Some(10))], &[], &[(1, Some(20))]]);
        });
    }

    /// More vectors than a client would send, all writing the same two keys.
    #[test]
    fn many_vectors_write_the_same_keys() {
        run_in_circuit_with_storage(|| {
            let vectors: Vec<Vec<Command>> = (0..16)
                .map(|round| vec![(1, Some(round)), (2, Some(100 + round))])
                .collect();
            let borrowed: Vec<&[Command]> = vectors.iter().map(|v| v.as_slice()).collect();
            merges_to_the_model(&borrowed);
        });
    }

    /// Vectors as `Stamp` requires them: sorted by key, each key once.
    ///
    /// Keys are drawn from a small range so that vectors overlap heavily, which
    /// is what makes the merge choose between them.
    fn a_vector() -> impl proptest::strategy::Strategy<Value = Vec<Command>> {
        use proptest::prelude::*;
        proptest::collection::btree_map(
            0i32..6,
            prop_oneof![Just(None), (0i32..4).prop_map(Some)],
            0..6,
        )
        .prop_map(|map| map.into_iter().collect())
    }

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(256))]

        /// The merge keeps the last update to each key across every vector.
        #[test]
        fn a_merge_of_random_vectors_agrees_with_the_model(
            vectors in proptest::collection::vec(a_vector(), 0..6)
        ) {
            run_in_circuit_with_storage(move || {
                let borrowed: Vec<&[Command]> = vectors.iter().map(|v| v.as_slice()).collect();
                merges_to_the_model(&borrowed);
            });
        }
    }

    /// The operator sorts what it is handed, so a client can append in any order
    /// and write a key as often as it likes.
    ///
    /// Two updates to one key within a step share a stamp, which cannot tell
    /// them apart, so the order the client wrote them in is what decides: the
    /// last one wins.
    #[test]
    fn a_vector_is_sorted_before_the_merge() {
        run_in_circuit_with_storage(|| {
            let factories = factories();

            // Out of key order, with key 1 written three times.
            let mut operator = Stamp::new(&factories);
            assert_eq!(
                step(
                    &mut operator,
                    &[&[
                        (3, Some(30)),
                        (1, Some(10)),
                        (2, Some(20)),
                        (1, Some(11)),
                        (1, Some(12)),
                    ]]
                ),
                vec![(1, 12, 0, 1), (2, 20, 0, 1), (3, 30, 0, 1)]
            );

            // A delete after inserts is the write that survives.
            let mut operator = Stamp::new(&factories);
            let records = step(&mut operator, &[&[(1, Some(10)), (1, None)]]);
            assert_eq!(records.len(), 1);
            let (key, _value, stamp, weight) = records[0];
            assert_eq!((key, stamp, weight), (1, 0, -1));

            // An insert after a delete likewise.
            let mut operator = Stamp::new(&factories);
            assert_eq!(
                step(&mut operator, &[&[(1, None), (1, Some(10))]]),
                vec![(1, 10, 0, 1)]
            );

            // An empty vector drops out of the merge.
            let mut operator = Stamp::new(&factories);
            assert_eq!(
                step(&mut operator, &[&[], &[(1, Some(10))]]),
                vec![(1, 10, 0, 1)]
            );
        });
    }

    /// Only the last update for a key survives a step.
    ///
    /// Each vector holds a key once, so the earlier updates are in earlier
    /// vectors, and the merge is what has to put them in order.
    #[test]
    fn the_last_update_for_a_key_wins() {
        run_in_circuit_with_storage(|| {
            let factories = factories();

            // The later vector's update is the later arrival.
            let mut operator = Stamp::new(&factories);
            let records = step(&mut operator, &[&[(1, Some(10))], &[(1, Some(20))]]);
            assert_eq!(records, vec![(1, 20, 0, 1)]);

            // A delete after an insert leaves the delete.
            let mut operator = Stamp::new(&factories);
            let records = step(&mut operator, &[&[(1, Some(10))], &[(1, None)]]);
            assert_eq!(records.len(), 1);
            let (key, _value, stamp, weight) = records[0];
            assert_eq!((key, stamp, weight), (1, 0, -1));
        });
    }

    /// The stamp counts steps within a transaction and starts over with each
    /// transaction, so it orders a key's updates across the transaction and
    /// nothing beyond it.
    #[test]
    fn the_stamp_counts_steps_and_resets_per_transaction() {
        run_in_circuit_with_storage(|| {
            let factories = factories();
            let mut operator = Stamp::new(&factories);

            assert_eq!(
                step(&mut operator, &[&[(1, Some(10))]]),
                vec![(1, 10, 0, 1)]
            );
            assert_eq!(
                step(&mut operator, &[&[(1, Some(11))]]),
                vec![(1, 11, 1, 1)]
            );
            assert_eq!(
                step(&mut operator, &[&[(1, Some(12))]]),
                vec![(1, 12, 2, 1)]
            );

            operator.start_transaction();
            assert_eq!(
                step(&mut operator, &[&[(1, Some(13))]]),
                vec![(1, 13, 0, 1)]
            );
        });
    }

    /// A step with nothing in it emits nothing.
    #[test]
    fn an_empty_step_emits_nothing() {
        run_in_circuit_with_storage(|| {
            let factories = factories();
            let mut operator = Stamp::new(&factories);
            assert_eq!(step(&mut operator, &[]), vec![]);
            assert_eq!(step(&mut operator, &[&[]]), vec![]);
        });
    }

    /// A transaction longer than the stamp can count is refused rather than
    /// wrapped.
    ///
    /// Wrapping would hand a later step a smaller stamp than an earlier one, and
    /// the resolution keeps the largest, so the transaction would silently take
    /// an update the client had already replaced.
    #[test]
    #[should_panic(expected = "more steps than a stamp can count")]
    fn a_transaction_longer_than_the_stamp_can_count_is_refused() {
        run_in_circuit_with_storage(|| {
            let factories = factories();
            let mut operator = Stamp::new(&factories);
            operator.stamp = u32::MAX;
            let _ = TOKIO.block_on(operator.eval_owned(Vec::new()));
        });
    }

    /// The operator resolves updates by keeping the last one, which an `Update`
    /// command cannot express, so it is rejected rather than misapplied.
    #[test]
    #[should_panic(expected = "does not support `Update`")]
    fn an_update_command_is_rejected() {
        run_in_circuit_with_storage(|| {
            let factories = factories();
            let mut operator = Stamp::new(&factories);
            let pairs: Vec<Tup2<i32, Update<i32, i32>>> = vec![Tup2(1, Update::Update(7))];
            let input: Vec<Box<DynPairs<Key, DynUpdate<Val, Upd>>>> =
                vec![Box::new(LeanVec::from(pairs)).erase_box()];
            let _ = TOKIO.block_on(operator.eval_owned(input));
        });
    }
}

#[cfg(test)]
mod remove_stamp_tests {
    use super::*;
    use crate::dynamic::LeanVec;
    use crate::trace::{Batch, BatchLocation, Cursor, test::run_in_circuit_with_storage};
    use feldera_storage::tokio::TOKIO;

    type Key = DynData;
    type Val = DynData;
    type Batched = OrdIndexedZSet<Key, Val>;

    /// `(key, value, stamp, weight)`.
    type Row = (i32, i32, u32, ZWeight);

    fn factories() -> AddLazyInputMapFactories<Batched, DynData> {
        AddLazyInputMapFactories::new::<i32, i32, i32>()
    }

    fn stamped_batch(
        factories: &AddLazyInputMapFactories<Batched, DynData>,
        rows: &[Row],
    ) -> Stamped<Batched> {
        let tuples: Vec<Tup2<Tup2<i32, Tup2<i32, u32>>, ZWeight>> = rows
            .iter()
            .map(|&(key, value, stamp, weight)| Tup2(Tup2(key, Tup2(value, stamp)), weight))
            .collect();
        let mut tuples = Box::new(LeanVec::from(tuples)).erase_box();
        <Stamped<Batched> as crate::trace::Batch>::dyn_from_tuples(
            &factories.stamped_factories,
            (),
            &mut tuples,
        )
    }

    /// Every `(key, value, weight)` a batch or trace exposes.
    fn contents<B>(batch: &B) -> Vec<(i32, i32, ZWeight)>
    where
        B: BatchReader<Key = Key, Val = Val, Time = (), R = crate::DynZWeight>,
    {
        let mut out = Vec::new();
        let mut cursor = batch.cursor();
        while cursor.key_valid() {
            while cursor.val_valid() {
                out.push((
                    *unsafe { cursor.key().downcast::<i32>() },
                    *unsafe { cursor.val().downcast::<i32>() },
                    **cursor.weight(),
                ));
                cursor.step_val();
            }
            cursor.step_key();
        }
        out
    }

    /// A stamped batch presents itself as an unstamped one: the stamp is
    /// invisible, records differing only in it consolidate, and a record left
    /// with no weight disappears along with a key left with nothing.
    #[test]
    fn a_stamped_batch_projects_to_its_values() {
        run_in_circuit_with_storage(|| {
            let factories = factories();
            let rows: &[Row] = &[
                (1, 10, 0, 1),
                (1, 10, 3, 2),
                (1, 20, 1, 1),
                (2, 30, 0, 1),
                (2, 30, 4, -1),
            ];
            let projected = Batched::project_batch(
                &factories.batch_factories,
                &stamped_batch(&factories, rows),
            );
            assert_eq!(contents(&projected), vec![(1, 10, 3), (1, 20, 1)]);
        });
    }

    /// A stamped batch that has spilled projects the same way.
    #[test]
    fn a_spilled_stamped_batch_projects_the_same() {
        run_in_circuit_with_storage(|| {
            let factories = factories();
            let rows: &[Row] = &[(1, 10, 0, 1), (1, 10, 3, 2), (1, 20, 1, 1)];

            let stamped = stamped_batch(&factories, rows);
            let spilled = stamped
                .persisted()
                .expect("an in-memory batch persists to storage");
            assert_eq!(spilled.location(), BatchLocation::Storage);

            let projected = Batched::project_batch(&factories.batch_factories, &spilled);
            assert_eq!(contents(&projected), vec![(1, 10, 3), (1, 20, 1)]);
        });
    }

    /// A spine of stamped batches reads as a spine of unstamped ones, across
    /// batch boundaries: a value split over two batches consolidates once the
    /// spine merges them.
    #[test]
    fn a_stamped_spine_reads_as_an_unstamped_one() {
        run_in_circuit_with_storage(|| {
            let factories = factories();

            let mut stamped: Spine<Stamped<Batched>> = <Spine<_> as Trace>::new(
                &factories.stamped_factories,
                Arc::new(String::from("stamped")),
                TraceRole::Integral,
            );
            TOKIO.block_on(
                stamped.insert(stamped_batch(&factories, &[(1, 10, 0, 1), (2, 30, 0, 1)])),
            );
            TOKIO.block_on(
                stamped.insert(stamped_batch(&factories, &[(1, 10, 1, 2), (2, 30, 1, -1)])),
            );

            let unstamped = TOKIO.block_on(remove_stamp(
                &factories.batch_factories,
                stamped.get_batches(),
                Arc::new(String::from("unstamped")),
            ));

            // Key 1 keeps value 10 with the summed weight; key 2's two stamps
            // cancel, so both the value and the key are gone.
            assert_eq!(contents(&unstamped), vec![(1, 10, 3)]);
        });
    }
}
