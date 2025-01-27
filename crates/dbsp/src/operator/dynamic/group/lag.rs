use super::{GroupTransformer, Monotonicity};
use crate::algebra::{OrdIndexedZSetFactories, ZRingValue};
use crate::operator::dynamic::filter_map::DynFilterMap;
use crate::{
    algebra::{HasZero, IndexedZSet, OrdIndexedZSet, ZCursor},
    dynamic::{
        ClonableTrait, DataTrait, DynData, DynPair, DynUnit, DynVec, Erase, Factory, LeanVec,
        WithFactory,
    },
    operator::dynamic::MonoIndexedZSet,
    trace::{
        cursor::{CursorPair, ReverseKeyCursor},
        BatchReaderFactories,
    },
    utils::Tup2,
    DBData, DynZWeight, RootCircuit, Stream, ZWeight,
};
use std::{cmp::Ordering, marker::PhantomData, ops::Neg};

const MAX_RETRACTIONS_CAPACITY: usize = 100_000usize;

pub struct LagFactories<B: IndexedZSet, OV: DataTrait + ?Sized> {
    input_factories: B::Factories,
    output_factories: OrdIndexedZSetFactories<B::Key, DynPair<B::Val, OV>>,
    keys_factory: &'static dyn Factory<AffectedKeys<B::Val>>,
    output_val_factory: &'static dyn Factory<OV>,
}

impl<B, OV> LagFactories<B, OV>
where
    B: IndexedZSet,
    OV: DataTrait + ?Sized,
{
    pub fn new<KType, VType, OVType>() -> Self
    where
        KType: DBData + Erase<B::Key>,
        VType: DBData + Erase<B::Val>,
        OVType: DBData + Erase<OV>,
    {
        Self {
            input_factories: BatchReaderFactories::new::<KType, VType, ZWeight>(),
            output_factories: BatchReaderFactories::new::<KType, Tup2<VType, OVType>, ZWeight>(),
            keys_factory: WithFactory::<LeanVec<VType>>::FACTORY,
            output_val_factory: WithFactory::<OVType>::FACTORY,
        }
    }
}

pub struct LagCustomOrdFactories<
    B: IndexedZSet,
    V2: DataTrait + ?Sized,
    VL: DataTrait + ?Sized,
    OV: DataTrait + ?Sized,
> {
    lag_factories: LagFactories<OrdIndexedZSet<B::Key, V2>, VL>,
    output_factories: OrdIndexedZSetFactories<B::Key, OV>,
}

impl<B, V2, VL, OV> LagCustomOrdFactories<B, V2, VL, OV>
where
    B: IndexedZSet,
    V2: DataTrait + ?Sized,
    VL: DataTrait + ?Sized,
    OV: DataTrait + ?Sized,
{
    pub fn new<KType, VType, V2Type, VLType, OVType>() -> Self
    where
        KType: DBData + Erase<B::Key>,
        VType: DBData + Erase<B::Val>,
        V2Type: DBData + Erase<V2>,
        VLType: DBData + Erase<VL>,
        OVType: DBData + Erase<OV>,
    {
        Self {
            lag_factories: LagFactories::new::<KType, V2Type, VLType>(),
            output_factories: BatchReaderFactories::new::<KType, OVType, ZWeight>(),
        }
    }
}

impl<B> Stream<RootCircuit, B>
where
    B: IndexedZSet + Send,
{
    /// See [`Stream::lag`].
    #[allow(clippy::type_complexity)]
    pub fn dyn_lag<OV>(
        &self,
        factories: &LagFactories<B, OV>,
        offset: isize,
        project: Box<dyn Fn(Option<&B::Val>, &mut OV)>,
    ) -> Stream<RootCircuit, OrdIndexedZSet<B::Key, DynPair<B::Val, OV>>>
    where
        OV: DataTrait + ?Sized,
    {
        self.dyn_group_transform(
            &factories.input_factories,
            &factories.output_factories,
            Box::new(Lag::new(
                factories.output_factories.val_factory(),
                factories.keys_factory,
                factories.output_val_factory,
                offset.unsigned_abs(),
                offset > 0,
                project,
                if offset > 0 {
                    |k1: &B::Val, k2: &B::Val| k1.cmp(k2)
                } else {
                    |k1: &B::Val, k2: &B::Val| k2.cmp(k1)
                },
            )),
        )
    }
}

impl Stream<RootCircuit, MonoIndexedZSet> {
    pub fn dyn_lag_custom_order_mono(
        &self,
        factories: &LagCustomOrdFactories<MonoIndexedZSet, DynData, DynData, DynData>,
        offset: isize,
        encode: Box<dyn Fn(&DynData, &mut DynData)>,
        project: Box<dyn Fn(Option<&DynData>, &mut DynData)>,
        decode: Box<dyn Fn(&DynData, &DynData, &mut DynData)>,
    ) -> Stream<RootCircuit, MonoIndexedZSet> {
        self.dyn_lag_custom_order(factories, offset, encode, project, decode)
    }
}

impl<B, K, V> Stream<RootCircuit, B>
where
    B: IndexedZSet<Key = K, Val = V> + Send,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    /// See [`Stream::lag_custom_order`].
    pub fn dyn_lag_custom_order<V2, VL, OV>(
        &self,
        factories: &LagCustomOrdFactories<B, V2, VL, OV>,
        offset: isize,
        encode: Box<dyn Fn(&V, &mut V2)>,
        project: Box<dyn Fn(Option<&V2>, &mut VL)>,
        decode: Box<dyn Fn(&V2, &VL, &mut OV)>,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    where
        V2: DataTrait + ?Sized,
        VL: DataTrait + ?Sized,
        OV: DataTrait + ?Sized,
        B: for<'a> DynFilterMap<DynItemRef<'a> = (&'a K, &'a V)>,
    {
        self.dyn_map_index(
            &factories.lag_factories.input_factories,
            Box::new(move |(k, v), kv| {
                let (out_k, out_v) = kv.split_mut();
                k.clone_to(out_k);
                encode(v, out_v);
            }),
        )
        .dyn_lag(&factories.lag_factories, offset, project)
        .dyn_map_index(
            &factories.output_factories,
            Box::new(move |(k, v), kv| {
                let (out_k, out_v) = kv.split_mut();
                let (v1, v2) = v.split();
                k.clone_to(out_k);
                decode(v1, v2, out_v);
            }),
        )
    }
}

/// Implement both `lag` and `lead` operators.
struct Lag<I: DataTrait + ?Sized, O: DataTrait + ?Sized, KCF> {
    name: String,
    lag: usize,
    /// `true` for `lag`, `false` for `lead`.
    asc: bool,
    project: Box<dyn Fn(Option<&I>, &mut O)>,
    output_pair_factory: &'static dyn Factory<DynPair<I, O>>,
    output_val_factory: &'static dyn Factory<O>,
    /// List of keys that must be re-evaluated, computed during
    /// the forward pass of the algorithm.
    affected_keys: Box<DynVec<I>>,
    /// Keys encountered during the backward pass of the algorithm.
    ///
    /// See `struct EncounteredKey`.
    encountered_keys: Vec<EncounteredKey>,
    /// Index of the next key from `affected_keys` we expect to
    /// encounter.
    next_key: isize,
    /// Number of steps the input cursor took after encountering
    /// the previous key from `affected_keys`.
    offset_from_prev: ZWeight,
    /// For the purpose of computing `lag`, we iterate over a key with weight
    /// `w` as if it occurred `w` times in the trace.  This field stores the
    /// remaining number of steps (since it is not tracked by the cursor).
    remaining_weight: ZWeight,
    /// Key comparison function.  Set to `cmp` for ascending
    /// order and the reverse of `cmp` for descending order.
    key_cmp: KCF,
    output_pair: Box<DynPair<I, O>>,
    _phantom: PhantomData<fn(&I, &O)>,
}

impl<I, O, KCF> Lag<I, O, KCF>
where
    I: DataTrait + ?Sized,
    O: DataTrait + ?Sized,
    KCF: Fn(&I, &I) -> Ordering + 'static,
{
    #[allow(clippy::too_many_arguments)]
    fn new(
        output_pair_factory: &'static dyn Factory<DynPair<I, O>>,
        keys_factory: &'static dyn Factory<DynVec<I>>,
        output_val_factory: &'static dyn Factory<O>,
        lag: usize,
        asc: bool,
        project: Box<dyn Fn(Option<&I>, &mut O)>,
        key_cmp: KCF,
    ) -> Self {
        Self {
            name: format!("{}({lag})", if asc { "lag" } else { "lead" }),
            output_pair_factory,
            output_val_factory,
            lag,
            asc,
            project,
            affected_keys: keys_factory.default_box(),
            encountered_keys: Vec::new(),
            next_key: 0,
            offset_from_prev: 0,
            remaining_weight: 0,
            key_cmp,
            output_pair: output_pair_factory.default_box(),
            _phantom: PhantomData,
        }
    }

    /// Add `key` to `self.affected_keys`.
    fn record_affected_key(&mut self, key: &I) -> usize {
        let idx = self.affected_keys.len();
        self.affected_keys.push_ref(key);
        self.encountered_keys.push(EncounteredKey::new());
        idx
    }

    /// Retract all entries from the output trace whose first element is
    /// equal to `cursor.key().fst()`; return total weight of removed records.
    fn retract_key(
        &mut self,
        cursor: &mut dyn ZCursor<DynPair<I, O>, DynUnit, ()>,
        output_cb: &mut dyn FnMut(&mut DynPair<I, O>, &mut DynZWeight),
    ) -> usize {
        debug_assert!(cursor.key_valid());

        let mut result = 0;

        let idx = self.record_affected_key(cursor.key().fst());

        while cursor.key_valid() && cursor.key().fst() == self.affected_keys.index(idx) {
            let weight = **cursor.weight();
            // The output trace should not contain negative weights by construction.
            debug_assert!(weight > 0);
            cursor.key().clone_to(self.output_pair.as_mut());
            //println!("retract: {:?} with weight {weight}", &self.output_pair);
            output_cb(self.output_pair.as_mut(), weight.neg().erase_mut());
            result += weight as usize;
            step_key_skip_zeros(cursor);
        }
        skip_zeros(cursor);

        result
    }

    /// Move input `cursor` `steps` steps back.  Keep track of keys in the
    /// `self.affected_keys` array encountered along the way, update their
    /// `offset` and `weight` fields in `self.encountered_keys`.
    fn step_key_reverse_n(&mut self, cursor: &mut dyn ZCursor<I, DynUnit, ()>, mut steps: usize) {
        // println!(
        //     "step_key_reverse_n {steps} (offset_from_prev = {}, remaining_weight = {})",
        //     self.offset_from_prev, self.remaining_weight
        // );
        while steps > 0 && cursor.key_valid() {
            steps -= 1;
            self.offset_from_prev += 1;

            //println!("remaining_weight: {}", self.remaining_weight);
            if self.remaining_weight > 0 {
                self.remaining_weight -= 1;
            }

            if self.remaining_weight == 0 {
                step_key_reverse_skip_non_positive(cursor);
                if cursor.key_valid() {
                    self.remaining_weight = **cursor.weight();
                    // println!(
                    //     "key valid, key: {:?}, weight: {}",
                    //     cursor.key(),
                    //     self.remaining_weight
                    // );
                }

                //println!("next_key: {}", self.next_key);

                while self.next_key >= 0
                    && (!cursor.key_valid()
                        || (self.key_cmp)(
                            cursor.key(),
                            &self.affected_keys[self.next_key as usize],
                        ) == Ordering::Less)
                {
                    //println!("skipped {:?}", &self.affected_keys[self.next_key as usize]);
                    self.encountered_keys[self.next_key as usize].offset = Some(None);
                    self.next_key -= 1;
                }

                if cursor.key_valid()
                    && self.next_key >= 0
                    && cursor.key() == &self.affected_keys[self.next_key as usize]
                {
                    // println!(
                    //     "encountered {:?} at offset {} with weight {}",
                    //     &self.affected_keys[self.next_key as usize],
                    //     self.offset_from_prev,
                    //     self.remaining_weight
                    // );
                    debug_assert!(self.offset_from_prev >= 0);

                    self.encountered_keys[self.next_key as usize].offset =
                        Some(Some(self.offset_from_prev as usize));
                    self.offset_from_prev = -self.remaining_weight;
                    self.encountered_keys[self.next_key as usize].weight = self.remaining_weight;
                    self.next_key -= 1;
                }
            }
        }
    }

    /// Forward pass: compute keys that require updates.  Retract them from
    /// output trace and record the keys in `self.affected_keys`, so we can replace
    /// them with new values during the backward pass.
    ///
    /// # Example
    ///
    /// Consider `lag(3)` operator and assume that the input collection
    /// contains values `{1, 2, 3, 4, 6, 7, 8, 9, 10}`, all with weight 1,
    /// and the delta contains keys `{0 => 1, 5 => 1}`.  The set
    /// of affected keys includes all keys in delta and for each key in
    /// delta `3` following keys in the output trace: `{0, 1, 2, 3, 5, 6, 7, 8}`.
    ///
    /// Consider delta equal to `{ 0 => 1, 6 => -1 }`?  The retraction of `6` affects
    /// the next three keys, so the set of affected keys is:
    /// `{0, 1, 2, 3, 6, 7, 8, 9}`.
    fn compute_retractions(
        &mut self,
        input_delta: &mut dyn ZCursor<I, DynUnit, ()>,
        output_trace: &mut dyn ZCursor<DynPair<I, O>, DynUnit, ()>,
        output_cb: &mut dyn FnMut(&mut DynPair<I, O>, &mut DynZWeight),
    ) {
        // println!("compute retractions");
        self.affected_keys.clear();
        self.encountered_keys.clear();

        let mut lag: usize = 0;

        skip_zeros(output_trace);

        while input_delta.key_valid() && output_trace.key_valid() {
            // - `input_delta` points to the _next_ delta key to process.
            // - `lag` is the number of remaining retractions to perform to capture the effect
            //   of the last processed delta.

            match (self.key_cmp)(output_trace.key().fst(), input_delta.key()) {
                Ordering::Less if lag > 0 => {
                    // We have applied the previous delta and still have some retractions to
                    // perform. Note that this will retract all values for this key in
                    // `output_trace`, regardless of the value of `lag`.  The reason is that
                    // the ordering of `(I, O)` pairs in the trace is determined by the ordering
                    // of type `O` and not the order in which the corresponding entry that `O`
                    // was projected from appeared in the input trace, so we don't have a way
                    // to retract precisely the affected values only.
                    let retractions = self.retract_key(output_trace, output_cb);
                    lag = lag.saturating_sub(retractions);
                }
                Ordering::Less => {
                    // We have processed the previous delta and all required retractions;
                    // seek to the next key to process.
                    let delta_key = input_delta.key();

                    output_trace.seek_key_with(&|key| {
                        (self.key_cmp)(key.fst(), delta_key) != Ordering::Less
                    });
                    skip_zeros(output_trace);
                }
                Ordering::Equal => {
                    // Delta modifies current key. Retract all existing values for this key
                    // from the output trace.
                    self.retract_key(output_trace, output_cb);
                    lag = self.lag;
                    input_delta.step_key();
                }
                Ordering::Greater => {
                    // Record new key.
                    self.record_affected_key(input_delta.key());
                    lag = self.lag;
                    input_delta.step_key();
                }
            }
        }

        // Finish processing remaining retractions.
        while output_trace.key_valid() && lag > 0 {
            let retractions = self.retract_key(output_trace, output_cb);
            lag = lag.saturating_sub(retractions);
        }

        // Record remaining keys in `input_delta`.
        while input_delta.key_valid() {
            self.record_affected_key(input_delta.key());
            input_delta.step_key();
        }

        // println!("affected_keys: {:?}", &self.affected_keys);
    }

    /// Backward pass: compute updated values for all keys in
    /// `self.retractions`.
    fn compute_updates<CB>(
        &mut self,
        input_cursor: &mut dyn ZCursor<I, DynUnit, ()>,
        mut output_cb: CB,
    ) where
        CB: FnMut(&mut DynPair<I, O>, &mut DynZWeight),
    {
        //println!("compute updates");

        input_cursor.fast_forward_keys();
        skip_non_positive_reverse(input_cursor);
        // println!(
        //     "current key after fast_forward: {:?}",
        //     input_cursor.get_key()
        // );

        // Index of the current key in the `affected_keys` array for which we are
        // computing update.  We traverse the array backward, starting from the
        // last element.
        let mut current = self.affected_keys.len() as isize - 1;

        // The first key in the `affected_keys` array that hasn't been observed by the
        // cursor yet.  Once the key has been observed, it is assigned a number equal
        // to its distance from the previous key or `self.lag` if the
        // previous key is more than `self.lag` steps behind.
        self.next_key = current;
        self.offset_from_prev = 0;
        if input_cursor.key_valid() {
            self.remaining_weight = **input_cursor.weight();
        }

        let mut new_val = self.output_val_factory.default_box();
        let mut output_pair = self.output_pair_factory.default_box();

        while current >= 0 {
            // println!(
            //     "compute_updates: computing update for affected key at index {current}, next_key: {}",
            //     self.next_key
            // );

            match self.encountered_keys[current as usize].offset {
                Some(Some(offset)) => {
                    // println!(
                    //     "key: {:?} was encountered at offset {offset} with weight {}",
                    //     &self.affected_keys.index(current as usize),
                    //     self.encountered_keys[current as usize].weight
                    // );

                    // The key has been observed by the cursor and is known to be
                    // exactly `offset` steps ahead of the previous key.
                    // Move the cursor `offset` steps to point to the delayed record
                    // in the trace.

                    // update offsets in keys we encounter along the way.
                    self.step_key_reverse_n(input_cursor, offset);

                    // Output insertion.
                    let weight = self.encountered_keys[current as usize].weight;
                    for _i in 0..weight {
                        (self.project)(input_cursor.get_key(), &mut new_val);
                        output_pair.from_refs(&self.affected_keys[current as usize], &new_val);
                        //println!("output: {:?}", &output_pair);
                        output_cb(output_pair.as_mut(), 1.erase_mut());
                        self.step_key_reverse_n(input_cursor, 1);
                    }
                    current -= 1;
                }
                Some(None) => {
                    // println!(
                    //     "key: {:?}, offset: Some(None)",
                    //     &self.affected_keys.index(current as usize)
                    // );
                    // Key does not occur in the input trace.
                    current -= 1;
                }
                None => {
                    // println!(
                    //     "key: {:?}, offset: None",
                    //     &self.affected_keys.index(current as usize)
                    // );
                    // Key is ahead of the current location of the cursor.
                    // Seek to the key.
                    input_cursor.seek_key_reverse(&self.affected_keys[current as usize]);

                    // We may have skipped over `current` and potentially multiple other keys.
                    // All skipped keys are not in the trace.
                    while !input_cursor.key_valid()
                        || (self.key_cmp)(input_cursor.key(), &self.affected_keys[current as usize])
                            == Ordering::Less
                    {
                        current -= 1;
                        if current < 0 {
                            break;
                        }
                    }

                    if current >= 0 && &self.affected_keys[current as usize] == input_cursor.key() {
                        // The cursor points to current key.  Move it `lag` steps to point to
                        // the matching delayed record.
                        self.encountered_keys[current as usize].offset = Some(Some(self.lag));
                        self.encountered_keys[current as usize].weight = **input_cursor.weight();
                        self.next_key = current - 1;
                        self.remaining_weight = **input_cursor.weight();
                        self.offset_from_prev = -self.remaining_weight;
                    } else {
                        // New current key is again ahead of the cursor; we'll seek to it at the
                        // next iteration.
                        self.next_key = current;
                    }
                }
            }
        }

        // We want to reuse the allocation across multiple calls, but cap it
        // at `MAX_RETRACTIONS_CAPACITY`.
        if self.encountered_keys.capacity() >= MAX_RETRACTIONS_CAPACITY {
            self.encountered_keys = Vec::new();
            self.affected_keys.clear();
            self.affected_keys.shrink_to_fit();
        }
    }
}

/// Per-key state created by the `lag` operator during forward pass.
///
/// * Records retractions to be applied to the output collection.
type AffectedKeys<K> = DynVec<K>;

/// Per-key state created by the `lag` operator during backward pass.
///
/// The cursor generally stays `lag` steps ahead of the current
/// key for which we are computing the value of the lag function.
/// As it moves forward (actually, backward, since this is the
/// backward pass), it encounters keys that will be evaluated next.
/// Since the cursor only moves in one direction, we won't get a
/// chance to visit those keys again, so we store all info we need
/// about the key in this struct, including
///
/// * The weight of each affected key.
/// * Offset from the previous encountered affected key. Determines how far to
///   move the cursor to reach the matching delayed record.
#[derive(Debug)]
struct EncounteredKey {
    /// The weight of the key in the input collection.
    weight: ZWeight,
    /// Offset from the previous key in the `affected_keys` array that occurs
    /// in the input trace.
    ///
    /// * `None` - key hasn't been encountered yet
    /// * `Some(None)` - key does not occur in the input trace.
    /// * `Some(Some(n))` - key is `n` steps away from the previous key from
    ///   `affected_keys` that is present in the input trace.  Specifically,
    ///   it is the sum of all positive weights between the previous key
    ///   (exclusive) and the current key (exclusive).
    offset: Option<Option<usize>>,
}

impl EncounteredKey {
    fn new() -> Self {
        Self {
            weight: HasZero::zero(),
            offset: None,
        }
    }
}

impl<I, O, KCF> GroupTransformer<I, DynPair<I, O>> for Lag<I, O, KCF>
where
    I: DataTrait + ?Sized,
    O: DataTrait + ?Sized,
    KCF: Fn(&I, &I) -> Ordering + 'static,
{
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn monotonicity(&self) -> Monotonicity {
        Monotonicity::Unordered
    }

    fn transform(
        &mut self,
        input_delta: &mut dyn ZCursor<I, DynUnit, ()>,
        input_trace: &mut dyn ZCursor<I, DynUnit, ()>,
        output_trace: &mut dyn ZCursor<DynPair<I, O>, DynUnit, ()>,
        output_cb: &mut dyn FnMut(&mut DynPair<I, O>, &mut DynZWeight),
    ) {
        // {
        //     println!("input_delta:");
        //     while input_delta.key_valid() {
        //         let w = **input_delta.weight();
        //         println!("    {:?} -> {:?}", input_delta.key(), w);
        //         input_delta.step_key();
        //     }
        //     input_delta.rewind_keys();
        // }
        // {
        //     println!("input_trace:");
        //     while input_trace.key_valid() {
        //         let w = **input_trace.weight();
        //         println!("    {:?} -> {:?}", input_trace.key(), w);
        //         input_trace.step_key();
        //     }
        //     input_trace.rewind_keys();
        // }
        // {
        //     println!("output_trace:");
        //     while output_trace.key_valid() {
        //         let w = **output_trace.weight();
        //         println!("    {:?} -> {:?}", output_trace.key(), w);
        //         output_trace.step_key();
        //     }
        //     output_trace.rewind_keys();
        // }

        if self.asc {
            self.compute_retractions(input_delta, output_trace, output_cb);
            self.compute_updates(&mut CursorPair::new(input_delta, input_trace), output_cb);
        } else {
            self.compute_retractions(
                &mut ReverseKeyCursor::new(input_delta),
                &mut ReverseKeyCursor::new(output_trace),
                output_cb,
            );
            self.compute_updates(
                &mut ReverseKeyCursor::new(&mut CursorPair::new(input_delta, input_trace)),
                output_cb,
            );
        }
    }
}

fn step_key_skip_zeros<I>(cursor: &mut dyn ZCursor<I, DynUnit, ()>)
where
    I: DataTrait + ?Sized,
{
    cursor.step_key();
    skip_zeros(cursor)
}

fn step_key_reverse_skip_non_positive<I>(cursor: &mut dyn ZCursor<I, DynUnit, ()>)
where
    I: DataTrait + ?Sized,
{
    cursor.step_key_reverse();
    skip_non_positive_reverse(cursor)
}

fn skip_zeros<I>(cursor: &mut dyn ZCursor<I, DynUnit, ()>)
where
    I: DataTrait + ?Sized,
{
    while cursor.key_valid() && cursor.weight().is_zero() {
        cursor.step_key();
    }
}

fn skip_non_positive_reverse<I>(cursor: &mut dyn ZCursor<I, DynUnit, ()>)
where
    I: DataTrait + ?Sized,
{
    while cursor.key_valid() && cursor.weight().le0() {
        cursor.step_key_reverse();
    }
}
