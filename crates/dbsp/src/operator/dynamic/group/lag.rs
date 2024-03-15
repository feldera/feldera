use super::{GroupTransformer, Monotonicity};
use crate::algebra::OrdIndexedZSetFactories;
use crate::operator::dynamic::filter_map::DynFilterMap;
use crate::{
    algebra::{HasZero, IndexedZSet, OrdIndexedZSet, ZCursor},
    dynamic::{DataTrait, DynPair, DynUnit, DynVec, Erase, Factory, LeanVec, WithFactory},
    trace::{
        cursor::{CursorPair, ReverseKeyCursor},
        BatchReader, BatchReaderFactories, Spillable,
    },
    utils::Tup2,
    DBData, DynZWeight, RootCircuit, Stream, ZWeight,
};
use std::{cmp::Ordering, marker::PhantomData, ops::Neg};

const MAX_RETRACTIONS_CAPACITY: usize = 100_000usize;

pub struct LagFactories<B: IndexedZSet + Spillable, OV: DataTrait + ?Sized> {
    input_factories: B::Factories,
    stored_factories: <B::Spilled as BatchReader>::Factories,
    output_factories: OrdIndexedZSetFactories<B::Key, DynPair<B::Val, OV>>,
    retraction_factory: &'static dyn Factory<ForwardRetraction<DynPair<B::Val, OV>>>,
    retractions_factory: &'static dyn Factory<ForwardRetractions<DynPair<B::Val, OV>>>,
    output_val_factory: &'static dyn Factory<OV>,
}

impl<B, OV> LagFactories<B, OV>
where
    B: IndexedZSet + Spillable,
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
            stored_factories: BatchReaderFactories::new::<KType, VType, ZWeight>(),
            output_factories: BatchReaderFactories::new::<KType, Tup2<VType, OVType>, ZWeight>(),
            retraction_factory: WithFactory::<Tup2<Tup2<VType, OVType>, ZWeight>>::FACTORY,
            retractions_factory:
                WithFactory::<LeanVec<Tup2<Tup2<VType, OVType>, ZWeight>>>::FACTORY,
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
    B: IndexedZSet + Spillable + Send,
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
            &factories.stored_factories,
            &factories.output_factories,
            Box::new(Lag::new(
                factories.output_factories.val_factory(),
                factories.retraction_factory,
                factories.retractions_factory,
                factories.output_val_factory,
                offset.unsigned_abs(),
                offset > 0,
                project,
                if offset > 0 {
                    |k1: &B::Val, k2: &B::Val| k1.cmp(k2)
                } else {
                    |k1: &B::Val, k2: &B::Val| k2.cmp(k1)
                },
                if offset > 0 {
                    |v1: &OV, v2: &OV| v1.cmp(v2)
                } else {
                    |v1: &OV, v2: &OV| v2.cmp(v1)
                },
            )),
        )
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
struct Lag<I: DataTrait + ?Sized, O: DataTrait + ?Sized, KCF, VCF> {
    name: String,
    lag: usize,
    /// `true` for `lag`, `false` for `lead`.
    asc: bool,
    project: Box<dyn Fn(Option<&I>, &mut O)>,
    output_pair_factory: &'static dyn Factory<DynPair<I, O>>,
    output_val_factory: &'static dyn Factory<O>,
    retraction_key: Box<DynPair<I, O>>,
    retraction: Box<ForwardRetraction<DynPair<I, O>>>,
    /// Array of retractions reused across multiple
    /// invocations of the operator.
    forward_retractions: Box<ForwardRetractions<DynPair<I, O>>>,
    backward_retractions: Vec<BackwardRetraction>,
    /// Index of the next key from `retractions` we expect to
    /// encounter.
    next_key: isize,
    /// Number of steps the input cursor took after encountering
    /// the previous key from `retractions`.
    offset_from_prev: usize,
    /// Key comparison function.  Set to `cmp` for ascending
    /// order and the reverse of `cmp` for descending order.
    key_cmp: KCF,
    /// Value comparison function.
    val_cmp: VCF,
    _phantom: PhantomData<fn(&I, &O)>,
}

impl<I, O, KCF, VCF> Lag<I, O, KCF, VCF>
where
    I: DataTrait + ?Sized,
    O: DataTrait + ?Sized,
    KCF: Fn(&I, &I) -> Ordering + 'static,
    VCF: Fn(&O, &O) -> Ordering + 'static,
{
    #[allow(clippy::too_many_arguments)]
    fn new(
        output_pair_factory: &'static dyn Factory<DynPair<I, O>>,
        retraction_factory: &'static dyn Factory<ForwardRetraction<DynPair<I, O>>>,
        retractions_factory: &'static dyn Factory<ForwardRetractions<DynPair<I, O>>>,
        output_val_factory: &'static dyn Factory<O>,
        lag: usize,
        asc: bool,
        project: Box<dyn Fn(Option<&I>, &mut O)>,
        key_cmp: KCF,
        val_cmp: VCF,
    ) -> Self {
        Self {
            name: format!("{}({lag})", if asc { "lag" } else { "lead" }),
            retraction_key: output_pair_factory.default_box(),
            retraction: retraction_factory.default_box(),
            output_pair_factory,
            output_val_factory,
            lag,
            asc,
            project,
            forward_retractions: retractions_factory.default_box(),
            backward_retractions: Vec::new(),
            next_key: 0,
            offset_from_prev: 0,
            key_cmp,
            val_cmp,
            _phantom: PhantomData,
        }
    }

    fn push_retraction_from_key(&mut self, key: &I, mut w: ZWeight) {
        let (i, o) = self.retraction_key.split_mut();
        key.clone_to(i);
        (self.project)(None, o);

        self.retraction
            .from_vals(self.retraction_key.as_mut(), w.erase_mut());
        self.forward_retractions.push_val(&mut *self.retraction);

        self.backward_retractions.push(BackwardRetraction::new());
    }

    fn push_retraction_from_ref(&mut self, key: &DynPair<I, O>, w: ZWeight) {
        self.retraction.from_refs(key, w.erase());
        self.forward_retractions.push_val(&mut *self.retraction);

        self.backward_retractions.push(BackwardRetraction::new());
    }

    /// Move input `cursor` `n` steps back.  Keep track of keys in the
    /// `self.retractions` array encountered along the way, update their
    /// `offset` and `new_weight` fields.
    fn step_key_reverse_n(&mut self, cursor: &mut dyn ZCursor<I, DynUnit, ()>, mut steps: usize) {
        while steps > 0 && cursor.key_valid() {
            step_key_reverse_skip_zeros(cursor);

            while self.next_key >= 0
                && (!cursor.key_valid()
                    || (self.key_cmp)(
                        cursor.key(),
                        self.forward_retractions[self.next_key as usize].key().fst(),
                    ) == Ordering::Less)
            {
                self.backward_retractions[self.next_key as usize].offset = Some(None);
                self.next_key -= 1;
            }

            steps -= 1;
            self.offset_from_prev += 1;

            if cursor.key_valid()
                && self.next_key >= 0
                && cursor.key() == self.forward_retractions[self.next_key as usize].key().fst()
            {
                // println!("reverse_n: found {:?}, offset {offset_from_prev}", cursor.key());
                self.backward_retractions[self.next_key as usize].offset =
                    Some(Some(self.offset_from_prev));
                self.offset_from_prev = 0;
                self.backward_retractions[self.next_key as usize].new_weight = **cursor.weight();
                self.next_key -= 1;
            }
        }
    }

    /// Forward pass: compute keys that require updates,
    /// record these keys in `self.retractions`.
    ///
    /// # Example
    ///
    /// Consider `lag(3)` operator and assume that the input collection
    /// contains values `{1, 2, 3, 4, 6, 7, 8, 9, 10}` and the delta contains
    /// keys `{0, 5, 6}`.  The set of affected keys includes all keys in delta
    /// and for each key in delta `3` following keys in the output trace:
    /// `{0, 1, 2, 3, 5, 6, 7, 8, 9}`.
    fn compute_retractions(
        &mut self,
        input_delta: &mut dyn ZCursor<I, DynUnit, ()>,
        output_trace: &mut dyn ZCursor<DynPair<I, O>, DynUnit, ()>,
    ) {
        self.forward_retractions.clear();
        self.backward_retractions.clear();

        let mut offset = self.lag + 1;

        skip_zeros(output_trace);

        while input_delta.key_valid() && output_trace.key_valid() {
            // - `input_delta` points to the _next_ delta key to process.
            // - `offset` is the number of steps taken from the current key being processed.

            match (self.key_cmp)(output_trace.key().fst(), input_delta.key()) {
                Ordering::Less => {
                    if offset <= self.lag {
                        // We are processing the previous key and haven't taken `lag`
                        // steps yet.
                        let w = output_trace.weight().neg();
                        self.push_retraction_from_ref(output_trace.key(), w);

                        step_key_skip_zeros(output_trace);
                        offset += 1;
                    } else {
                        // Done processing previous key. Seek to the next key to process.
                        let delta_key = input_delta.key();
                        output_trace.seek_key_with(&|key| {
                            (self.key_cmp)(key.fst(), delta_key) != Ordering::Less
                        });
                        skip_zeros(output_trace);
                    }
                }
                Ordering::Equal => {
                    // Reached the next key -- reset offset to 0 and move `input_delta` to the next
                    // key.
                    input_delta.step_key();
                    offset = 0;
                }
                Ordering::Greater => {
                    // Output cursor overshot next key.  This is only possible if the key is not
                    // present in the output trace.  Record the key as a zero-weight retraction,
                    // so we process it during reverse pass.
                    self.push_retraction_from_key(input_delta.key(), HasZero::zero());
                    input_delta.step_key();
                    offset = 1;
                }
            }
        }

        // println!("retractions before suffix: {:?}", retractions);

        // Finish processing the last key.
        while output_trace.key_valid() && offset <= self.lag {
            let w = output_trace.weight().neg();
            self.push_retraction_from_ref(output_trace.key(), w);
            step_key_skip_zeros(output_trace);
            offset += 1;
        }

        // println!("retractions after output_trace: {:?}", retractions);

        // Record remaining keys in `input_delta`.
        while input_delta.key_valid() {
            self.push_retraction_from_key(input_delta.key(), HasZero::zero());
            input_delta.step_key();
        }

        // println!("retractions: {:?}", self.retractions);
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
        input_cursor.fast_forward_keys();
        // println!("current key after fast_forward: {:?}", input_cursor.key());

        // Index of the current key in the retractions array for which we are
        // computing update.  We traverse the array backward, hence the index
        // is relative to the end of the array.
        let mut current = self.forward_retractions.len() as isize - 1;

        // The first key in the `retractions` array that hasn't been observed by the
        // cursor yet.  Once the key has been observed, it is assigned a number equal
        // to its distance from the previous key in `retractions` or `self.lag` if the
        // previous key is more than `self.lag` steps behind.
        self.next_key = current;
        self.offset_from_prev = 0;

        let mut new_val = self.output_val_factory.default_box();
        let mut output_pair = self.output_pair_factory.default_box();

        while current >= 0 {
            // println!("current: {current}");

            match self.backward_retractions[current as usize].offset {
                Some(Some(offset)) => {
                    // println!("offset: {offset}");

                    // The key has been observed by the cursor and is known to be
                    // exactly `offset` steps ahead of the previous key.
                    // Move the cursor `offset` steps to point to the delayed record
                    // in the trace.

                    // update offsets in keys we encounter along the way.
                    self.step_key_reverse_n(input_cursor, offset);

                    // Output retraction and insertion in the correct order.

                    (self.project)(input_cursor.get_key(), &mut new_val);
                    let (key_old_val, old_weight) =
                        self.forward_retractions[current as usize].split_mut();
                    let (key, old_val): (&mut I, &mut O) = key_old_val.split_mut();
                    let mut new_weight = self.backward_retractions[current as usize].new_weight;

                    if old_weight.is_zero() {
                        if !new_weight.is_zero() {
                            output_pair.from_vals(key, &mut new_val);
                            output_cb(output_pair.as_mut(), new_weight.erase_mut());
                        }
                    } else if new_weight.is_zero() {
                        output_pair.from_vals(key, old_val);
                        output_cb(output_pair.as_mut(), old_weight);
                    } else {
                        match (self.val_cmp)(old_val, &new_val) {
                            Ordering::Greater => {
                                key.clone_to(output_pair.fst_mut());
                                old_val.move_to(output_pair.snd_mut());
                                output_cb(output_pair.as_mut(), old_weight);

                                output_pair.from_vals(key, &mut new_val);
                                output_cb(output_pair.as_mut(), new_weight.erase_mut());
                            }
                            Ordering::Equal => {
                                let mut weight = new_weight + **old_weight;
                                if !weight.is_zero() {
                                    output_cb(key_old_val, weight.erase_mut());
                                }
                            }
                            Ordering::Less => {
                                key.clone_to(output_pair.fst_mut());
                                new_val.move_to(output_pair.snd_mut());
                                output_cb(output_pair.as_mut(), new_weight.erase_mut());

                                output_cb(key_old_val, old_weight);
                            }
                        }
                    }

                    current -= 1;
                }
                Some(None) => {
                    // println!("offset: Some(None)");
                    // Key does not occur in the input trace.  Output retraction only.
                    let (key_old_val, old_weight) =
                        self.forward_retractions[current as usize].split_mut();
                    if !old_weight.is_zero() {
                        output_cb(key_old_val, old_weight);
                    }
                    current -= 1;
                }
                None => {
                    // println!("offset: None");
                    // Key is ahead of the current location of the cursor.
                    // Seek to the key.
                    input_cursor
                        .seek_key_reverse(self.forward_retractions[current as usize].key().fst());
                    self.offset_from_prev = 0;

                    // We may have skipped over `current` and potentially multiple other keys.
                    // All skipped keys are not in the trace.  Output retractions for them and
                    // move `current` to the first key that has not been skipped.
                    while !input_cursor.key_valid()
                        || (self.key_cmp)(
                            input_cursor.key(),
                            self.forward_retractions[current as usize].key().fst(),
                        ) == Ordering::Less
                    {
                        // println!("retract {:?} (key: {:?})", &self.retractions[current as
                        // usize].key().0, input_cursor.key());
                        let (old_key_val, old_weight) =
                            self.forward_retractions[current as usize].split_mut();
                        if !old_weight.is_zero() {
                            output_cb(old_key_val, old_weight);
                        }
                        current -= 1;
                        if current < 0 {
                            break;
                        }
                    }

                    if current >= 0
                        && self.forward_retractions[current as usize].key().fst()
                            == input_cursor.key()
                    {
                        // The cursor points to current key.  Move it `lag` steps to point to
                        // the matching delayed record.
                        self.backward_retractions[current as usize].offset = Some(Some(self.lag));
                        self.backward_retractions[current as usize].new_weight =
                            **input_cursor.weight();
                        self.next_key = current - 1;
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
        if self.backward_retractions.capacity() >= MAX_RETRACTIONS_CAPACITY {
            self.backward_retractions = Vec::new();
            self.forward_retractions.clear();
            self.forward_retractions.shrink_to_fit();
        }
    }
}

/// Per-key state created by the `lag` operator during forward pass.
///
/// * Records retractions to be applied to the output collection.
type ForwardRetraction<K> = DynPair<K, DynZWeight>;

impl<K: DataTrait + ?Sized> ForwardRetraction<K> {
    fn key(&self) -> &K {
        self.fst()
    }
}

type ForwardRetractions<K> = DynVec<ForwardRetraction<K>>;

/// Per-key state created by the `lag` operator during backward pass:
/// * The new weight of each affected key.
/// * Offset from the previous encountered affected key. Determines how far to
///   move the cursor to reach the matching delayed record.
#[derive(Debug)]
struct BackwardRetraction {
    /// The new weight of the key in the input collection.  Populated during
    /// backward pass.
    new_weight: ZWeight,
    /// Offset from the previous key in the retractions array.  Populated during
    /// backward pass.
    ///
    /// * `None` - key hasn't been encountered yet
    /// * `Some(None)` - key does not occur in the input trace.
    /// * `Some(Some(n))` - key is `n` steps away from the previous key from
    ///   `retractions` that is present in the input trace.
    offset: Option<Option<usize>>,
}

impl BackwardRetraction {
    fn new() -> Self {
        Self {
            new_weight: HasZero::zero(),
            offset: None,
        }
    }
}

impl<I, O, KCF, VCF> GroupTransformer<I, DynPair<I, O>> for Lag<I, O, KCF, VCF>
where
    I: DataTrait + ?Sized,
    O: DataTrait + ?Sized,
    KCF: Fn(&I, &I) -> Ordering + 'static,
    VCF: Fn(&O, &O) -> Ordering + 'static,
{
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn monotonicity(&self) -> Monotonicity {
        // Since outputs are produced during the second (backward) pass,
        // `lag` produces outputs in the descending order, while `lead` -- in
        // ascending.
        if self.asc {
            Monotonicity::Descending
        } else {
            Monotonicity::Ascending
        }
    }

    fn transform(
        &mut self,
        input_delta: &mut dyn ZCursor<I, DynUnit, ()>,
        input_trace: &mut dyn ZCursor<I, DynUnit, ()>,
        output_trace: &mut dyn ZCursor<DynPair<I, O>, DynUnit, ()>,
        output_cb: &mut dyn FnMut(&mut DynPair<I, O>, &mut DynZWeight),
    ) {
        /*
        {
            println!("input_delta:");
            while input_delta.key_valid() {
                let w = input_delta.weight();
                println!("    {:?} -> {:?}", input_delta.key(), w);
                input_delta.step_key();
            }
            input_delta.rewind_keys();
        }
        {
            println!("input_trace:");
            while input_trace.key_valid() {
                let w = input_trace.weight();
                println!("    {:?} -> {:?}", input_trace.key(), w);
                input_trace.step_key();
            }
            input_trace.rewind_keys();
        }
        {
            println!("output_trace:");
            while output_trace.key_valid() {
                let w = output_trace.weight();
                println!("    {:?} -> {:?}", output_trace.key(), w);
                output_trace.step_key();
            }
            output_trace.rewind_keys();
        }
        */

        if self.asc {
            self.compute_retractions(input_delta, output_trace);
            self.compute_updates(&mut CursorPair::new(input_delta, input_trace), output_cb);
        } else {
            self.compute_retractions(
                &mut ReverseKeyCursor::new(input_delta),
                &mut ReverseKeyCursor::new(output_trace),
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

fn step_key_reverse_skip_zeros<I>(cursor: &mut dyn ZCursor<I, DynUnit, ()>)
where
    I: DataTrait + ?Sized,
{
    cursor.step_key_reverse();
    skip_zeros_reverse(cursor)
}

fn skip_zeros<I>(cursor: &mut dyn ZCursor<I, DynUnit, ()>)
where
    I: DataTrait + ?Sized,
{
    while cursor.key_valid() && cursor.weight().is_zero() {
        cursor.step_key();
    }
}

fn skip_zeros_reverse<I>(cursor: &mut dyn ZCursor<I, DynUnit, ()>)
where
    I: DataTrait + ?Sized,
{
    while cursor.key_valid() && cursor.weight().is_zero() {
        cursor.step_key_reverse();
    }
}
