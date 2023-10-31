use super::{
    custom_ord::{CmpFunc, WithCustomOrd},
    DiffGroupTransformer, Monotonicity, NonIncrementalGroupTransformer,
};
use crate::{
    algebra::ZRingValue, operator::FilterMap, trace::Cursor, DBData, DBWeight, IndexedZSet,
    OrdIndexedZSet, RootCircuit, Stream,
};
use std::marker::PhantomData;

impl<B> Stream<RootCircuit, B>
where
    B: IndexedZSet + Send,
{
    /// Pick `k` smallest values in each group.
    ///
    /// For each key in the input stream, removes all but `k` smallest values.
    #[allow(clippy::type_complexity)]
    pub fn topk_asc(&self, k: usize) -> Stream<RootCircuit, OrdIndexedZSet<B::Key, B::Val, B::R>>
    where
        B::R: ZRingValue,
    {
        self.group_transform(DiffGroupTransformer::new(TopK::asc(k)))
    }

    /// Pick `k` largest values in each group.
    ///
    /// For each key in the input stream, removes all but `k` largest values.
    #[allow(clippy::type_complexity)]
    pub fn topk_desc(&self, k: usize) -> Stream<RootCircuit, OrdIndexedZSet<B::Key, B::Val, B::R>>
    where
        B::R: ZRingValue,
    {
        self.group_transform(DiffGroupTransformer::new(TopK::desc(k)))
    }
}

impl<K, V, R> Stream<RootCircuit, OrdIndexedZSet<K, V, R>>
where
    K: DBData,
    V: DBData,
    R: DBWeight + ZRingValue,
{
    /// Pick `k` smallest values in each group based on a custom comparison
    /// function.
    ///
    /// This method is similar to [`topk_asc`](`Stream::topk_asc`), but instead
    /// of ordering elements according to `trait Ord for V`, it uses a
    /// user-defined comparison function `F`.
    ///
    /// ## Correctness
    ///
    /// * `CF` must establish a _total_ order over `V`, consistent with `impl Eq
    ///   for V`, i.e., `CF::cmp(v1, v2) == Equal <=> v1.eq(v2)`.
    pub fn topk_custom_order<F>(&self, k: usize) -> Self
    where
        F: CmpFunc<V>,
    {
        self.map_index(|(k, v)| (k.clone(), <WithCustomOrd<V, F>>::new(v.clone())))
            .group_transform(DiffGroupTransformer::new(TopK::asc(k)))
            .map_index(|(k, v)| (k.clone(), v.val.clone()))
    }

    /// Rank elements in the group and output all elements with `rank <= k`.
    ///
    /// This operator implements the behavior of the following SQL pattern:
    /// ```text
    /// SELECT
    ///     ...,
    ///     RANK() OVER (PARTITION BY .. ORDER BY ...) AS rank
    /// FROM table
    /// WHERE rank <= K
    /// ```
    ///
    /// The `CF` type and the `rank_eq_func` function together establish the
    /// ranking of values in the group:
    /// * `CF` establishes a _total_ ordering of elements such that `v1 < v2 =>
    ///   rank(v1) <= rank(v2)`.
    /// * `rank_eq_func` checks that two elements have equal rank, i.e., have
    ///   equal values in all the ORDER BY columns in the SQL query above:
    ///   `rank_eq_func(v1, v2) <=> rank(v1) == rank(v2)`.
    ///
    /// The `output_func` closure takes a value and its rank and produces an
    /// output value.
    ///
    /// ## Correctness
    ///
    /// * `CF` must establish a total order over `V`, consistent with `impl Eq
    ///   for V`, i.e., `CF::cmp(v1, v2) == Equal <=> v1.eq(v2)`.
    /// * `CF` must be consistent with `rank_eq_func`, i.e., `CF::cmp(v1, v2) ==
    ///   Equal => rank_eq_func(v1, v2)`.
    pub fn topk_rank_custom_order<CF, EF, OF, OV>(
        &self,
        k: usize,
        rank_eq_func: EF,
        output_func: OF,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV, R>>
    where
        OV: DBData,
        CF: CmpFunc<V>,
        EF: Fn(&V, &V) -> bool + 'static,
        OF: Fn(i64, &V) -> OV + 'static,
    {
        self.map_index(|(k, v)| (k.clone(), <WithCustomOrd<V, CF>>::new(v.clone())))
            .group_transform(DiffGroupTransformer::new(TopKRank::sparse(
                k,
                move |v1: &WithCustomOrd<V, CF>, v2: &WithCustomOrd<V, CF>| {
                    rank_eq_func(&v1.val, &v2.val)
                },
                move |rank, v: &WithCustomOrd<V, _>| output_func(rank, &v.val),
            )))
    }

    /// Rank elements in the group using dense ranking and output all elements
    /// with `rank <= k`.
    ///
    /// Similar to [`topk_rank_custom_order`](`Self::topk_rank_custom_order`),
    /// but computes a dense ranking of elements in the group.
    pub fn topk_dense_rank_custom_order<CF, EF, OF, OV>(
        &self,
        k: usize,
        rank_eq_func: EF,
        output_func: OF,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV, R>>
    where
        OV: DBData,
        CF: CmpFunc<V>,
        EF: Fn(&V, &V) -> bool + 'static,
        OF: Fn(i64, &V) -> OV + 'static,
    {
        self.map_index(|(k, v)| (k.clone(), <WithCustomOrd<V, CF>>::new(v.clone())))
            .group_transform(DiffGroupTransformer::new(TopKRank::dense(
                k,
                move |v1: &WithCustomOrd<V, CF>, v2: &WithCustomOrd<V, CF>| {
                    rank_eq_func(&v1.val, &v2.val)
                },
                move |rank, v: &WithCustomOrd<V, _>| output_func(rank, &v.val),
            )))
    }

    /// Pick `k` smallest values in each group based on a custom comparison
    /// function.  Return the `k` elements along with their row numbers.
    ///
    /// This operator implements the behavior of the following SQL pattern:
    ///
    /// ```text
    /// SELECT
    ///     ...,
    ///     ROW_NUMBER() OVER (PARTITION BY .. ORDER BY ...) AS row_number
    /// FROM table
    /// WHERE row_number <= K
    /// ```
    ///
    /// ## Correctness
    ///
    /// * `CF` must establish a _total_ order over `V`, consistent with `impl Eq
    ///   for V`, i.e., `CF::cmp(v1, v2) == Equal <=> v1.eq(v2)`.
    pub fn topk_row_number_custom_order<CF, OF, OV>(
        &self,
        k: usize,
        output_func: OF,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV, R>>
    where
        OV: DBData,
        CF: CmpFunc<V>,
        OF: Fn(i64, &V) -> OV + 'static,
    {
        self.map_index(|(k, v)| (k.clone(), <WithCustomOrd<V, CF>>::new(v.clone())))
            .group_transform(DiffGroupTransformer::new(TopKRowNumber::new(
                k,
                move |rank, v: &WithCustomOrd<V, _>| output_func(rank, &v.val),
            )))
    }
}

struct TopK<I, R, const ASCENDING: bool> {
    k: usize,
    name: String,
    // asc: bool,
    _phantom: PhantomData<(I, R)>,
}

impl<I, R> TopK<I, R, true> {
    fn asc(k: usize) -> Self {
        Self {
            k,
            name: format!("top-{k}-asc"),
            _phantom: PhantomData,
        }
    }
}

impl<I, R> TopK<I, R, false> {
    fn desc(k: usize) -> Self {
        Self {
            k,
            name: format!("top-{k}-desc"),
            _phantom: PhantomData,
        }
    }
}

impl<I, R, const ASCENDING: bool> NonIncrementalGroupTransformer<I, I, R> for TopK<I, R, ASCENDING>
where
    I: DBData,
    R: DBWeight,
{
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn monotonicity(&self) -> Monotonicity {
        if ASCENDING {
            Monotonicity::Ascending
        } else {
            Monotonicity::Descending
        }
    }

    fn transform<C, CB>(&mut self, cursor: &mut C, mut output_cb: CB)
    where
        C: Cursor<I, (), (), R>,
        CB: FnMut(I, R),
    {
        let mut count = 0usize;

        if ASCENDING {
            while cursor.key_valid() && count < self.k {
                let w = cursor.weight();
                if !w.is_zero() {
                    output_cb(cursor.key().clone(), w);
                    count += 1;
                }
                cursor.step_key();
            }
        } else {
            cursor.fast_forward_keys();

            while cursor.key_valid() && count < self.k {
                let w = cursor.weight();
                if !w.is_zero() {
                    output_cb(cursor.key().clone(), w);
                    count += 1;
                }
                cursor.step_key_reverse();
            }
        }
    }
}

struct TopKRank<I, R, EF, OF> {
    k: usize,
    dense: bool,
    name: String,
    rank_eq_func: EF,
    output_func: OF,
    _phantom: PhantomData<(I, R)>,
}

impl<I, R, EF, OF> TopKRank<I, R, EF, OF> {
    fn sparse(k: usize, rank_eq_func: EF, output_func: OF) -> Self {
        Self {
            k,
            dense: false,
            name: format!("top-{k}-rank"),
            rank_eq_func,
            output_func,
            _phantom: PhantomData,
        }
    }

    fn dense(k: usize, rank_eq_func: EF, output_func: OF) -> Self {
        Self {
            k,
            dense: true,
            name: format!("top-{k}-dense-rank"),
            rank_eq_func,
            output_func,
            _phantom: PhantomData,
        }
    }
}

impl<I, R, EF, OF, OV> NonIncrementalGroupTransformer<I, OV, R> for TopKRank<I, R, EF, OF>
where
    I: DBData,
    OV: DBData,
    EF: Fn(&I, &I) -> bool + 'static,
    OF: Fn(i64, &I) -> OV + 'static,
    R: DBWeight,
{
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn monotonicity(&self) -> Monotonicity {
        // We don't assume that `OF` preserves ordering.
        Monotonicity::Unordered
    }

    fn transform<C, CB>(&mut self, cursor: &mut C, mut output_cb: CB)
    where
        C: Cursor<I, (), (), R>,
        CB: FnMut(OV, R),
    {
        let mut count = 0usize;

        let mut rank = 1;
        let mut prev_key = None;
        while cursor.key_valid() {
            let w = cursor.weight();
            if !w.is_zero() {
                count += 1;
                let key = cursor.key();
                if let Some(prev_key) = &prev_key {
                    if !(self.rank_eq_func)(key, prev_key) {
                        // Rank stays the same while iterating over equal-ranked elements,
                        // and then increases by one when computing dense ranking or skips
                        // to `count` otherwise.
                        if self.dense {
                            rank += 1;
                        } else {
                            rank = count as i64;
                        }
                        if rank as usize > self.k {
                            break;
                        }
                    }
                };

                output_cb((self.output_func)(rank, key), w);
                prev_key = Some(key.clone());
            }
            cursor.step_key();
        }
    }
}

struct TopKRowNumber<I, R, OF> {
    k: usize,
    name: String,
    output_func: OF,
    _phantom: PhantomData<(I, R)>,
}

impl<I, R, OF> TopKRowNumber<I, R, OF> {
    fn new(k: usize, output_func: OF) -> Self {
        Self {
            k,
            name: format!("top-{k}-row_number"),
            output_func,
            _phantom: PhantomData,
        }
    }
}

impl<I, R, OF, OV> NonIncrementalGroupTransformer<I, OV, R> for TopKRowNumber<I, R, OF>
where
    I: DBData,
    OV: DBData,
    OF: Fn(i64, &I) -> OV + 'static,
    R: DBWeight,
{
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn monotonicity(&self) -> Monotonicity {
        // We don't assume that `OF` preserves ordering.
        Monotonicity::Unordered
    }

    fn transform<C, CB>(&mut self, cursor: &mut C, mut output_cb: CB)
    where
        C: Cursor<I, (), (), R>,
        CB: FnMut(OV, R),
    {
        let mut count = 0usize;

        while cursor.key_valid() && count < self.k {
            let w = cursor.weight();
            if !w.is_zero() {
                count += 1;
                output_cb((self.output_func)(count as i64, cursor.key()), w);
            }
            cursor.step_key();
        }
    }
}
