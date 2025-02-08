use crate::{
    dynamic::{DowncastTrait, DynData, DynUnit, Erase},
    DBData, OrdIndexedZSet, OrdZSet, RootCircuit, Stream,
};

use super::dynamic::asof_join::AsofJoinFactories;

impl<K1, V1> Stream<RootCircuit, OrdIndexedZSet<K1, V1>>
where
    K1: DBData,
    V1: DBData,
{
    /// Asof-join operator.
    ///
    /// An asof-join operator combines records from two tables based on a common key
    /// (similar to an equi-join), as well as a timestamp.  It assumes that both tables
    /// contain a timestamp column (`ts`).  It matches each value `v` in `self` with
    /// the value in `other` that has the same key and the largest timestamp not
    /// exceeding `v.ts`.  If there are multiple values with the same timestamp, the
    /// operator picks the largest one based on the ordering (according to `Ord`) on
    /// type `V2`.  If there is no value `v2`, such that `v2.ts <= v.ts` in `other`,
    /// then the value `None` is used, i.e., this operator behaves as a left join.
    ///
    /// The operator assumes that values in both collections are sorted by timestamp,
    /// i.e., `impl Ord for V1` must satisfy `ts_func1(v) < ts_func1(u) ==> v < u`.
    /// Similarly for `V2`: `ts_func2(v) < ts_func2(u) ==> v < u`.
    ///
    /// # Arguments
    ///
    /// * `self` - the left-hand side of the join.
    /// * `other` - the right-hand side of the join.
    /// * `join` - join function that maps a key, a value from `self`, and an optional
    ///   value from `other` to an output value.
    /// * `ts_func1` - extracts the value of the timestamp column from a record in `self`.
    /// * `ts_func2` - extracts the value of the timestamp column from a record in `other`.
    #[track_caller]
    pub fn asof_join<TS, F, TSF1, TSF2, V2, V>(
        &self,
        other: &Stream<RootCircuit, OrdIndexedZSet<K1, V2>>,
        join: F,
        ts_func1: TSF1,
        ts_func2: TSF2,
    ) -> Stream<RootCircuit, OrdZSet<V>>
    where
        TS: DBData,
        V2: DBData,
        V: DBData,
        F: Fn(&K1, &V1, Option<&V2>) -> V + Clone + 'static,
        TSF1: Fn(&V1) -> TS + Clone + 'static,
        TSF2: Fn(&V2) -> TS + 'static,
    {
        let join_factories = AsofJoinFactories::new::<TS, K1, V1, V2, V, ()>();

        let ts_func1_clone = ts_func1.clone();

        let dyn_ts_func1 = Box::new(move |v: &DynData, ts: &mut DynData| unsafe {
            *ts.downcast_mut() = ts_func1_clone(v.downcast())
        });

        let ts_func1_clone = ts_func1.clone();

        let tscmp_func = Box::new(move |v1: &DynData, v2: &DynData| unsafe {
            ts_func1_clone(v1.downcast()).cmp(&ts_func2(v2.downcast()))
        });
        let valts_cmp_func = Box::new(move |v1: &DynData, ts: &DynData| unsafe {
            // println!("cmp {v1:?}, {ts:?}");
            ts_func1(v1.downcast()).cmp(ts.downcast())
        });
        let join_func = Box::new(
            move |key: &DynData,
                  v1: &DynData,
                  v2: Option<&DynData>,
                  cb: &mut dyn FnMut(&mut DynData, &mut DynUnit)| unsafe {
                let mut v = join(key.downcast(), v1.downcast(), v2.map(|v| v.downcast()));
                cb(v.erase_mut(), ().erase_mut());
            },
        );

        self.inner()
            .dyn_asof_join(
                &join_factories,
                &other.inner(),
                dyn_ts_func1,
                tscmp_func,
                valts_cmp_func,
                join_func,
            )
            .typed()
    }
}
