//! Operators to organize time series data into windows.

use crate::{
    algebra::{IndexedZSet, NegByRef, ZBatchReader},
    circuit::{
        operator_traits::{Operator, TernaryOperator},
        Circuit, GlobalNodeId, OwnershipPreference, Scope, Stream,
    },
    dynamic::{
        rkyv::{DeserializableDyn, SerializeDyn},
        ClonableTrait, DataTrait, DynData, WeightTrait,
    },
    operator::{
        dynamic::{trace::TraceBound, MonoIndexedZSet},
        require_persistent_id,
    },
    storage::file::{to_bytes, with_serializer},
    trace::{BatchFactories, BatchReader, BatchReaderFactories, Cursor, SpineSnapshot},
    Error, RootCircuit, Runtime,
};
use feldera_storage::StoragePath;
use minitrace::trace;
use rkyv::Deserialize;
use std::{borrow::Cow, marker::PhantomData};

impl Stream<RootCircuit, MonoIndexedZSet> {
    pub fn dyn_window_mono(
        &self,
        factories: &<MonoIndexedZSet as BatchReader>::Factories,
        inclusive: (bool, bool),
        bounds: &Stream<RootCircuit, (Box<DynData>, Box<DynData>)>,
    ) -> Stream<RootCircuit, MonoIndexedZSet> {
        self.dyn_window(factories, inclusive, bounds)
    }
}

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: IndexedZSet,
    Box<B::Key>: Clone,
{
    /// See [`Stream::window`].
    pub fn dyn_window(
        &self,
        factories: &B::Factories,
        inclusive: (bool, bool),
        bounds: &Stream<C, (Box<B::Key>, Box<B::Key>)>,
    ) -> Stream<C, B> {
        let bound = TraceBound::new();
        let bound_clone = bound.clone();
        bounds.apply(move |(lower, _upper)| {
            bound_clone.set(lower.clone());
        });
        let trace = self
            .dyn_integrate_trace_with_bound(factories, bound, TraceBound::new())
            .delay_trace();
        self.circuit().add_ternary_operator(
            <Window<B, SpineSnapshot<B>>>::new(factories, inclusive),
            &trace,
            self,
            bounds,
        )
    }
}

/// A window that is serialized to a file.
#[derive(rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
struct CommittedWindow {
    window: Option<(Vec<u8>, Vec<u8>)>,
}

impl<B: IndexedZSet, T: ZBatchReader> From<&Window<B, T>> for CommittedWindow {
    fn from(value: &Window<B, T>) -> Self {
        // Transform the window bounds into a serialized form and store it as a byte vector.
        // This is necessary because the key type is not sized.
        let window = value.window.as_ref().map(|(a, b)| {
            let sa = with_serializer(Default::default(), |s| a.serialize(s).unwrap()).0;
            let sb = with_serializer(Default::default(), |s| b.serialize(s).unwrap()).0;
            (sa.into_vec(), sb.into_vec())
        });

        CommittedWindow { window }
    }
}

struct Window<B, T>
where
    B: IndexedZSet,
    T: ZBatchReader,
{
    // For error reporting.
    global_id: GlobalNodeId,
    factories: B::Factories,
    left_inclusive: bool,
    right_inclusive: bool,
    // `None` means we're at the start of a clock epoch, no inputs
    // have been received yet, and window boundaries haven't been set.
    window: Option<(Box<B::Key>, Box<B::Key>)>,
    _phantom: PhantomData<(B, T)>,
}

impl<B, T> Window<B, T>
where
    B: IndexedZSet,
    T: ZBatchReader,
{
    pub fn new(factories: &B::Factories, (left_inclusive, right_inclusive): (bool, bool)) -> Self {
        Self {
            global_id: GlobalNodeId::root(),
            factories: factories.clone(),
            left_inclusive,
            right_inclusive,
            window: None,
            _phantom: PhantomData,
        }
    }

    /// Return the absolute path of the file for a checkpointed Window.
    fn checkpoint_file(base: &StoragePath, persistent_id: &str) -> StoragePath {
        base.child(format!("window-{}.dat", persistent_id))
    }
}

impl<B, T> Operator for Window<B, T>
where
    B: IndexedZSet,
    T: ZBatchReader,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Window")
    }

    fn init(&mut self, global_id: &GlobalNodeId) {
        self.global_id = global_id.clone();
    }

    fn clock_start(&mut self, _scope: Scope) {
        self.window = None;
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        // Windows can currently only be used in top-level circuits.
        // Do we have meaningful examples of using windows inside nested scopes?
        panic!("'Window' operator used in fixedpoint iteration")
    }

    fn commit(&mut self, base: &StoragePath, persistent_id: Option<&str>) -> Result<(), Error> {
        let persistent_id = require_persistent_id(persistent_id, &self.global_id)?;
        let window_path = Self::checkpoint_file(base, persistent_id);

        let committed: CommittedWindow = (self as &Self).into();
        let as_bytes = to_bytes(&committed).expect("Serializing CommittedWindow should work.");
        Runtime::storage_backend()
            .unwrap()
            .write(&window_path, as_bytes)?;
        Ok(())
    }

    fn restore(&mut self, base: &StoragePath, persistent_id: Option<&str>) -> Result<(), Error> {
        let persistent_id = require_persistent_id(persistent_id, &self.global_id)?;

        let window_path = Self::checkpoint_file(base, persistent_id);
        let content = Runtime::storage_backend().unwrap().read(&window_path)?;
        let archived = unsafe { rkyv::archived_root::<CommittedWindow>(&content) };
        let committed: CommittedWindow = archived.deserialize(&mut rkyv::Infallible).unwrap();

        self.window = committed.window.map(|(a, b)| {
            // Serialize the window bounds back into the key.
            let mut boxed_a = self.factories.key_factory().default_box();
            let mut boxed_b = self.factories.key_factory().default_box();
            unsafe { boxed_a.deserialize_from_bytes(&a, 0) };
            unsafe { boxed_b.deserialize_from_bytes(&b, 0) };

            (boxed_a, boxed_b)
        });

        Ok(())
    }

    fn clear_state(&mut self) -> Result<(), Error> {
        self.window = None;
        Ok(())
    }
}

/// `true` if cursor points to a key to the left of the interval.
fn before_start<K, V, T, R, C>(cursor: &C, start: &K, inclusive: bool) -> bool
where
    C: Cursor<K, V, T, R>,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    if inclusive {
        cursor.key() < start
    } else {
        cursor.key() <= start
    }
}

/// `true` if cursor points to a key _not_ to the right of the interval.
fn before_end<K, V, T, R, C>(cursor: &C, end: &K, inclusive: bool) -> bool
where
    C: Cursor<K, V, T, R>,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    if inclusive {
        cursor.key() <= end
    } else {
        cursor.key() < end
    }
}

/// Seek to the first location after the end of the interval.
fn seek_after_end<K, V, T, R, C>(cursor: &mut C, end: &K, inclusive: bool)
where
    C: Cursor<K, V, T, R>,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    if inclusive {
        cursor.seek_key_with(&|key| key > end)
    } else {
        cursor.seek_key(end)
    }
}

/// Seek to the first location within the interval.
fn seek_start<K, V, T, R, C>(cursor: &mut C, start: &K, inclusive: bool)
where
    C: Cursor<K, V, T, R>,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    if inclusive {
        cursor.seek_key(start)
    } else {
        cursor.seek_key_with(&|key| key > start)
    }
}

impl<B, T> TernaryOperator<T, B, (Box<B::Key>, Box<B::Key>), B> for Window<B, T>
where
    B: IndexedZSet,
    T: ZBatchReader<Key = B::Key, Val = B::Val, Time = ()> + Clone,
    Box<B::Key>: Clone,
{
    /// * `batch` - input stream containing new time series data points indexed
    ///   by time.
    /// * `trace` - trace of the input stream up to, but not including current
    ///   clock cycle.
    /// * `bounds` - window bounds.  The lower bound must grow monotonically.
    // TODO: This can be optimized to add tuples in order, so we can use the
    // builder API to construct the output batch.  This requires processing
    // regions in order + extra care to iterate over `batch` and `trace` jointly
    // in region3.
    #[trace]
    async fn eval(
        &mut self,
        trace: Cow<'_, T>,
        batch: Cow<'_, B>,
        bounds: Cow<'_, (Box<B::Key>, Box<B::Key>)>,
    ) -> B {
        //           ┌────────────────────────────────────────┐
        //           │       previous window                  │
        //           │                                        │             e1
        // ──────────┴────────────┬───────────────────────────┴──────────────┬─────►
        //          s0          s1│                          e0              │
        //                        │         new window                       │
        //                        └──────────────────────────────────────────┘
        //  region 1: [s0 .. s1)
        //  region 2: [s1 .. e0)
        //  region 3: [e0 .. e1)

        let (start1, end1) = bounds.into_owned();
        // println!("{:?}-{:?}", start1, end1);
        let trace = trace.as_ref();
        let batch = batch.as_ref();

        // TODO: In order to preallocate the buffer, we need to estimate the number of
        // keys in each component below.  For this, we need to extend
        // `Cursor::seek` to return the number of keys skipped over by the search.
        let mut tuples = self.factories.weighted_items_factory().default_box();
        let mut tuple = self.factories.weighted_item_factory().default_box();
        let mut key = self.factories.key_factory().default_box();

        let mut trace_cursor = trace.cursor();
        let mut batch_cursor = batch.cursor();

        if let Some((start0, end0)) = &self.window {
            // Retract tuples in `trace` that slid out of the window (region 1).
            seek_start(&mut trace_cursor, start0, self.left_inclusive);
            while trace_cursor.key_valid()
                && before_start(&trace_cursor, &start1, self.left_inclusive)
                && before_end(&trace_cursor, end0, self.right_inclusive)
            {
                trace_cursor.key().clone_to(&mut *key);
                trace_cursor.map_values(&mut |val, weight| {
                    let (kv, w) = tuple.split_mut();
                    let (k, v) = kv.split_mut();
                    key.clone_to(k);
                    val.clone_to(v);
                    **w = weight.neg_by_ref();
                    tuples.push_val(&mut *tuple);
                });
                trace_cursor.step_key();
            }

            // If the window shrunk, retract values that dropped off the right end of the
            // window.
            if &end1 < end0 {
                seek_after_end(&mut trace_cursor, &end1, self.right_inclusive);

                while trace_cursor.key_valid()
                    && before_end(&trace_cursor, end0, self.right_inclusive)
                {
                    trace_cursor.key().clone_to(&mut key);
                    trace_cursor.map_values(&mut |val, weight| {
                        let (kv, w) = tuple.split_mut();
                        let (k, v) = kv.split_mut();
                        key.clone_to(k);
                        val.clone_to(v);
                        **w = weight.neg_by_ref();

                        tuples.push_val(&mut *tuple)
                    });
                    trace_cursor.step_key();
                }
            }

            // Add tuples in `trace` that slid into the window (region 3).
            seek_after_end(&mut trace_cursor, end0, self.right_inclusive);

            // In case start1 > end0
            seek_start(&mut trace_cursor, &start1, self.left_inclusive);
            // trace_cursor.seek_key(max(end0, &start1));
            while trace_cursor.key_valid() && before_end(&trace_cursor, &end1, self.right_inclusive)
            {
                trace_cursor.key().clone_to(&mut key);
                trace_cursor.map_values(&mut |val, weight| {
                    let (kv, w) = tuple.split_mut();
                    let (k, v) = kv.split_mut();
                    key.clone_to(k);
                    val.clone_to(v);
                    weight.clone_to(w);

                    tuples.push_val(&mut *tuple)
                });
                trace_cursor.step_key();
            }
        };

        // Insert tuples in `batch` that fall within the new window.
        seek_start(&mut batch_cursor, &start1, self.left_inclusive);
        // batch_cursor.seek_key(&start1);
        while batch_cursor.key_valid() && before_end(&batch_cursor, &end1, self.right_inclusive) {
            batch_cursor.key().clone_to(&mut key);
            batch_cursor.map_values(&mut |val, weight| {
                let (kv, w) = tuple.split_mut();
                let (k, v) = kv.split_mut();
                key.clone_to(k);
                val.clone_to(v);
                weight.clone_to(w);

                tuples.push_val(&mut *tuple)
            });
            batch_cursor.step_key();
        }

        self.window = Some((start1, end1));
        B::dyn_from_tuples(&self.factories, (), &mut tuples)
    }

    fn input_preference(
        &self,
    ) -> (
        OwnershipPreference,
        OwnershipPreference,
        OwnershipPreference,
    ) {
        (
            OwnershipPreference::INDIFFERENT,
            OwnershipPreference::INDIFFERENT,
            OwnershipPreference::INDIFFERENT,
        )
    }
}

#[cfg(test)]
mod test {
    use crate::{
        dynamic::{DowncastTrait, DynData, Erase},
        indexed_zset,
        operator::{dynamic::trace::TraceBound, Generator},
        typed_batch::{OrdIndexedZSet, TypedBox},
        utils::{Tup2, Tup3},
        zset, Circuit, IndexedZSet, OrdZSet, RootCircuit, Runtime, Stream,
    };
    use size_of::SizeOf;
    use std::vec;

    type Time = u64;

    // A simple, but ineffient implementation of `window` for testing.
    // (it's inefficient even for a non-incremental implementation).
    impl<C, B> Stream<C, B>
    where
        C: Circuit,
        B: IndexedZSet,
        Box<B::DynK>: Clone,
    {
        pub fn window_non_incremental(
            &self,
            (left_inclusive, right_inclusive): (bool, bool),
            bounds: &Stream<C, (TypedBox<B::Key, B::DynK>, TypedBox<B::Key, B::DynK>)>,
        ) -> Stream<C, B> {
            self.apply2(bounds, move |batch, (start, end)| {
                batch.filter(|k, _v| {
                    let left = if left_inclusive {
                        k >= start
                    } else {
                        k > start
                    };
                    let right = if right_inclusive { k <= end } else { k < end };

                    left && right
                })
            })
        }
    }

    // Test all combinations of open and closed intervals against reference implementation.
    fn compare_with_reference(
        stream: &Stream<RootCircuit, OrdIndexedZSet<u64, String>>,
        bounds: &Stream<RootCircuit, (TypedBox<Time, DynData>, TypedBox<Time, DynData>)>,
    ) {
        let closed_closed = stream.window((true, true), bounds).integrate();
        let closed_open = stream.window((true, false), bounds).integrate();
        let open_closed = stream.window((false, true), bounds).integrate();
        let open_open = stream.window((false, false), bounds).integrate();

        let closed_closed_expected = stream
            .integrate()
            .window_non_incremental((true, true), bounds);
        let closed_open_expected = stream
            .integrate()
            .window_non_incremental((true, false), bounds);
        let open_closed_expected = stream
            .integrate()
            .window_non_incremental((false, true), bounds);
        let open_open_expected = stream
            .integrate()
            .window_non_incremental((false, false), bounds);

        closed_closed.apply2(&closed_closed_expected, |actual, expected| {
            assert_eq!(actual, expected)
        });

        closed_open.apply2(&closed_open_expected, |actual, expected| {
            assert_eq!(actual, expected)
        });

        open_closed.apply2(&open_closed_expected, |actual, expected| {
            assert_eq!(actual, expected)
        });

        open_open.apply2(&open_open_expected, |actual, expected| {
            assert_eq!(actual, expected)
        });
    }

    #[test]
    fn sliding() {
        let circuit = RootCircuit::build(move |circuit| {
            let mut input = vec![
                zset! {
                    // old value before the first window, should never appear in the output.
                    Tup2(800, "800".to_string()) => 1i64, Tup2(900, "900".to_string()) => 1, Tup2(950, "950".to_string()) => 1, Tup2(999, "999".to_string()) => 1,
                    // will appear in the next window
                    Tup2(1000, "1000".to_string()) => 1
                },
                zset! {
                    // old value before the first window
                    Tup2(700, "700".to_string()) => 1,
                    // too late, the window already moved forward
                    Tup2(900, "900".to_string()) => 1,
                    Tup2(901, "901".to_string()) => 1,
                    Tup2(999, "999".to_string()) => 1,
                    Tup2(1000, "1000".to_string()) => 1,
                    Tup2(1001, "1001".to_string()) => 1, // will appear in the next window
                    Tup2(1002, "1002".to_string()) => 1, // will appear two windows later
                    Tup2(1003, "1003".to_string()) => 1, // will appear three windows later
                },
                zset! { Tup2(1004, "1004".to_string()) => 1 }, // no new values in this window
                zset! {},
                zset! {},
                zset! {},
            ]
            .into_iter();

            let mut output = vec![
                indexed_zset! { 900 => {"900".to_string() => 1} , 950 => {"950".to_string() => 1} , 999 => {"999".to_string() => 1} },
                indexed_zset! { 900 => {"900".to_string() => -1} , 901 => {"901".to_string() => 1} , 999 => {"999".to_string() => 1} , 1000 => {"1000".to_string() => 2} },
                indexed_zset! { 901 => {"901".to_string() => -1} , 1001 => {"1001".to_string() => 1} },
                indexed_zset! { 1002 => {"1002".to_string() => 1} },
                indexed_zset! { 1003 => {"1003".to_string() => 1} },
                indexed_zset! { 1004 => {"1004".to_string() => 1} },
            ]
            .into_iter();

            let mut clock: Time = 1000;
            let bounds: Stream<_, (TypedBox<Time, DynData>, TypedBox<Time, DynData>)> = circuit.add_source(Generator::new(move || {
                let res = (TypedBox::new(clock - 100), TypedBox::new(clock));
                clock += 1;
                res
            }));

            let index1: Stream<_, OrdIndexedZSet<Time, String>> = circuit
                .add_source(Generator::new(move || input.next().unwrap()))
                .map_index(|Tup2(k, v)| (*k, v.clone()));
            index1
                .window((true, false), &bounds)
                .inspect(move |batch| assert_eq!(batch, &output.next().unwrap()));

            compare_with_reference(&index1, &bounds);

            Ok(())
        })
        .unwrap()
        .0;

        for _ in 0..6 {
            circuit.step().unwrap();
        }
    }

    #[test]
    fn tumbling() {
        let circuit = RootCircuit::build(move |circuit| {
            let mut input = vec![
                // window: 995..1000
                zset! { Tup2(700, "700".to_string()) => 1 , Tup2(995, "995".to_string()) => 1 , Tup2(996, "996".to_string()) => 1 , Tup2(999, "999".to_string()) => 1 , Tup2(1000, "1000".to_string()) => 1 },
                zset! { Tup2(995, "995".to_string()) =>  1 , Tup2(1000, "1000".to_string()) => 1 , Tup2(1001, "1001".to_string()) => 1 },
                zset! { Tup2(999, "999".to_string()) => 1 },
                zset! { Tup2(1002, "1002".to_string()) => 1 },
                zset! { Tup2(1003, "1003".to_string()) => 1 },
                // window: 1000..1005
                zset! { Tup2(996, "996".to_string()) => 1 }, // no longer within window
                zset! { Tup2(999, "999".to_string()) => 1 },
                zset! { Tup2(1004, "1004".to_string()) => 1 },
                zset! { Tup2(1005, "1005".to_string()) => 1 }, // next window
                zset! { Tup2(1010, "1010".to_string()) => 1 },
                // window: 1005..1010
                zset! { Tup2(1005, "1005".to_string()) => 1  },
            ]
            .into_iter();

            let mut output = vec![
                indexed_zset! { 995 => {"995".to_string() => 1} , 996 => {"996".to_string() => 1} , 999 => {"999".to_string() => 1} },
                indexed_zset! { 995 => {"995".to_string() => 1} },
                indexed_zset! { 999 => {"999".to_string() => 1} },
                indexed_zset! {},
                indexed_zset! {},
                indexed_zset! { 1000 => {"1000".to_string() => 2} , 1001 => {"1001".to_string() => 1} , 1002 => {"1002".to_string() => 1} , 1003 => {"1003".to_string() => 1} , 995 => {"995".to_string() => -2} , 996 => {"996".to_string() => -1} , 999 => {"999".to_string() => -2} },
                indexed_zset! {},
                indexed_zset! { 1004 => {"1004".to_string() => 1} },
                indexed_zset! {},
                indexed_zset! {},
                indexed_zset! { 1000 => {"1000".to_string() => -2} , 1001 => {"1001".to_string() => -1} , 1002 => {"1002".to_string() => -1} , 1003 => {"1003".to_string() => -1} , 1004 => {"1004".to_string() => -1} , 1005 => {"1005".to_string() => 2} },
            ]
            .into_iter();

            const WINDOW_SIZE: Time = 5;
            let mut clock: Time = 1000;
            let bounds: Stream<_, (TypedBox<Time, DynData>, TypedBox<Time, DynData>)> = circuit
                .add_source(Generator::new(move || {
                    let start = (clock / WINDOW_SIZE) * WINDOW_SIZE - WINDOW_SIZE;
                    let res = (TypedBox::new(start), TypedBox::new(start + WINDOW_SIZE));
                    clock += 1;
                    res
                }));

            let index1: Stream<_, OrdIndexedZSet<Time, String>> = circuit
                .add_source(Generator::new(move || input.next().unwrap()))
                .map_index(|Tup2(k, v)| (*k, v.clone()));
            index1
                .window((true, false), &bounds)
                .inspect(move |batch| assert_eq!(batch, &output.next().unwrap()));

            compare_with_reference(&index1, &bounds);

            Ok(())
        })
        .unwrap().0;

        for _ in 0..11 {
            circuit.step().unwrap();
        }
    }

    #[test]
    fn shrinking() {
        let circuit = RootCircuit::build(move |circuit| {
            let mut input= vec![
                zset! {
                    Tup2(800u64, "800".to_string()) => 1,
                    Tup2(900, "900".to_string()) => 1,
                    Tup2(950, "950".to_string()) => 1,
                    Tup2(990, "990".to_string()) => 1,
                    Tup2(999, "999".to_string()) => 1,
                    Tup2(1000, "1000".to_string()) => 1
                },
                zset! {
                    Tup2(700, "700".to_string()) => 1,
                    Tup2(900, "900".to_string()) => 1,
                    Tup2(901, "901".to_string()) => 1,
                    Tup2(915, "915".to_string()) => 1,
                    Tup2(940, "940".to_string()) => 1,
                    Tup2(985, "985".to_string()) => 1,
                    Tup2(999, "999".to_string()) => 1,
                    Tup2(1000, "1000".to_string()) => 1,
                    Tup2(1001, "1001".to_string()) => 1,
                    Tup2(1002, "1002".to_string()) => 1,
                    Tup2(1003, "1003".to_string()) => 1,
                },
                zset! { Tup2(1004, "1004".to_string()) => 1,
                        Tup2(1010, "1010".to_string()) => 1,
                        Tup2(1020, "1020".to_string()) => 1,
                        Tup2(1039, "1039".to_string()) => 1 },
                zset! {},
                zset! {},
                zset! {},
                zset! {}
            ]
            .into_iter();

            let mut output = vec![
                indexed_zset! { Time => String: 900 => {"900".to_string() => 1} , 950 => {"950".to_string() => 1} , 990 => {"990".to_string() => 1}, 999 => {"999".to_string() => 1} },
                indexed_zset! { 900 => {"900".to_string() => -1} , 915 => {"915".to_string() => 1} , 940 => {"940".to_string() => 1} , 985 => {"985".to_string() => 1}, 990 => {"990".to_string() => -1}, 999 => {"999".to_string() => -1} },
                indexed_zset! { 915 => {"915".to_string() => -1} , 985 => {"985".to_string() => -1} },
                indexed_zset! { 1000 => {"1000".to_string() => 2}, 1001 => {"1001".to_string() => 1}, 1002 => {"1002".to_string() => 1}, 1003 => {"1003".to_string() => 1}, 1004 => {"1004".to_string() => 1}, 1010 => {"1010".to_string() => 1}, 1020 => {"1020".to_string() => 1}, 1039 => {"1039".to_string() => 1}, 985 => {"985".to_string() => 1}, 990 => {"990".to_string() => 1}, 999 => {"999".to_string() => 2} },
                indexed_zset! { 1039 => {"1039".to_string() => -1}, 940 => {"940".to_string() => -1} },
                indexed_zset! { 1020 => {"1020".to_string() => -1}, 950 => {"950".to_string() => -1} },
                indexed_zset! {}
            ]
            .into_iter();

            let mut windows = vec![
                // Shrink window from both ends.
                (900u64, 1000u64),
                (910, 990),
                (920, 980),
                // Jump forward and start shrinking again.
                (940, 1040),
                (950, 1030),
                (960, 1020),
                // Empty interval
                (1020,1020)

            ]
            .into_iter();

            let bounds: Stream<_, (TypedBox<Time, DynData>, TypedBox<Time, DynData>)> = circuit
                .add_source(Generator::new(move || {
                    let (t1, t2) = windows.next().unwrap();
                    (TypedBox::new(t1), TypedBox::new(t2))
                }));

            let index1: Stream<_, OrdIndexedZSet<Time, String>> = circuit
                .add_source(Generator::new(move || input.next().unwrap()))
                .map_index(|Tup2(k, v)| (*k, v.clone()));
            index1
                .window((true, false), &bounds)
                .inspect(move |batch| assert_eq!(batch, &output.next().unwrap()));

            compare_with_reference(&index1, &bounds);

            Ok(())
        })
        .unwrap().0;

        for _ in 0..6 {
            circuit.step().unwrap();
        }
    }

    #[test]
    fn bounded_memory() {
        let (mut dbsp, input_handle) = Runtime::init_circuit(8, |circuit| {
            let (input, input_handle) = circuit.add_input_indexed_zset::<i64, i64>();
            let bounds =
                input
                    .waterline_monotonic(|| 0, |ts| *ts)
                    .apply(|ts: &TypedBox<i64, DynData>| {
                        (
                            TypedBox::new(*ts.inner().downcast_checked::<i64>() - 1000),
                            TypedBox::new(*ts.inner().downcast_checked::<i64>()),
                        )
                    });

            let bound = TraceBound::new();
            bound.set(Box::new(i64::MAX).erase_box());

            input.window((true, false), &bounds);

            input
                .integrate_trace_with_bound(bound, TraceBound::new())
                .apply(|trace| {
                    assert!(trace.size_of().total_bytes() < 50000);
                });

            Ok(input_handle)
        })
        .unwrap();

        for i in 0..10000 {
            for j in i * 100..(i + 1) * 100 {
                input_handle.push(j, (1, 1));
            }
            dbsp.step().unwrap();
        }
    }

    /// This test shows how to implement a SQL query similar to
    ///
    /// SELECT
    ///   user,
    ///   COUNT(*)
    ///   FROM transactions
    ///   WHERE transactions.time >= now() - 1000
    /// GROUP BY user
    ///
    /// where now() is a monotonically increasing timestamp.
    /// By using `window` to implement the `WHERE` clause, we automatically GC old transactions
    /// and compute changes to the window incrementally.
    type Clock = i64;

    /// (time, user, amt)
    type Transaction = Tup3<Clock, String, u64>;

    #[test]
    fn aggregate_with_now() {
        let (mut dbsp, (now_handle, data_handle)) = Runtime::init_circuit(8, |circuit| {
            let (now, now_handle) = circuit.add_input_stream::<Clock>();

            let (data, data_handle) = circuit.add_input_zset::<Transaction>();
            let data_by_time = data.map_index(|x| (x.0, x.clone()));

            let bounds = now.apply(|ts: &Clock| (TypedBox::new(*ts - 1000), TypedBox::new(*ts)));

            let counts = data_by_time
                .window((true, false), &bounds)
                .map_index(|(_ts, x)| (x.1.clone(), x.clone()))
                .weighted_count();

            // Reference implementation: filter the entire collection wrt to now.
            let expected = data
                .integrate()
                .apply2(&now, |batch, ts| {
                    OrdZSet::from_keys(
                        (),
                        batch
                            .iter()
                            .filter_map(|(x, (), w)| {
                                if x.0 <= *ts && x.0 >= *ts - 1000 {
                                    Some(Tup2(x.clone(), w))
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<_>>(),
                    )
                })
                .differentiate()
                .map_index(|x| (x.1.clone(), x.clone()))
                .weighted_count();

            expected.apply2(&counts, |expected, actual| assert_eq!(expected, actual));

            Ok((now_handle, data_handle))
        })
        .unwrap();

        for i in 1..1000 {
            now_handle.set_for_all(i * 10);
            for j in (i - 1) * 10..i * 10 {
                data_handle.push(Tup3(j, format!("{}", j % 10), j as u64), 1);
            }
            dbsp.step().unwrap();
        }
    }
}
