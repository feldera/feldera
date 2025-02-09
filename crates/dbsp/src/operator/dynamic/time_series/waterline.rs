use dyn_clone::clone_box;
use size_of::SizeOf;

use crate::circuit::checkpointer::Checkpoint;
use crate::dynamic::{DynData, DynUnit};
use crate::operator::dynamic::{MonoIndexedZSet, MonoZSet};
use crate::{
    dynamic::DataTrait,
    operator::communication::new_exchange_operators,
    trace::{BatchReader, Cursor, Rkyv},
    Circuit, NumEntries, RootCircuit, Runtime, Stream,
};
use std::{cmp::max, panic::Location};

pub trait LeastUpperBoundFn<TS: ?Sized>: Fn(&TS, &TS, &mut TS) {
    fn fork(&self) -> LeastUpperBoundFunc<TS>;
}

impl<TS: ?Sized, F> LeastUpperBoundFn<TS> for F
where
    F: Fn(&TS, &TS, &mut TS) + Clone + 'static,
{
    fn fork(&self) -> LeastUpperBoundFunc<TS> {
        Box::new(self.clone())
    }
}

pub type LeastUpperBoundFunc<TS> = Box<dyn LeastUpperBoundFn<TS>>;

impl Stream<RootCircuit, MonoIndexedZSet> {
    #[track_caller]
    pub fn dyn_waterline_mono(
        &self,
        init: Box<dyn Fn() -> Box<DynData>>,
        extract_ts: Box<dyn Fn(&DynData, &DynData, &mut DynData)>,
        least_upper_bound: LeastUpperBoundFunc<DynData>,
    ) -> Stream<RootCircuit, Box<DynData>> {
        self.dyn_waterline(init, extract_ts, least_upper_bound)
    }
}

impl Stream<RootCircuit, MonoZSet> {
    #[track_caller]
    pub fn dyn_waterline_mono(
        &self,
        init: Box<dyn Fn() -> Box<DynData>>,
        extract_ts: Box<dyn Fn(&DynData, &DynUnit, &mut DynData)>,
        least_upper_bound: LeastUpperBoundFunc<DynData>,
    ) -> Stream<RootCircuit, Box<DynData>> {
        self.dyn_waterline(init, extract_ts, least_upper_bound)
    }
}

impl<B> Stream<RootCircuit, B>
where
    B: BatchReader + Clone + 'static,
{
    /// See [`Stream::waterline_monotonic`].
    #[track_caller]
    pub fn dyn_waterline_monotonic<TS>(
        &self,
        init: Box<dyn Fn() -> Box<TS>>,
        waterline_func: Box<dyn Fn(&B::Key, &mut TS)>,
    ) -> Stream<RootCircuit, Box<TS>>
    where
        TS: Checkpoint + DataTrait + ?Sized,
        Box<TS>: Clone + SizeOf + NumEntries + Rkyv,
    {
        self.circuit().region("waterline_monotonic", || {
            let local_waterline = self.stream_fold(init(), move |old_waterline, batch| {
                let mut new_waterline = clone_box(old_waterline.as_ref());
                let mut cursor = batch.cursor();
                cursor.fast_forward_keys();
                match cursor.get_key() {
                    Some(key) => {
                        waterline_func(key, &mut new_waterline);
                        max(old_waterline, new_waterline)
                    }
                    None => old_waterline,
                }
            });

            if let Some(runtime) = Runtime::runtime() {
                let num_workers = runtime.num_workers();
                if num_workers == 1 {
                    return local_waterline;
                }

                let (sender, receiver) = new_exchange_operators(
                    &runtime,
                    Runtime::worker_index(),
                    Some(Location::caller()),
                    init,
                    move |waterline: Box<TS>, waterlines: &mut Vec<Box<TS>>| {
                        for _ in 0..num_workers {
                            waterlines.push(clone_box(waterline.as_ref()));
                        }
                    },
                    |result, waterline| {
                        if &waterline > result {
                            *result = waterline;
                        }
                    },
                );

                self.circuit()
                    .add_exchange(sender, receiver, &local_waterline)
            } else {
                local_waterline
            }
        })
    }
}

impl<B> Stream<RootCircuit, B>
where
    B: BatchReader + Clone + 'static,
{
    /// See [`Stream::waterline`].
    #[track_caller]
    pub fn dyn_waterline<TS>(
        &self,
        init: Box<dyn Fn() -> Box<TS>>,
        extract_ts: Box<dyn Fn(&B::Key, &B::Val, &mut TS)>,
        least_upper_bound: LeastUpperBoundFunc<TS>,
    ) -> Stream<RootCircuit, Box<TS>>
    where
        TS: Checkpoint + DataTrait + ?Sized,
        Box<TS>: Clone + SizeOf + NumEntries + Rkyv,
    {
        self.circuit().region("waterline", || {
            let least_upper_bound_clone = least_upper_bound.fork();

            let local_waterline = self.stream_fold(init(), move |mut old_waterline, batch| {
                let mut ts = clone_box(old_waterline.as_ref());
                let mut new_waterline = clone_box(old_waterline.as_ref());

                let mut cursor = batch.cursor();

                while cursor.key_valid() {
                    while cursor.val_valid() {
                        extract_ts(cursor.key(), cursor.val(), &mut ts);
                        least_upper_bound_clone(&old_waterline, &mut ts, &mut new_waterline);
                        new_waterline.clone_to(&mut old_waterline);
                        cursor.step_val();
                    }
                    cursor.step_key();
                }
                new_waterline
            });

            if let Some(runtime) = Runtime::runtime() {
                let num_workers = runtime.num_workers();
                if num_workers == 1 {
                    return local_waterline;
                }

                let (sender, receiver) = new_exchange_operators(
                    &runtime,
                    Runtime::worker_index(),
                    Some(Location::caller()),
                    init,
                    move |waterline: Box<TS>, waterlines: &mut Vec<Box<TS>>| {
                        for _ in 0..num_workers {
                            waterlines.push(waterline.clone());
                        }
                    },
                    move |result, waterline| {
                        let old_result = clone_box(result);
                        least_upper_bound(&old_result, &waterline, result.as_mut());
                    },
                );

                self.circuit()
                    .add_exchange(sender, receiver, &local_waterline)
            } else {
                local_waterline
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        dynamic::{DowncastTrait, DynData},
        typed_batch::TypedBox,
        utils::Tup2,
        Runtime,
    };
    use std::cmp::max;

    #[allow(clippy::borrowed_box)]
    fn test_waterline_monotonic(workers: usize) {
        let mut expected_waterlines = vec![115, 115, 125, 145].into_iter();

        let (mut dbsp, input_handle) = Runtime::init_circuit(workers, move |circuit| {
            let (stream, handle) = circuit.add_input_zset();
            stream
                .waterline_monotonic(|| 0, |ts| ts + 5)
                .inner_data()
                .inspect(move |waterline: &Box<DynData>| {
                    if Runtime::worker_index() == 0 {
                        assert_eq!(
                            waterline.downcast_checked::<i32>(),
                            &expected_waterlines.next().unwrap()
                        );
                    }
                });
            Ok(handle)
        })
        .unwrap();

        input_handle.append(&mut vec![
            Tup2::new(100, 1),
            Tup2::new(110, 1),
            Tup2::new(50, 1),
        ]);
        dbsp.step().unwrap();

        input_handle.append(&mut vec![
            Tup2::new(90, 1),
            Tup2::new(90, 1),
            Tup2::new(50, 1),
        ]);
        dbsp.step().unwrap();

        input_handle.append(&mut vec![
            Tup2::new(110, 1),
            Tup2::new(120, 1),
            Tup2::new(100, 1),
        ]);
        dbsp.step().unwrap();

        input_handle.append(&mut vec![
            Tup2::new(130, 1),
            Tup2::new(140, 1),
            Tup2::new(0, 1),
        ]);
        dbsp.step().unwrap();

        dbsp.kill().unwrap();
    }

    #[test]
    fn test_waterline_monotonic1() {
        test_waterline_monotonic(1);
    }

    #[test]
    fn test_waterline_monotonic4() {
        test_waterline_monotonic(4);
    }

    fn test_waterline(workers: usize) {
        let mut expected_waterlines = vec![(-10, 1), (100, 3), (100, 7), (250, 7)].into_iter();

        let (mut dbsp, input_handle) = Runtime::init_circuit(workers, move |circuit| {
            let (stream, handle) = circuit.add_input_indexed_zset::<i32, i32>();
            stream
                .waterline(
                    || (i32::MIN, i32::MIN),
                    |k, v| (*k, *v),
                    |(ts1_left, ts2_left), (ts1_right, ts2_right)| {
                        (max(*ts1_left, *ts1_right), max(*ts2_left, *ts2_right))
                    },
                )
                .inspect(move |waterline: &TypedBox<(i32, i32), DynData>| {
                    if Runtime::worker_index() == 0 {
                        assert_eq!(
                            waterline.inner().downcast_checked::<(i32, i32)>(),
                            &expected_waterlines.next().unwrap()
                        );
                    }
                });
            Ok(handle)
        })
        .unwrap();

        input_handle.append(&mut vec![
            Tup2::new(-100, Tup2::new(-5, 1)),
            Tup2::new(-10, Tup2::new(1, 1)),
            Tup2::new(-200, Tup2::new(1, 1)),
        ]);
        dbsp.step().unwrap();

        input_handle.append(&mut vec![
            Tup2::new(0, Tup2::new(1, 1)),
            Tup2::new(-100, Tup2::new(2, 1)),
            Tup2::new(100, Tup2::new(3, 1)),
        ]);
        dbsp.step().unwrap();

        input_handle.append(&mut vec![
            Tup2::new(50, Tup2::new(5, 1)),
            Tup2::new(-200, Tup2::new(-10, 1)),
            Tup2::new(99, Tup2::new(7, 1)),
        ]);
        dbsp.step().unwrap();

        input_handle.append(&mut vec![
            Tup2::new(130, Tup2::new(1, 1)),
            Tup2::new(140, Tup2::new(1, 1)),
            Tup2::new(250, Tup2::new(1, 1)),
        ]);
        dbsp.step().unwrap();

        dbsp.kill().unwrap();
    }

    #[test]
    fn test_waterline1() {
        test_waterline(1);
    }

    #[test]
    fn test_waterline4() {
        test_waterline(4);
    }
}
