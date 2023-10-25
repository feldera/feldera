use crate::{
    operator::communication::new_exchange_operators,
    trace::{cursor::Cursor, BatchReader, Rkyv},
    Circuit, DBData, NumEntries, RootCircuit, Runtime, Stream,
};
use size_of::SizeOf;
use std::{cmp::max, panic::Location};

impl<B> Stream<RootCircuit, B>
where
    B: BatchReader + Clone + 'static,
{
    /// Compute the watermark of a time series, where the watermark function is
    /// monotonic in event time.  The notion of time here is distinct from the
    /// DBSP logical time and can be modeled using any type that implements
    /// `Ord`.
    ///
    /// Watermark is an attribute of a time series that indicates the latest
    /// timestamp such that no data points with timestamps older than the
    /// watermark should appear in the stream. Every record in the time
    /// series carries watermark information that can be extracted by
    /// applying a user-provided function to it.  The watermark of the time
    /// series is the maximum of watermarks of all its data points.
    ///
    /// This method computes the watermark of a time series assuming that the
    /// watermark function is monotonic in the event time, e.g., `watermark
    /// = event_time - 5s`.  Such watermarks are the most common in practice
    /// and can be computed efficiently by only considering the latest
    /// timestamp in each input batch.   The method takes a stream of batches
    /// indexed by timestamp and outputs a stream of watermarks (scalar
    /// values).  Its output at each timestamp is a scalar (not a Z-set),
    /// computed as the maximum of the previous watermark and the largest
    /// watermark in the new input batch.
    #[track_caller]
    pub fn watermark_monotonic<WF, IF, TS>(
        &self,
        init: IF,
        watermark_func: WF,
    ) -> Stream<RootCircuit, TS>
    where
        IF: Fn() -> TS + 'static,
        WF: Fn(&B::Key) -> TS + 'static,
        TS: Ord + Clone + SizeOf + NumEntries + Send + Rkyv + 'static,
    {
        let local_watermark = self.stream_fold(init(), move |old_watermark, batch| {
            let mut cursor = batch.cursor();
            cursor.fast_forward_keys();
            match cursor.get_key() {
                Some(key) => max(old_watermark, watermark_func(key)),
                None => old_watermark,
            }
        });

        if let Some(runtime) = Runtime::runtime() {
            let num_workers = runtime.num_workers();
            if num_workers == 1 {
                return local_watermark;
            }

            let (sender, receiver) = new_exchange_operators(
                &runtime,
                Runtime::worker_index(),
                Some(Location::caller()),
                init,
                move |watermark: TS, watermarks: &mut Vec<TS>| {
                    for _ in 0..num_workers {
                        watermarks.push(watermark.clone());
                    }
                },
                |result, watermark| {
                    if &watermark > result {
                        *result = watermark;
                    }
                },
            );

            self.circuit()
                .add_exchange(sender, receiver, &local_watermark)
        } else {
            local_watermark
        }
    }
}

impl<B> Stream<RootCircuit, B>
where
    B: BatchReader + Clone + 'static,
{
    /// Computes the least upper bound over all records that occurred in the
    /// stream with respect to some user-defined lattice.
    ///
    /// The primary use of this function is in time series analytics in
    /// computing the largest timestamp observed in the stream, which can in
    /// turn be used in computing retainment policies for data in this
    /// stream and streams derived from it (see
    /// [`Stream::integrate_trace_retain_keys`] and
    /// [`Stream::integrate_trace_retain_values`]).
    ///
    /// Note: the notion of time here is distinct from the DBSP logical time and
    /// represents one or several physical timestamps embedded in the input
    /// data.
    ///
    /// In the special case where timestamps form a total order and the input
    /// stream is indexed by time, the
    /// [`watermark_monotonic`](`Stream::watermark_monotonic`) function can
    /// be used instead of this method to compute the bound more
    /// efficiently.
    ///
    /// # Arguments
    ///
    /// * `init` - initial value of the bound, usually the bottom element of the
    ///   lattice.
    /// * `extract_ts` - extracts a timestamp from a key-value pair.
    /// * `least_upper_bound` - computes the least upper bound of two
    ///   timestamps.
    #[track_caller]
    pub fn watermark<TS, WF, IF, LB>(
        &self,
        init: IF,
        extract_ts: WF,
        least_upper_bound: LB,
    ) -> Stream<RootCircuit, TS>
    where
        IF: Fn() -> TS + 'static,
        WF: Fn(&B::Key, &B::Val) -> TS + 'static,
        LB: Fn(&TS, &TS) -> TS + Clone + 'static,
        TS: DBData + NumEntries,
    {
        let least_upper_bound_clone = least_upper_bound.clone();

        let local_watermark = self.stream_fold(init(), move |old_watermark, batch| {
            let mut watermark = old_watermark;

            let mut cursor = batch.cursor();

            while cursor.key_valid() {
                while cursor.val_valid() {
                    watermark = least_upper_bound_clone(
                        &watermark,
                        &extract_ts(cursor.key(), cursor.val()),
                    );
                    cursor.step_val();
                }
                cursor.step_key();
            }
            watermark
        });

        if let Some(runtime) = Runtime::runtime() {
            let num_workers = runtime.num_workers();
            if num_workers == 1 {
                return local_watermark;
            }

            let (sender, receiver) = new_exchange_operators(
                &runtime,
                Runtime::worker_index(),
                Some(Location::caller()),
                init,
                move |watermark: TS, watermarks: &mut Vec<TS>| {
                    for _ in 0..num_workers {
                        watermarks.push(watermark.clone());
                    }
                },
                move |result, watermark| {
                    *result = least_upper_bound(result, &watermark);
                },
            );

            self.circuit()
                .add_exchange(sender, receiver, &local_watermark)
        } else {
            local_watermark
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::max;

    use crate::Runtime;

    fn test_watermark_monotonic(workers: usize) {
        let mut expected_watermarks = vec![115, 115, 125, 145].into_iter();

        let (mut dbsp, input_handle) = Runtime::init_circuit(workers, move |circuit| {
            let (stream, handle) = circuit.add_input_zset();
            stream
                .watermark_monotonic(|| 0, |ts| ts + 5)
                .inspect(move |watermark| {
                    if Runtime::worker_index() == 0 {
                        assert_eq!(watermark, &expected_watermarks.next().unwrap());
                    }
                });
            Ok(handle)
        })
        .unwrap();

        input_handle.append(&mut vec![(100, 1), (110, 1), (50, 1)]);
        dbsp.step().unwrap();

        input_handle.append(&mut vec![(90, 1), (90, 1), (50, 1)]);
        dbsp.step().unwrap();

        input_handle.append(&mut vec![(110, 1), (120, 1), (100, 1)]);
        dbsp.step().unwrap();

        input_handle.append(&mut vec![(130, 1), (140, 1), (0, 1)]);
        dbsp.step().unwrap();

        dbsp.kill().unwrap();
    }

    #[test]
    fn test_watermark_monotonic1() {
        test_watermark_monotonic(1);
    }

    #[test]
    fn test_watermark_monotonic4() {
        test_watermark_monotonic(4);
    }

    fn test_watermark(workers: usize) {
        let mut expected_watermarks = vec![(-10, 1), (100, 3), (100, 7), (250, 7)].into_iter();

        let (mut dbsp, input_handle) = Runtime::init_circuit(workers, move |circuit| {
            let (stream, handle) = circuit.add_input_indexed_zset::<i32, i32, _>();
            stream
                .watermark(
                    || (i32::MIN, i32::MIN),
                    |k, v| (*k, *v),
                    |(ts1_left, ts2_left), (ts1_right, ts2_right)| {
                        (max(*ts1_left, *ts1_right), max(*ts2_left, *ts2_right))
                    },
                )
                .inspect(move |watermark| {
                    if Runtime::worker_index() == 0 {
                        assert_eq!(watermark, &expected_watermarks.next().unwrap());
                    }
                });
            Ok(handle)
        })
        .unwrap();

        input_handle.append(&mut vec![(-100, (-5, 1)), (-10, (1, 1)), (-200, (1, 1))]);
        dbsp.step().unwrap();

        input_handle.append(&mut vec![(0, (1, 1)), (-100, (2, 1)), (100, (3, 1))]);
        dbsp.step().unwrap();

        input_handle.append(&mut vec![(50, (5, 1)), (-200, (-10, 1)), (99, (7, 1))]);
        dbsp.step().unwrap();

        input_handle.append(&mut vec![(130, (1, 1)), (140, (1, 1)), (250, (1, 1))]);
        dbsp.step().unwrap();

        dbsp.kill().unwrap();
    }

    #[test]
    fn test_watermark1() {
        test_watermark(1);
    }

    #[test]
    fn test_watermark4() {
        test_watermark(4);
    }
}
