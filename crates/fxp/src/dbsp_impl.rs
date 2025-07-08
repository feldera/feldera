use dbsp::algebra::{HasOne, HasZero, MulByRef, OptionWeightType};
use feldera_types::serde_with_context::{
    serde_config::DecimalFormat, DeserializeWithContext, SerializeWithContext, SqlSerdeConfig,
};
use serde::{Deserializer, Serialize, Serializer};
use smallstr::SmallString;
use std::fmt::{Display, Write};

use crate::{DynamicDecimal, Fixed, FixedInteger};

impl<const P: usize, const S: usize> OptionWeightType for Fixed<P, S> {}
impl<const P: usize, const S: usize> OptionWeightType for &Fixed<P, S> {}

impl<const P: usize, const S: usize> HasZero for Fixed<P, S> {
    fn is_zero(&self) -> bool {
        *self == Self::ZERO
    }

    fn zero() -> Self {
        Self::ZERO
    }
}

impl<const P: usize, const S: usize> HasOne for Fixed<P, S> {
    /// This will panic if 1 can't be represented in this type (that is, if `S
    /// >= P`).
    fn one() -> Self {
        Self::ONE
    }
}

impl<const P: usize, const S: usize> MulByRef<isize> for Fixed<P, S> {
    type Output = Self;

    fn mul_by_ref(&self, other: &isize) -> Self::Output {
        self.checked_mul_generic(FixedInteger::for_isize(*other))
            .unwrap()
    }
}

impl<const P: usize, const S: usize> MulByRef<i64> for Fixed<P, S> {
    type Output = Self;

    fn mul_by_ref(&self, other: &i64) -> Self::Output {
        self.checked_mul_generic(FixedInteger::for_i64(*other))
            .unwrap()
    }
}

impl<const P: usize, const S: usize> MulByRef<i32> for Fixed<P, S> {
    type Output = Self;

    fn mul_by_ref(&self, other: &i32) -> Self::Output {
        self.checked_mul_generic(FixedInteger::for_i32(*other))
            .unwrap()
    }
}

fn serialize_with_context_helper<S, T>(
    value: T,
    serializer: S,
    context: &SqlSerdeConfig,
) -> Result<S::Ok, S::Error>
where
    T: Serialize + Display,
    S: Serializer,
{
    match context.decimal_format {
        DecimalFormat::Numeric => value.serialize(serializer),
        DecimalFormat::String => {
            let mut string = SmallString::<[u8; 64]>::new();
            write!(&mut string, "{value}").unwrap();
            serializer.serialize_str(&string)
        }
    }
}

impl<const P: usize, const S: usize> SerializeWithContext<SqlSerdeConfig> for Fixed<P, S> {
    fn serialize_with_context<Ser>(
        &self,
        serializer: Ser,
        context: &SqlSerdeConfig,
    ) -> Result<Ser::Ok, Ser::Error>
    where
        Ser: Serializer,
    {
        serialize_with_context_helper(self, serializer, context)
    }
}

impl SerializeWithContext<SqlSerdeConfig> for DynamicDecimal {
    fn serialize_with_context<Ser>(
        &self,
        serializer: Ser,
        context: &SqlSerdeConfig,
    ) -> Result<Ser::Ok, Ser::Error>
    where
        Ser: Serializer,
    {
        serialize_with_context_helper(self, serializer, context)
    }
}

impl<'de, C, const P: usize, const S: usize> DeserializeWithContext<'de, C> for Fixed<P, S> {
    #[inline(never)]
    fn deserialize_with_context<D>(deserializer: D, _context: &'de C) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        serde::Deserialize::deserialize(deserializer)
    }
}

impl<'de, C> DeserializeWithContext<'de, C> for DynamicDecimal {
    #[inline(never)]
    fn deserialize_with_context<D>(deserializer: D, _context: &'de C) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        serde::Deserialize::deserialize(deserializer)
    }
}

#[cfg(test)]
mod test {
    use std::sync::{Arc, Mutex};

    use dbsp::{
        algebra::DefaultSemigroup, indexed_zset, operator::Fold, typed_batch::OrdIndexedZSet,
        utils::Tup2, Runtime, ZWeight,
    };

    use crate::Fixed;

    // This is copied from `crates/dbsp/src/operator/dynamic/aggregate/mod.rs`
    // with the value type changed from `i64` to `Fixed<10,0>`.
    fn count_test(workers: usize) {
        type D = Fixed<10, 0>;
        const fn d(x: i32) -> D {
            D::for_i32(x)
        }

        let count_weighted_output: Arc<Mutex<OrdIndexedZSet<u64, ZWeight>>> =
            Arc::new(Mutex::new(indexed_zset! {}));
        let sum_weighted_output: Arc<Mutex<OrdIndexedZSet<u64, D>>> =
            Arc::new(Mutex::new(indexed_zset! {}));
        let count_distinct_output: Arc<Mutex<OrdIndexedZSet<u64, D>>> =
            Arc::new(Mutex::new(indexed_zset! {}));
        let sum_distinct_output: Arc<Mutex<OrdIndexedZSet<u64, D>>> =
            Arc::new(Mutex::new(indexed_zset! {}));

        let count_weighted_output_clone = count_weighted_output.clone();
        let count_distinct_output_clone = count_distinct_output.clone();
        let sum_weighted_output_clone = sum_weighted_output.clone();
        let sum_distinct_output_clone = sum_distinct_output.clone();

        let (mut dbsp, input_handle) = Runtime::init_circuit(workers, move |circuit| {
            let (input_stream, input_handle) = circuit.add_input_indexed_zset::<u64, D>();
            input_stream
                .weighted_count()
                .gather(0)
                .inspect(move |batch| {
                    if Runtime::worker_index() == 0 {
                        *count_weighted_output.lock().unwrap() = batch.clone();
                    }
                });

            input_stream
                .aggregate_linear(|value: &D| *value)
                .gather(0)
                .inspect(move |batch| {
                    if Runtime::worker_index() == 0 {
                        *sum_weighted_output.lock().unwrap() = batch.clone();
                    }
                });

            input_stream
                .aggregate(<Fold<_, _, DefaultSemigroup<_>, _, _>>::new(
                    d(0),
                    |sum: &mut D, _v: &D, _w| *sum += d(1),
                ))
                .gather(0)
                .inspect(move |batch| {
                    if Runtime::worker_index() == 0 {
                        *count_distinct_output.lock().unwrap() = batch.clone();
                    }
                });

            input_stream
                .aggregate(<Fold<_, _, DefaultSemigroup<_>, _, _>>::new(
                    d(0),
                    |sum: &mut D, v: &D, _w| *sum += v,
                ))
                .gather(0)
                .inspect(move |batch| {
                    if Runtime::worker_index() == 0 {
                        *sum_distinct_output.lock().unwrap() = batch.clone();
                    }
                });

            Ok(input_handle)
        })
        .unwrap();

        input_handle.append(&mut vec![Tup2(1u64, Tup2(d(1), 1)), Tup2(1, Tup2(d(2), 2))]);
        dbsp.step().unwrap();
        assert_eq!(
            &*count_distinct_output_clone.lock().unwrap(),
            &indexed_zset! {1 => {d(2) => 1}}
        );
        assert_eq!(
            &*sum_distinct_output_clone.lock().unwrap(),
            &indexed_zset! {1 => {d(3) => 1}}
        );
        assert_eq!(
            &*count_weighted_output_clone.lock().unwrap(),
            &indexed_zset! {1 => {3 => 1}}
        );
        assert_eq!(
            &*sum_weighted_output_clone.lock().unwrap(),
            &indexed_zset! {1 => {d(5) => 1}}
        );

        input_handle.append(&mut vec![
            Tup2(2, Tup2(d(2), 1)),
            Tup2(2, Tup2(d(4), 1)),
            Tup2(1, Tup2(d(2), -1)),
        ]);
        dbsp.step().unwrap();
        assert_eq!(
            &*count_distinct_output_clone.lock().unwrap(),
            &indexed_zset! {2 => {d(2) => 1}}
        );
        assert_eq!(
            &*sum_distinct_output_clone.lock().unwrap(),
            &indexed_zset! {2 => {d(6) => 1}}
        );
        assert_eq!(
            &*count_weighted_output_clone.lock().unwrap(),
            &indexed_zset! {1 => {3 => -1, 2 => 1}, 2 => {2 => 1}}
        );
        assert_eq!(
            &*sum_weighted_output_clone.lock().unwrap(),
            &indexed_zset! {2 => {d(6) => 1}, 1 => {d(5) => -1, d(3) => 1}}
        );

        input_handle.append(&mut vec![Tup2(1, Tup2(d(3), 1)), Tup2(1, Tup2(d(2), -1))]);
        dbsp.step().unwrap();
        assert_eq!(
            &*count_distinct_output_clone.lock().unwrap(),
            &indexed_zset! {}
        );
        assert_eq!(
            &*sum_distinct_output_clone.lock().unwrap(),
            &indexed_zset! {1 => {d(3) => -1, d(4) => 1}}
        );
        assert_eq!(
            &*count_weighted_output_clone.lock().unwrap(),
            &indexed_zset! {}
        );
        assert_eq!(
            &*sum_weighted_output_clone.lock().unwrap(),
            &indexed_zset! {1 => {d(3) => -1, d(4) => 1}}
        );

        dbsp.kill().unwrap();
    }

    #[test]
    fn count_test1() {
        count_test(1);
    }

    #[test]
    fn count_test4() {
        count_test(4);
    }
}
