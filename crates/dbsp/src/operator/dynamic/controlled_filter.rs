use std::borrow::Cow;

use crate::{
    algebra::{OrdZSet, ZBatch},
    circuit::{
        circuit_builder::RefStreamValue,
        operator_traits::{BinaryOperator, Operator},
    },
    dynamic::{DataTrait, Erase},
    trace::{Batch, BatchReader, BatchReaderFactories, Builder, Cursor},
    Circuit, DBData, Scope, Stream, ZWeight,
};

pub struct ControlledFilterFactories<Z, E>
where
    Z: ZBatch,
    E: DataTrait + ?Sized,
{
    batch_factories: Z::Factories,
    errors_factory: <OrdZSet<E> as BatchReader>::Factories,
}

impl<Z, E> ControlledFilterFactories<Z, E>
where
    Z: ZBatch,
    E: DataTrait + ?Sized,
{
    pub fn new<KType, VType, EType>() -> Self
    where
        KType: DBData + Erase<Z::Key>,
        VType: DBData + Erase<Z::Val>,
        EType: DBData + Erase<E>,
    {
        Self {
            batch_factories: BatchReaderFactories::new::<KType, VType, ZWeight>(),
            errors_factory: BatchReaderFactories::new::<EType, (), ZWeight>(),
        }
    }
}

impl<C, Z> Stream<C, Z>
where
    C: Circuit,
    Z: ZBatch<Time = ()>,
{
    #[track_caller]
    pub fn dyn_controlled_key_filter<T, E>(
        &self,
        factories: ControlledFilterFactories<Z, E>,
        threshold: &Stream<C, Box<T>>,
        filter_func: Box<dyn Fn(&T, &Z::Key) -> bool>,
        report_func: Box<dyn Fn(&T, &Z::Key, &Z::Val, ZWeight, &mut E)>,
    ) -> (Stream<C, Z>, Stream<C, OrdZSet<E>>)
    where
        T: DataTrait + ?Sized,
        Box<T>: Clone,
        E: DataTrait + ?Sized,
    {
        let error_stream_val = RefStreamValue::empty();
        let filter = ControlledKeyFilter::new(
            factories,
            filter_func,
            report_func,
            error_stream_val.clone(),
        );
        let output = self.circuit().add_binary_operator(filter, self, threshold);
        let error_stream = Stream::with_value(
            self.circuit().clone(),
            output.local_node_id(),
            error_stream_val,
        );

        (output, error_stream)
    }

    #[track_caller]
    pub fn dyn_controlled_value_filter<T, E>(
        &self,
        factories: ControlledFilterFactories<Z, E>,
        threshold: &Stream<C, Box<T>>,
        filter_func: Box<dyn Fn(&T, &Z::Key, &Z::Val) -> bool>,
        report_func: Box<dyn Fn(&T, &Z::Key, &Z::Val, ZWeight, &mut E)>,
    ) -> (Stream<C, Z>, Stream<C, OrdZSet<E>>)
    where
        T: DataTrait + ?Sized,
        Box<T>: Clone,
        E: DataTrait + ?Sized,
    {
        let error_stream_val = RefStreamValue::empty();
        let filter = ControlledValueFilter::new(
            factories,
            filter_func,
            report_func,
            error_stream_val.clone(),
        );
        let output = self.circuit().add_binary_operator(filter, self, threshold);
        let error_stream = Stream::with_value(
            self.circuit().clone(),
            output.local_node_id(),
            error_stream_val,
        );

        (output, error_stream)
    }
}

struct ControlledKeyFilter<Z, T, E>
where
    Z: ZBatch,
    T: DataTrait + ?Sized,
    E: DataTrait + ?Sized,
{
    factories: ControlledFilterFactories<Z, E>,
    filter_func: Box<dyn Fn(&T, &Z::Key) -> bool>,
    report_func: Box<dyn Fn(&T, &Z::Key, &Z::Val, ZWeight, &mut E)>,
    error_stream_val: RefStreamValue<OrdZSet<E>>,
}

impl<Z, T, E> ControlledKeyFilter<Z, T, E>
where
    Z: ZBatch,
    T: DataTrait + ?Sized,
    E: DataTrait + ?Sized,
{
    fn new(
        factories: ControlledFilterFactories<Z, E>,
        filter_func: Box<dyn Fn(&T, &Z::Key) -> bool>,
        report_func: Box<dyn Fn(&T, &Z::Key, &Z::Val, ZWeight, &mut E)>,
        error_stream_val: RefStreamValue<OrdZSet<E>>,
    ) -> Self {
        Self {
            factories,
            filter_func,
            report_func,
            error_stream_val,
        }
    }
}

impl<Z, T, E> Operator for ControlledKeyFilter<Z, T, E>
where
    Z: ZBatch,
    T: DataTrait + ?Sized,
    E: DataTrait + ?Sized,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("ControlledKeyFilter")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<Z, T, E> BinaryOperator<Z, Box<T>, Z> for ControlledKeyFilter<Z, T, E>
where
    Z: ZBatch<Time = ()>,
    T: DataTrait + ?Sized,
    E: DataTrait + ?Sized,
{
    fn eval(&mut self, data: &Z, threshold: &Box<T>) -> Z {
        let mut builder = Z::Builder::new_builder(&self.factories.batch_factories, ());

        let mut cursor = data.cursor();

        let mut errors =
            <OrdZSet<E> as Batch>::Builder::new_builder(&self.factories.errors_factory, ());

        let mut error = self.factories.errors_factory.key_factory().default_box();

        while cursor.key_valid() {
            if (self.filter_func)(threshold, cursor.key()) {
                while cursor.val_valid() {
                    let w = **cursor.weight();
                    builder.push_refs(cursor.key(), cursor.val(), w.erase());
                    cursor.step_val();
                }
            } else {
                while cursor.val_valid() {
                    let w = **cursor.weight();
                    (self.report_func)(threshold, cursor.key(), cursor.val(), w, &mut error);

                    errors.push_vals(&mut error, ().erase_mut(), 1.erase_mut());
                    cursor.step_val();
                }
            }
            cursor.step_key();
        }

        unsafe { self.error_stream_val.put(errors.done()) };
        builder.done()
    }
}

struct ControlledValueFilter<Z, T, E>
where
    Z: ZBatch,
    T: DataTrait + ?Sized,
    E: DataTrait + ?Sized,
{
    factories: ControlledFilterFactories<Z, E>,
    filter_func: Box<dyn Fn(&T, &Z::Key, &Z::Val) -> bool>,
    report_func: Box<dyn Fn(&T, &Z::Key, &Z::Val, ZWeight, &mut E)>,
    error_stream_val: RefStreamValue<OrdZSet<E>>,
}

impl<Z, T, E> ControlledValueFilter<Z, T, E>
where
    Z: ZBatch,
    T: DataTrait + ?Sized,
    E: DataTrait + ?Sized,
{
    fn new(
        factories: ControlledFilterFactories<Z, E>,
        filter_func: Box<dyn Fn(&T, &Z::Key, &Z::Val) -> bool>,
        report_func: Box<dyn Fn(&T, &Z::Key, &Z::Val, ZWeight, &mut E)>,
        error_stream_val: RefStreamValue<OrdZSet<E>>,
    ) -> Self {
        Self {
            factories,
            filter_func,
            report_func,
            error_stream_val,
        }
    }
}

impl<Z, T, E> Operator for ControlledValueFilter<Z, T, E>
where
    Z: ZBatch,
    T: DataTrait + ?Sized,
    E: DataTrait + ?Sized,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("ControlledValueFilter")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}
impl<Z, T, E> BinaryOperator<Z, Box<T>, Z> for ControlledValueFilter<Z, T, E>
where
    Z: ZBatch<Time = ()>,
    T: DataTrait + ?Sized,
    E: DataTrait + ?Sized,
{
    fn eval(&mut self, data: &Z, threshold: &Box<T>) -> Z {
        let mut builder = Z::Builder::new_builder(&self.factories.batch_factories, ());

        let mut cursor = data.cursor();

        let mut errors =
            <OrdZSet<E> as Batch>::Builder::new_builder(&self.factories.errors_factory, ());

        let mut error = self.factories.errors_factory.key_factory().default_box();

        while cursor.key_valid() {
            while cursor.val_valid() {
                let w = **cursor.weight();

                if (self.filter_func)(threshold, cursor.key(), cursor.val()) {
                    builder.push_refs(cursor.key(), cursor.val(), w.erase());
                } else {
                    (self.report_func)(threshold, cursor.key(), cursor.val(), w, &mut error);

                    errors.push_vals(&mut error, ().erase_mut(), 1.erase_mut());
                }
                cursor.step_val();
            }
            cursor.step_key();
        }

        unsafe { self.error_stream_val.put(errors.done()) };
        builder.done()
    }
}

#[cfg(test)]
mod test {
    use std::cmp::max;

    use crate::{
        circuit::CircuitConfig,
        utils::{Tup1, Tup2},
        OrdIndexedZSet, OrdZSet, Runtime,
    };

    #[test]
    fn controlled_key_filter_test() {
        let (mut dbsp, (input_handle, output_handle, error_handle)) =
            Runtime::init_circuit(CircuitConfig::from(4), |circuit| {
                let (input, input_handle) = circuit.add_input_zset::<u64>();
                let threshold = input.waterline(
                    || Tup1(0),
                    |&x, _| Tup1(x.saturating_sub(5)),
                    |x, y| *max(x, y),
                );

                let (output, errors) = input.controlled_key_filter(
                    &threshold,
                    |t, k| *k >= t.0,
                    |t, k, _v, _w| format!("{k} < {}", t.0),
                );

                let output_handle = output.output();
                let error_handle = errors.output();

                Ok((input_handle, output_handle, error_handle))
            })
            .unwrap();

        for i in 1..11 {
            for j in 0..10 {
                input_handle.push(10 * i + j - 10, 1);
            }

            dbsp.step().unwrap();

            let expected_output = (4..10).map(|j| Tup2(10 * i + j - 10, 1)).collect();

            assert_eq!(
                output_handle.consolidate(),
                OrdZSet::from_keys((), expected_output)
            );

            let expected_errors = (0..4)
                .map(|j| Tup2(format!("{} < {}", 10 * i + j - 10, 10 * i - 6), 1))
                .collect();

            assert_eq!(
                error_handle.consolidate(),
                OrdZSet::from_keys((), expected_errors)
            );
        }
    }

    #[test]
    fn controlled_value_filter_test() {
        let (mut dbsp, (input_handle, output_handle, error_handle)) =
            Runtime::init_circuit(CircuitConfig::from(4), |circuit| {
                let (input, input_handle) = circuit.add_input_zset::<u64>();
                let threshold = input.waterline(
                    || Tup1(0),
                    |&x, _| Tup1(x.saturating_sub(5)),
                    |x, y| *max(x, y),
                );

                let indexed = input.map_index(|i| (*i, *i));

                let (output, errors) = indexed.controlled_value_filter(
                    &threshold,
                    |t, k, v| *k >= t.0 && *v % 2 == 0,
                    |t, k, _v, _w| format!("{k} < {}", t.0),
                );

                let output_handle = output.output();
                let error_handle = errors.output();

                Ok((input_handle, output_handle, error_handle))
            })
            .unwrap();

        for i in 1..11 {
            for j in 0..10 {
                input_handle.push(10 * i + j - 10, 1);
            }

            dbsp.step().unwrap();

            let expected_output = [4, 6, 8]
                .iter()
                .map(|j| Tup2(Tup2(10 * i + j - 10, 10 * i + j - 10), 1))
                .collect();

            assert_eq!(
                output_handle.consolidate(),
                OrdIndexedZSet::from_tuples((), expected_output)
            );

            let expected_errors = [0, 1, 2, 3, 5, 7, 9]
                .iter()
                .map(|j| Tup2(format!("{} < {}", 10 * i + j - 10, 10 * i - 6), 1))
                .collect();

            assert_eq!(
                error_handle.consolidate(),
                OrdZSet::from_keys((), expected_errors)
            );
        }
    }
}
