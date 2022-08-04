//! Source operator that reads data from a CSV file.
#![cfg(feature = "with-serde")]

// TODO:
// - Error handling (currently we just panic on reader error or deserialization
//   error).
// - Batching (don't read the whole file in one clock cycle)
// - Async implementation (wait for data to become available in the reader)
// - Sharded implementation (currently we feed all data on worker 0).

use crate::{
    algebra::{ZRingValue, ZSet},
    circuit::{
        operator_traits::{Data, Operator, SourceOperator},
        Scope,
    },
    Runtime,
};
use csv::Reader as CsvReader;
use serde::Deserialize;
use std::{borrow::Cow, io::Read, marker::PhantomData};

/// A source operator that reads records of type `T` from a CSV file.
///
/// The operator reads the entire file and yields its contents
/// in the first clock cycle as a Z-set with unit weights.
pub struct CsvSource<R, T, W, C> {
    reader: CsvReader<R>,
    time: usize,
    _t: PhantomData<(C, T, W)>,
}

impl<R, T, W, C> CsvSource<R, T, W, C>
where
    C: Clone,
    R: Read,
{
    /// Create a [`CsvSource`] instance from any reader using
    /// default `CsvReader` settings.
    pub fn from_reader(reader: R) -> Self {
        Self::from_csv_reader(CsvReader::from_reader(reader))
    }

    /// Create a [`CsvSource`] from a pre-configured `CsvReader`.
    pub fn from_csv_reader(reader: CsvReader<R>) -> Self {
        Self {
            reader,
            time: 0,
            _t: PhantomData,
        }
    }
}

impl<R, T, W, C> Operator for CsvSource<R, T, W, C>
where
    C: Data,
    R: 'static,
    T: 'static,
    W: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("CsvSource")
    }
    fn clock_start(&mut self, _scope: Scope) {
        self.time = 0;
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        self.time >= 2
    }
}

impl<R, T, W, C> SourceOperator<C> for CsvSource<R, T, W, C>
where
    T: for<'de> Deserialize<'de> + 'static,
    W: ZRingValue + 'static,
    R: Read + 'static,
    C: Data + ZSet<Key = T, R = W>,
{
    fn eval(&mut self) -> C {
        let source = if self.time == 0 && Runtime::worker_index() == 0 {
            let data: Vec<_> = self
                .reader
                .deserialize()
                .map(|x| (x.unwrap(), W::one()))
                .collect();

            C::from_keys((), data)
        } else {
            C::zero()
        };
        self.time += 1;

        source
    }
}

#[cfg(test)]
mod test {
    use crate::{operator::CsvSource, zset, Circuit, OrdZSet};
    use csv::ReaderBuilder;

    #[test]
    fn test_csv_reader() {
        let circuit = Circuit::build(move |circuit| {
            let expected = zset! {
                (18, 3, 237641) => 1,
                (237641, 4, 18) => 1,
                (18, 5, 21) => 1,
                (18, 5, 22) => 1,
                (18, 5, 23) => 1,
                (18, 5, 24) => 1,
                (18, 5, 25) => 1,
            };
            let csv_data = "\
18,3,237641
237641,4,18
18,5,21
18,5,22
18,5,23
18,5,24
18,5,25
";
            let reader = ReaderBuilder::new()
                .delimiter(b',')
                .has_headers(false)
                .from_reader(csv_data.as_bytes());
            circuit
                .add_source(CsvSource::from_csv_reader(reader))
                .inspect(move |data: &OrdZSet<(usize, usize, usize), isize>| {
                    assert_eq!(data, &expected)
                });
        })
        .unwrap()
        .0;

        circuit.step().unwrap();
    }
}
