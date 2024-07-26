//! Source operator that reads data from a CSV file.

// TODO:
// - Error handling (currently we just panic on reader error or deserialization
//   error).
// - Batching (don't read the whole file in one clock cycle)
// - Async implementation (wait for data to become available in the reader)
// - Sharded implementation (currently we feed all data on worker 0).

use crate::utils::Tup2;
use crate::{
    circuit::{
        operator_traits::{Operator, SourceOperator},
        Scope,
    },
    DBData, OrdZSet, Runtime, ZWeight,
};
use csv::Reader as CsvReader;
use serde::Deserialize;
use std::{borrow::Cow, io::Read, marker::PhantomData};

/// A source operator that reads records of type `T` from a CSV file.
///
/// The operator reads the entire file and yields its contents
/// in the first clock cycle as a Z-set with unit weights.
pub struct CsvSource<R, T> {
    reader: CsvReader<R>,
    time: usize,
    _t: PhantomData<fn(&T)>,
}

impl<R, T> CsvSource<R, T>
where
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

impl<R, T> Operator for CsvSource<R, T>
where
    R: 'static,
    T: 'static,
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

impl<R, T> SourceOperator<OrdZSet<T>> for CsvSource<R, T>
where
    T: DBData + for<'de> Deserialize<'de> + 'static,
    R: Read + 'static,
{
    fn eval(&mut self) -> OrdZSet<T> {
        let source = if self.time == 0 && Runtime::worker_index() == 0 {
            let mut data = Vec::<Tup2<Tup2<T, ()>, ZWeight>>::new();

            for x in self.reader.deserialize() {
                data.push(Tup2(Tup2(x.unwrap(), ()), 1));
            }

            OrdZSet::<T>::from_tuples((), data)
        } else {
            OrdZSet::<T>::empty()
        };
        self.time += 1;

        source
    }
}

#[cfg(test)]
mod test {
    use crate::operator::CsvSource;
    use crate::utils::Tup3;
    use crate::{zset, Circuit, OrdZSet, RootCircuit};
    use csv::ReaderBuilder;

    #[test]
    fn test_csv_reader() {
        let circuit = RootCircuit::build(move |circuit| {
            let expected = zset! {
                Tup3(18, 3, 237641) => 1,
                Tup3(237641, 4, 18) => 1,
                Tup3(18, 5, 21) => 1,
                Tup3(18, 5, 22) => 1,
                Tup3(18, 5, 23) => 1,
                Tup3(18, 5, 24) => 1,
                Tup3(18, 5, 25) => 1,
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
                .inspect(move |data: &OrdZSet<Tup3<u64, u64, u64>>| assert_eq!(data, &expected));
            Ok(())
        })
        .unwrap()
        .0;

        circuit.step().unwrap();
    }
}
