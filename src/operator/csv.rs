//! Source operator that reads data from a CSV file.
#![cfg(feature = "serde")]

// TODO:
// - Error handling (currently we just panic on reader error or deserialization
//   error).
// - Batching (don't read the whole file in one clock cycle)
// - Async implementation (wait for data to become available in the reader)
// - Sharded implementation.

use crate::circuit::{
    operator_traits::{Data, Operator, SourceOperator},
    Scope,
};
use csv::{Reader as CsvReader, Result as CsvResult};
use serde::Deserialize;
use std::{borrow::Cow, io::Read, marker::PhantomData};

/// A source operator that reads records of type `T` from a CSV file.
///
/// The operator reads the entire file and yields its contents
/// in the first clock cycle.
pub struct CsvSource<R, C, T> {
    reader: CsvReader<R>,
    _t: PhantomData<(C, T)>,
}

impl<R, C, T> CsvSource<R, C, T>
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
            _t: PhantomData,
        }
    }
}

impl<R, C, T> Operator for CsvSource<R, C, T>
where
    C: Data,
    R: 'static,
    T: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("CsvSource")
    }
    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}
}

impl<R, C, T> SourceOperator<C> for CsvSource<R, C, T>
where
    T: for<'de> Deserialize<'de> + 'static,
    R: Read + 'static,
    C: Data + FromIterator<T>,
{
    fn eval(&mut self) -> C {
        self.reader.deserialize().map(CsvResult::unwrap).collect()
    }
}

#[cfg(test)]
mod test {
    use crate::{circuit::Root, operator::CsvSource};
    use csv::ReaderBuilder;

    #[test]
    fn test_csv_reader() {
        let root = Root::build(move |circuit| {
            let expected = vec![
                (18, 3, 237641),
                (237641, 4, 18),
                (18, 5, 21),
                (18, 5, 22),
                (18, 5, 23),
                (18, 5, 24),
                (18, 5, 25),
            ];
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
                .inspect(move |data: &Vec<(usize, usize, usize)>| assert_eq!(data, &expected));
        })
        .unwrap();

        root.step().unwrap();
    }
}
