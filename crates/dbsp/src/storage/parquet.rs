use std::{
    fs::File,
    ops::{Deref, DerefMut},
    path::Path,
    sync::Arc,
};

use arrow::{
    array::ArrayBuilder,
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::{
        arrow_reader::ParquetRecordBatchReaderBuilder,
        ArrowWriter,
    },
    errors::ParquetError,
};
use thiserror::Error;

use crate::{
    dynamic::arrow::{ArrowSupport, ArrowSupportDyn},
    typed_batch::OrdIndexedZSet,
    utils::Tup2,
    DBData, Runtime, ZWeight,
};

#[derive(Debug, Error)]
pub enum ParquetStorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("Parquet error: {0}")]
    Parquet(#[from] ParquetError),
}

pub struct ParquetIndexedZSet<K: DBData, V: DBData> {
    inner: OrdIndexedZSet<K, V>,
}

impl<K: DBData, V: DBData> ParquetIndexedZSet<K, V> {
    pub fn new(inner: OrdIndexedZSet<K, V>) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> OrdIndexedZSet<K, V> {
        self.inner
    }

    pub fn write_to_path<P: AsRef<Path>>(&self, path: P) -> Result<(), ParquetStorageError> {
        let mut key_builder = <K as ArrowSupport>::ArrayBuilderType::default();
        let mut value_builder = <V as ArrowSupport>::ArrayBuilderType::default();
        let mut diff_builder = <ZWeight as ArrowSupport>::ArrayBuilderType::default();

        for (key, value, diff) in self.inner.iter() {
            key.serialize_into_arrow_builder(key_builder.as_mut());
            value.serialize_into_arrow_builder(value_builder.as_mut());
            diff.serialize_into_arrow_builder(diff_builder.as_mut());
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", <K as ArrowSupport>::arrow_data_type(), false),
            Field::new("value", <V as ArrowSupport>::arrow_data_type(), false),
            Field::new("diff", <ZWeight as ArrowSupport>::arrow_data_type(), false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                key_builder.finish(),
                value_builder.finish(),
                diff_builder.finish(),
            ],
        )?;

        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }

    pub fn read_from_path<P: AsRef<Path>>(path: P) -> Result<Self, ParquetStorageError> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let mut reader = builder.build()?;
        let mut tuples = Vec::new();

        while let Some(batch) = reader.next() {
            let batch = batch?;
            let key_col = batch.column(0).clone();
            let value_col = batch.column(1).clone();
            let diff_col = batch.column(2).clone();

            for index in 0..batch.num_rows() {
                let mut key = K::default();
                key.deserialize_from_arrow(&key_col, index);
                let mut value = V::default();
                value.deserialize_from_arrow(&value_col, index);
                let mut diff = ZWeight::default();
                diff.deserialize_from_arrow(&diff_col, index);
                tuples.push(Tup2(Tup2(key, value), diff));
            }
        }

        Ok(Self {
            inner: OrdIndexedZSet::from_tuples((), tuples),
        })
    }
}

impl<K: DBData, V: DBData> From<OrdIndexedZSet<K, V>> for ParquetIndexedZSet<K, V> {
    fn from(value: OrdIndexedZSet<K, V>) -> Self {
        Self::new(value)
    }
}

impl<K: DBData, V: DBData> Deref for ParquetIndexedZSet<K, V> {
    type Target = OrdIndexedZSet<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K: DBData, V: DBData> DerefMut for ParquetIndexedZSet<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::Tup2;
    use tempfile::NamedTempFile;

    #[test]
    fn parquet_indexed_zset_roundtrip() {
        let tuples = vec![
            Tup2(Tup2(1u64, ()), 1),
            Tup2(Tup2(2u64, ()), -1),
            Tup2(Tup2(3u64, ()), 4),
        ];

        let batch = OrdIndexedZSet::from_tuples((), tuples);

        let parquet_batch = ParquetIndexedZSet::from(batch.clone());
        let tmp = NamedTempFile::new().unwrap();
        parquet_batch.write_to_path(tmp.path()).unwrap();

        let restored = ParquetIndexedZSet::<u64, ()>::read_from_path(tmp.path()).unwrap();
        assert_eq!(
            restored.iter().collect::<Vec<_>>(),
            batch.iter().collect::<Vec<_>>()
        );

        let (mut runtime, (mut input, output)) = Runtime::init_circuit(1, |circuit| {
            let (stream, handle) = circuit.add_input_indexed_zset::<u64, ()>();
            Ok((handle, stream.output()))
        })
        .unwrap();

        input.append(&restored);
        runtime.step().unwrap();

        let snapshot = output.consolidate();
        assert_eq!(
            snapshot.iter().collect::<Vec<_>>(),
            batch.iter().collect::<Vec<_>>()
        );

        runtime.kill().unwrap();
    }
}
