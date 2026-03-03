use dbsp::OrdIndexedZSet;
use dbsp::utils::Tup2;
use dbsp_adapters::static_compile::seroutput::SerBatchImpl;
use dbsp_adapters::{OutputConsumer, SerBatch};
use feldera_macros::IsNone;
use feldera_types::program_schema::{ColumnType, Field, Relation, SqlIdentifier};
use feldera_types::{deserialize_without_context, serialize_struct};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use size_of::SizeOf;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// Common type definitions for benchmarks
// ---------------------------------------------------------------------------

#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
pub struct BenchTestStruct {
    pub id: u32,
    pub b: bool,
    pub i: Option<i64>,
    pub s: String,
}

deserialize_without_context!(BenchTestStruct);

serialize_struct!(BenchTestStruct()[4]{
    id["id"]: u32,
    b["b"]: bool,
    i["i"]: Option<i64>,
    s["s"]: String
});

impl BenchTestStruct {
    pub fn relation_schema() -> Relation {
        Relation {
            name: SqlIdentifier::new("BenchTestStruct", false),
            fields: vec![
                Field::new("id".into(), ColumnType::bigint(false)),
                Field::new("b".into(), ColumnType::boolean(false)),
                Field::new("i".into(), ColumnType::bigint(true)),
                Field::new("s".into(), ColumnType::varchar(false)),
            ],
            materialized: false,
            properties: BTreeMap::new(),
        }
    }
}

#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
pub struct BenchKeyStruct {
    pub id: u32,
}

deserialize_without_context!(BenchKeyStruct);

serialize_struct!(BenchKeyStruct()[1]{
    id["id"]: u32
});

impl BenchKeyStruct {
    pub fn relation_schema() -> Relation {
        Relation {
            name: SqlIdentifier::new("BenchKeyStruct", false),
            fields: vec![Field::new("id".into(), ColumnType::bigint(false))],
            materialized: false,
            properties: BTreeMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Mock output consumer (used by Avro benchmark)
// ---------------------------------------------------------------------------

#[allow(clippy::type_complexity, dead_code)]
pub struct BenchOutputConsumer {
    data: Arc<
        Mutex<
            Vec<(
                Option<Vec<u8>>,
                Option<Vec<u8>>,
                Vec<(String, Option<Vec<u8>>)>,
            )>,
        >,
    >,
}

impl BenchOutputConsumer {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl OutputConsumer for BenchOutputConsumer {
    fn max_buffer_size_bytes(&self) -> usize {
        usize::MAX
    }

    fn batch_start(&mut self, _step: u64) {}

    fn push_buffer(&mut self, buffer: &[u8], _num_records: usize) {
        self.data
            .lock()
            .unwrap()
            .push((None, Some(buffer.to_vec()), vec![]));
    }

    fn push_key(
        &mut self,
        key: Option<&[u8]>,
        val: Option<&[u8]>,
        headers: &[(&str, Option<&[u8]>)],
        _num_records: usize,
    ) {
        self.data.lock().unwrap().push((
            key.map(|k| k.to_vec()),
            val.map(|v| v.to_vec()),
            headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.map(|bytes| bytes.to_vec())))
                .collect(),
        ));
    }

    fn batch_end(&mut self) {}
}

// ---------------------------------------------------------------------------
// Data generation and batch building
// ---------------------------------------------------------------------------

const SEED: u64 = 0xdeadbeef;

pub fn generate_test_data(num_records: usize) -> Vec<BenchTestStruct> {
    let mut rng = SmallRng::seed_from_u64(SEED);
    (0..num_records)
        .map(|i| BenchTestStruct {
            id: i as u32,
            b: rng.r#gen(),
            i: if rng.r#gen::<bool>() {
                Some(rng.gen_range(-1_000_000..1_000_000))
            } else {
                None
            },
            s: format!("record_{i}_{}", rng.gen_range(0u32..100_000)),
        })
        .collect()
}

pub fn build_indexed_batch(data: &[BenchTestStruct]) -> Arc<dyn SerBatch> {
    let tuples: Vec<_> = data
        .iter()
        .map(|v| Tup2(Tup2(BenchKeyStruct { id: v.id }, v.clone()), 1i64))
        .collect();
    let zset = OrdIndexedZSet::from_tuples((), tuples);
    Arc::new(<SerBatchImpl<_, BenchKeyStruct, BenchTestStruct>>::new(
        zset,
    ))
}
