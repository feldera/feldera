//! Descriptor-owned input and persisted state for the frozen global aggregates.

use super::{
    Layout, NativeFactory, NativeRow,
    aggregate_fixture::{case_from_file, count_filter, sum_column},
    circuit_fixture::{bag, items, number, one},
    layer::{ItemLayout, NativeLayerFactory},
    merge::{Rows, cache, compact, write_rows},
    single,
};
use dbsp::{
    Circuit, DBData, DBSPHandle, Error, OrdZSet, OutputHandle, Runtime,
    circuit::{
        CircuitConfig, CircuitStorageConfig, Scope,
        operator_traits::{FileOperatorCheckpoint, Operator, OperatorCheckpoint, UnaryOperator},
    },
    dynamic::DynData,
    operator::InputHandle,
    storage::{
        backend::{FileReader, StorageBackend, StoragePath},
        buffer_cache::FBuf,
        file::reader::ArchivedLayerReader,
    },
    utils::{Tup1, Tup2},
};
use feldera_native_probe::{
    ArchiveField, ArchiveInteger, Batch, Comparison, CompiledProgram, Expr, Program,
};
use feldera_types::config::{StorageConfig, StorageOptions};
use rkyv::{Archived, option::ArchivedOption};
use std::{borrow::Cow, collections::BTreeMap, mem::offset_of, sync::Arc};

type Counts = Tup2<i64, i64>;
type Updates<T> = Vec<Tup2<T, i64>>;
fn count_factory() -> Arc<NativeLayerFactory> {
    type Key = Tup1<i32>;
    type ArchivedKey = Archived<Key>;
    let key = NativeFactory {
        layout: Layout::compile(
            5,
            size_of::<ArchivedKey>(),
            align_of::<ArchivedKey>(),
            vec![ArchiveField {
                integer: ArchiveInteger::I32,
                payload_offset: offset_of!(ArchivedKey, 0),
                null_tag_offset: None,
            }],
        ),
    };
    Arc::new(
        // SAFETY: the key descriptor uses ArchivedKey offsets and single encodes Tup1<i64>.
        unsafe { NativeLayerFactory::new(key, single(), ItemLayout::for_rkyv::<Key, Tup1<i64>>()) },
    )
}

struct InputBatch {
    factory: Arc<NativeLayerFactory>,
    rows: Rows,
}

impl std::fmt::Debug for InputBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InputBatch")
            .field("rows", &self.rows.len())
            .finish()
    }
}

impl InputBatch {
    fn new(factory: Arc<NativeLayerFactory>, updates: Updates<Option<i32>>) -> Self {
        let mut totals = BTreeMap::<Option<i32>, i64>::new();
        for Tup2(key, weight) in updates {
            *totals.entry(key).or_default() += weight;
        }
        let rows = totals
            .into_iter()
            .filter(|(_, weight)| *weight != 0)
            .map(|(key, weight)| {
                (
                    factory.key().row(vec![key.map(i128::from)]),
                    factory.aux().row(vec![Some(weight.into())]),
                )
            })
            .collect();
        Self { factory, rows }
    }
}

fn integer(value: &DynData) -> Option<i64> {
    let row = value.as_any().downcast_ref::<NativeRow>().unwrap();
    row.values[0].map(|value| i64::try_from(value).unwrap())
}

fn weight(value: &DynData) -> i64 {
    integer(value).expect("weight must be present")
}

fn input_values(rows: &Rows) -> Batch {
    let values: Vec<_> = rows
        .iter()
        .map(|(key, _)| vec![integer(key.as_ref())])
        .collect();
    Batch::from_rows(1, &values).unwrap()
}

trait Aggregation: 'static {
    type Output: DBData;
    fn name(&self) -> &'static str;
    fn filter(&self, _rows: &Rows) -> Option<Vec<Option<i64>>> {
        None
    }
    fn summarize(&self, rows: &Rows) -> Self::Output;
}

struct CountDistinct(CompiledProgram);

impl CountDistinct {
    fn new(threshold: i32) -> Result<Self, feldera_native_probe::Error> {
        let program = Program::new(
            1,
            vec![
                Expr::Column(0),
                Expr::Literal(Some(threshold.into())),
                Expr::Compare {
                    op: Comparison::GreaterThan,
                    left: 0,
                    right: 1,
                },
            ],
        )?;
        Ok(Self(CompiledProgram::compile(&program)?))
    }
}

impl Aggregation for CountDistinct {
    type Output = Counts;

    fn name(&self) -> &'static str {
        "DescriptorCountDistinct"
    }

    fn filter(&self, rows: &Rows) -> Option<Vec<Option<i64>>> {
        Some(self.0.evaluate(&input_values(rows)).unwrap())
    }

    fn summarize(&self, rows: &Rows) -> Counts {
        Tup2(
            rows.iter().map(|(_, value)| weight(value.as_ref())).sum(),
            i64::try_from(rows.len()).unwrap(),
        )
    }
}

struct NullableSum(CompiledProgram);

impl NullableSum {
    fn new() -> Result<Self, feldera_native_probe::Error> {
        let program = Program::new(
            1,
            vec![
                Expr::Column(0),
                Expr::Literal(Some(0)),
                Expr::Coalesce(vec![0, 1]),
            ],
        )?;
        Ok(Self(CompiledProgram::compile(&program)?))
    }
}

impl Aggregation for NullableSum {
    type Output = Tup1<Option<i32>>;

    fn name(&self) -> &'static str {
        "DescriptorNullableSum"
    }

    fn summarize(&self, rows: &Rows) -> Self::Output {
        let coalesced = self.0.evaluate(&input_values(rows)).unwrap();
        let mut sum = 0_i128;
        let mut non_null = 0_i128;
        for ((key, diff), value) in rows.iter().zip(coalesced) {
            let diff = i128::from(weight(diff.as_ref()));
            sum += i128::from(value.expect("coalesced INTEGER")) * diff;
            if integer(key.as_ref()).is_some() {
                non_null += diff;
            }
        }
        Tup1((non_null != 0).then(|| i32::try_from(sum).unwrap()))
    }
}

struct AggregateState<A: Aggregation> {
    backend: Arc<dyn StorageBackend>,
    factory: Arc<NativeLayerFactory>,
    aggregate: A,
    layer: Option<Arc<dyn FileReader>>,
    snapshot: Option<Arc<FBuf>>,
    last: Option<A::Output>,
}

impl<A: Aggregation> AggregateState<A> {
    fn open(&self, path: &StoragePath) -> ArchivedLayerReader {
        ArchivedLayerReader::open(cache, self.backend.as_ref(), path, 1).unwrap()
    }

    fn transaction(&mut self, updates: Option<&InputBatch>) -> OrdZSet<A::Output> {
        let mut layers = Vec::with_capacity(2);
        if let Some(layer) = &self.layer {
            layers.push(self.open(layer.path()));
        }
        let spill = updates.map(|batch| {
            assert!(Arc::ptr_eq(&self.factory, &batch.factory));
            let filter = self.aggregate.filter(&batch.rows);
            let admitted = batch.rows.iter().enumerate().filter_map(|(index, row)| {
                filter
                    .as_ref()
                    .is_none_or(|flags| flags[index] == Some(1))
                    .then_some(row)
            });
            write_rows(self.backend.as_ref(), self.factory.clone(), admitted)
        });
        if let Some(spill) = &spill {
            layers.push(self.open(spill.path()));
        }
        let compacted = compact(&self.factory, &layers);
        let current = self.aggregate.summarize(&compacted);
        let file = write_rows(self.backend.as_ref(), self.factory.clone(), &compacted);
        // This bounded probe keeps a copy of its small layer for the checkpoint
        // writer. The raw layer still drives live compaction and restart.
        self.snapshot = Some(self.backend.read(file.path()).unwrap());
        self.layer = Some(file);

        let mut delta = Vec::new();
        if self.last.as_ref() != Some(&current) {
            delta.extend(self.last.replace(current.clone()).map(|old| Tup2(old, -1)));
            delta.push(Tup2(current, 1));
        }
        OrdZSet::from_keys((), delta)
    }
}

fn checkpoint_name(persistent_id: Option<&str>) -> String {
    let id = persistent_id.expect("the probe registers a persistent operator ID");
    format!("{id}-worker-{}.feldera", Runtime::worker_index())
}

impl<A: Aggregation> Operator for AggregateState<A> {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed(self.aggregate.name())
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        false
    }

    fn checkpoint(
        &mut self,
        persistent_id: Option<&str>,
    ) -> Result<Option<Box<dyn OperatorCheckpoint>>, Error> {
        let snapshot = self.snapshot.as_ref().ok_or_else(|| {
            std::io::Error::other("the probe needs one transaction before checkpoint")
        })?;
        Ok(Some(Box::new(FileOperatorCheckpoint {
            name: checkpoint_name(persistent_id),
            content: FBuf::clone(snapshot),
        })))
    }

    fn restore(&mut self, base: &StoragePath, persistent_id: Option<&str>) -> Result<(), Error> {
        let name = checkpoint_name(persistent_id);
        let path = base.clone().join(name.as_str());
        self.snapshot = Some(self.backend.read(&path)?);
        self.layer = Some(self.backend.open(&path)?);
        let rows = compact(&self.factory, &[self.open(&path)]);
        self.last = Some(self.aggregate.summarize(&rows));
        Ok(())
    }
}

impl<A: Aggregation> UnaryOperator<Option<Arc<InputBatch>>, OrdZSet<A::Output>>
    for AggregateState<A>
{
    async fn eval(&mut self, batch: &Option<Arc<InputBatch>>) -> OrdZSet<A::Output> {
        self.transaction(batch.as_deref())
    }
}

struct AggregateCircuit<T: DBData> {
    runtime: DBSPHandle,
    input: InputHandle<Option<Arc<InputBatch>>>,
    output: OutputHandle<OrdZSet<T>>,
    factory: Arc<NativeLayerFactory>,
}

impl<T: DBData> AggregateCircuit<T> {
    fn new<A: Aggregation<Output = T>>(
        config: CircuitConfig,
        factory: Arc<NativeLayerFactory>,
        create: impl FnOnce() -> Result<A, feldera_native_probe::Error> + Clone + Send + 'static,
    ) -> Self {
        let owned = factory.clone();
        let (runtime, (input, output)) = Runtime::init_circuit(config, move |circuit| {
            let aggregate = create()?;
            let persistent_id = aggregate.name();
            let state = AggregateState {
                backend: Runtime::storage_backend()?,
                factory: owned,
                aggregate,
                layer: None,
                snapshot: None,
                last: None,
            };
            let (source, input) = circuit.add_input_stream::<Option<Arc<InputBatch>>>();
            let output = circuit
                .add_unary_operator(state, &source)
                .set_persistent_id(Some(persistent_id))
                .output();
            Ok((input, output))
        })
        .unwrap();
        Self {
            runtime,
            input,
            output,
            factory,
        }
    }

    fn step(&mut self, updates: Updates<Option<i32>>, expected: Updates<T>) {
        self.input.set_for_worker(
            0,
            Some(Arc::new(InputBatch::new(self.factory.clone(), updates))),
        );
        self.runtime.transaction().unwrap();
        let result = self.output.take_from_worker(0).unwrap();
        assert_eq!(result, OrdZSet::from_keys((), expected));
    }
}

#[test]
#[ignore = "requires P0_COUNT_DISTINCT_CASE from the frozen native corpus archive"]
fn frozen_count_distinct_uses_owned_input_and_spilled_batches() {
    let case = case_from_file("P0_COUNT_DISTINCT_CASE", "emptyAggregate");
    let (column, threshold) = count_filter(&case);
    let directory = tempfile::tempdir().unwrap();
    let mut circuit =
        AggregateCircuit::new(storage(directory.path()), count_factory(), move || {
            CountDistinct::new(threshold)
        });
    let original_descriptor = Arc::downgrade(&circuit.factory.key().layout);
    let mut frozen_steps = 0;
    for command in items(&case["commands"]) {
        assert_eq!(command["kind"], "step");
        let input = bag(one(&command["inputs"]), "T", |fields| {
            Some(number(&fields[column], "DBSPI32Literal").unwrap())
        });
        let expected = bag(one(&command["outputs"]), "V", |fields| {
            let [rows, keys] = fields else {
                panic!("expected two counts")
            };
            Tup2(
                number(rows, "DBSPI64Literal").unwrap(),
                number(keys, "DBSPI64Literal").unwrap(),
            )
        });
        circuit.step(input, expected);
        frozen_steps += 1;
    }
    let first = threshold + 1;
    let second = threshold + 2;
    circuit.step(
        vec![
            Tup2(Some(threshold), 1),
            Tup2(Some(first), 2),
            Tup2(Some(first), 1),
            Tup2(Some(second), 1),
        ],
        vec![Tup2(Tup2(0, 0), -1), Tup2(Tup2(4, 2), 1)],
    );
    let checkpoint = circuit.runtime.checkpoint().run().unwrap();
    circuit.runtime.kill().unwrap();
    drop(circuit.factory);
    assert!(original_descriptor.upgrade().is_none());

    let mut restored_config = storage(directory.path());
    restored_config.storage.as_mut().unwrap().init_checkpoint = Some(checkpoint.uuid);
    let mut restored = AggregateCircuit::new(restored_config, count_factory(), move || {
        CountDistinct::new(threshold)
    });
    restored.step(vec![], vec![]);
    restored.step(
        vec![Tup2(Some(first), -2)],
        vec![Tup2(Tup2(4, 2), -1), Tup2(Tup2(2, 2), 1)],
    );
    restored.step(
        vec![Tup2(Some(first), -1), Tup2(Some(second), -1)],
        vec![Tup2(Tup2(2, 2), -1), Tup2(Tup2(0, 0), 1)],
    );
    restored.runtime.kill().unwrap();
    println!("descriptor COUNT DISTINCT: {frozen_steps} frozen steps and 4 restart checks");
}

fn storage(path: &std::path::Path) -> CircuitConfig {
    CircuitConfig::with_workers(1).with_storage(Some(
        CircuitStorageConfig::for_config(
            StorageConfig {
                path: path.to_string_lossy().into_owned(),
                cache: Default::default(),
            },
            StorageOptions::default(),
        )
        .unwrap(),
    ))
}

fn sum_factory() -> Arc<NativeLayerFactory> {
    type Key = Tup1<Option<i32>>;
    type ArchivedKey = Archived<Key>;
    let option = ArchivedOption::Some(0_i32);
    let ArchivedOption::Some(value) = &option else {
        unreachable!()
    };
    let payload = std::ptr::from_ref(value) as usize - std::ptr::from_ref(&option) as usize;
    let tag = offset_of!(ArchivedKey, 0);
    let key = NativeFactory {
        layout: Layout::compile(
            6,
            size_of::<ArchivedKey>(),
            align_of::<ArchivedKey>(),
            vec![ArchiveField {
                integer: ArchiveInteger::I32,
                payload_offset: tag + payload,
                null_tag_offset: Some(tag),
            }],
        ),
    };
    Arc::new(
        // SAFETY: the key descriptor uses ArchivedKey offsets and single encodes Tup1<i64>.
        unsafe { NativeLayerFactory::new(key, single(), ItemLayout::for_rkyv::<Key, Tup1<i64>>()) },
    )
}

#[test]
#[ignore = "requires P0_NULLABLE_SUM_CASE from the frozen native corpus archive"]
fn frozen_nullable_sum_uses_owned_input_and_spilled_batches() {
    let case = case_from_file("P0_NULLABLE_SUM_CASE", "optionAggregateTest");
    let column = sum_column(&case);
    let directory = tempfile::tempdir().unwrap();
    let mut circuit =
        AggregateCircuit::new(storage(directory.path()), sum_factory(), NullableSum::new);
    let original_descriptor = Arc::downgrade(&circuit.factory.key().layout);
    let mut frozen_steps = 0;
    for command in items(&case["commands"]) {
        assert_eq!(command["kind"], "step");
        let input = bag(one(&command["inputs"]), "T", |fields| {
            number(&fields[column], "DBSPI32Literal")
        });
        let expected = bag(one(&command["outputs"]), "V", |fields| {
            Tup1(number(&fields[0], "DBSPI32Literal"))
        });
        circuit.step(input, expected);
        frozen_steps += 1;
    }
    circuit.step(vec![Tup2(None, 1)], vec![]);
    circuit.step(
        vec![Tup2(Some(5), 1), Tup2(Some(-5), 1)],
        vec![Tup2(Tup1(None), -1), Tup2(Tup1(Some(0)), 1)],
    );
    let checkpoint = circuit.runtime.checkpoint().run().unwrap();
    circuit.runtime.kill().unwrap();
    drop(circuit.factory);
    assert!(original_descriptor.upgrade().is_none());

    let mut restored_config = storage(directory.path());
    restored_config.storage.as_mut().unwrap().init_checkpoint = Some(checkpoint.uuid);
    let mut restored = AggregateCircuit::new(restored_config, sum_factory(), NullableSum::new);
    restored.step(vec![], vec![]);
    restored.step(
        vec![Tup2(Some(5), -1), Tup2(Some(-5), -1)],
        vec![Tup2(Tup1(Some(0)), -1), Tup2(Tup1(None), 1)],
    );
    restored.step(vec![Tup2(None, -1)], vec![]);
    restored.runtime.kill().unwrap();
    println!("descriptor nullable SUM: {frozen_steps} frozen steps and 5 restart checks");
}
