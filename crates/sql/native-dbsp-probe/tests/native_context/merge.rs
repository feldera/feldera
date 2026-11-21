use super::{NativeFactory, layer};
use dbsp::{
    DynZWeight,
    dynamic::{ClonableTrait, DynData, DynUnit, Erase},
    storage::{
        backend::{FileReader, StorageBackend},
        buffer_cache::BufferCache,
        file::{
            format::BatchMetadata,
            reader::{ArchivedLayerReader, ArchivedLayerRow, ArchivedRows},
            writer::{Parameters, write_layer_with_factory},
        },
    },
    trace::{MergeCursor, MergeOutput, merge_untimed_cursors},
};
use feldera_types::config::{StorageConfig, StorageOptions};
use std::{collections::BTreeMap, sync::Arc};

pub(super) type Rows = Vec<(Box<DynData>, Box<DynData>)>;

pub(super) struct LayerCursor<'a> {
    row: Option<ArchivedLayerRow<'a>>,
    factory: &'a layer::NativeLayerFactory,
    key: Box<DynData>,
    weight: i64,
    val_valid: bool,
}

impl<'a> LayerCursor<'a> {
    pub(super) fn new(rows: ArchivedRows<'a>, factory: &'a layer::NativeLayerFactory) -> Self {
        let mut cursor = Self {
            row: rows.nth(0).unwrap(),
            factory,
            key: factory.key().default_box(),
            weight: 0,
            val_valid: false,
        };
        cursor.load();
        cursor
    }

    fn load(&mut self) {
        self.val_valid = self.row.is_some();
        if let Some(row) = &self.row {
            let (block, offset) = row.item();
            let item = self.factory.item();
            self.factory
                .key()
                .archived_in_block(block.clone(), offset + item.key_offset)
                .unwrap()
                .as_archived()
                .deserialize(self.key.as_mut());
            self.weight = decode(self.factory.aux(), block, offset + item.aux_offset)[0].unwrap();
        }
    }
}

impl MergeCursor<DynData, DynUnit, (), DynZWeight> for LayerCursor<'_> {
    fn key_valid(&self) -> bool {
        self.row.is_some()
    }

    fn val_valid(&self) -> bool {
        self.val_valid
    }

    fn key(&self) -> &DynData {
        self.key.as_ref()
    }

    fn val(&self) -> &DynUnit {
        ().erase()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&(), &DynZWeight)) {
        if self.val_valid {
            logic(&(), self.weight.erase());
        }
    }

    fn weight(&mut self) -> &DynZWeight {
        self.weight.erase()
    }

    fn step_key(&mut self) {
        self.row = self
            .row
            .take()
            .and_then(|mut row| row.advance().unwrap().then_some(row));
        self.load();
    }

    fn step_val(&mut self) {
        self.val_valid = false;
    }
}

struct MergeRows<'a> {
    factory: &'a layer::NativeLayerFactory,
    rows: Rows,
    weight: Option<i64>,
}

impl MergeOutput<DynData, DynUnit, (), DynZWeight> for MergeRows<'_> {
    fn push_key(&mut self, key: &DynData) {
        let mut owned = self.factory.key().default_box();
        key.clone_to(owned.as_mut());
        let weight = self.weight.take().unwrap();
        self.rows.push((
            owned,
            self.factory.aux().row(vec![Some(i128::from(weight))]),
        ));
    }

    fn push_val(&mut self, _value: &DynUnit) {
        assert!(self.weight.is_some());
    }

    fn push_time_diff(&mut self, _time: &(), weight: &DynZWeight) {
        assert!(self.weight.replace(**weight).is_none());
    }
}

fn decode(
    factory: &NativeFactory,
    block: Arc<dbsp::storage::buffer_cache::FBuf>,
    position: usize,
) -> Vec<Option<i64>> {
    factory
        .archived_in_block(block, position)
        .unwrap()
        .values()
        .into_iter()
        .map(|value| value.map(|slot| i64::try_from(slot).unwrap()))
        .collect()
}

fn factory() -> Arc<layer::NativeLayerFactory> {
    Arc::new(layer::NativeLayerFactory::integer_pair())
}

pub(super) fn cache() -> Option<Arc<BufferCache>> {
    thread_local! {
        static CACHE: Arc<BufferCache> = Arc::new(BufferCache::new(4 * 1024 * 1024));
    }
    Some(CACHE.with(Arc::clone))
}

pub(super) fn write_rows<'a>(
    backend: &dyn StorageBackend,
    factory: Arc<layer::NativeLayerFactory>,
    rows: impl IntoIterator<Item = &'a (Box<DynData>, Box<DynData>)>,
) -> Arc<dyn FileReader> {
    write_layer_with_factory(
        factory,
        rows.into_iter()
            .map(|(key, weight)| (key.as_ref(), weight.as_ref())),
        cache,
        backend,
        Parameters::default().with_min_key_data_block(4096),
        BatchMetadata::default(),
    )
    .unwrap()
}

pub(super) fn compact(factory: &layer::NativeLayerFactory, layers: &[ArchivedLayerReader]) -> Rows {
    let cursors = layers
        .iter()
        .map(|reader| LayerCursor::new(reader.rows(), factory))
        .collect();
    let mut output = MergeRows {
        factory,
        rows: Vec::new(),
        weight: None,
    };
    merge_untimed_cursors(cursors, Box::new(0_i64) as Box<DynZWeight>, &mut output);
    output.rows
}

#[test]
fn descriptor_owned_layer_merge_compacts_signed_updates() {
    let directory = tempfile::tempdir().unwrap();
    let backend = <dyn StorageBackend>::new(
        &StorageConfig {
            path: directory.path().to_string_lossy().into_owned(),
            cache: Default::default(),
        },
        &StorageOptions::default(),
    )
    .unwrap();
    let write = |factory, rows: &Rows| {
        let file = write_rows(backend.as_ref(), factory, rows);
        file.mark_for_checkpoint();
        file.path().clone()
    };

    let writer = factory();
    let writer_key = Arc::downgrade(&writer.key().layout);
    let mut expected = BTreeMap::<i64, i64>::new();
    let mut inputs = Vec::new();
    for entries in [
        (-256..256).map(|key| (key, 1_i64)).collect::<Vec<_>>(),
        (-128..384)
            .map(|key| (key, if key % 2 == 0 { -1 } else { 2 }))
            .collect(),
    ] {
        let rows = entries
            .into_iter()
            .map(|(key, weight)| {
                *expected.entry(key).or_default() += weight;
                (
                    writer
                        .key()
                        .row(vec![Some(i128::from(key)), Some(i128::from(3 * key))]),
                    writer.aux().row(vec![Some(i128::from(weight))]),
                )
            })
            .collect::<Vec<_>>();
        inputs.push(write(writer.clone(), &rows));
    }
    drop(writer);
    assert!(writer_key.upgrade().is_none());
    expected.retain(|_, weight| *weight != 0);

    let merger = factory();
    let merger_key = Arc::downgrade(&merger.key().layout);
    let inputs = inputs
        .iter()
        .map(|path| ArchivedLayerReader::open(cache, backend.as_ref(), path, 1).unwrap())
        .collect::<Vec<_>>();
    let output = compact(&merger, &inputs);
    assert_eq!(output.len(), expected.len());
    let checkpoint = write(merger.clone(), &output);
    drop(output);
    drop(inputs);
    drop(merger);
    assert!(merger_key.upgrade().is_none());

    let reader = ArchivedLayerReader::open(cache, backend.as_ref(), &checkpoint, 1).unwrap();
    let rows = reader.rows();
    let restored = factory();
    let mut actual = BTreeMap::new();
    for row in rows.range() {
        let (block, offset) = rows.nth(row).unwrap().unwrap().item();
        let key = decode(restored.key(), block.clone(), offset + layer::KEY_OFFSET);
        let key_value = key[0].unwrap();
        assert_eq!(key[1], Some(3 * key_value));
        let weight = decode(restored.aux(), block, offset + layer::AUX_OFFSET)[0].unwrap();
        assert!(actual.insert(key_value, weight).is_none());
    }
    assert_eq!(actual, expected);
    println!(
        "descriptor merge produced {} nonzero rows from 1024 updates",
        actual.len()
    );
}
