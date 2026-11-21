//! Compatibility tests for files written with a runtime factory.

use super::{
    Factories, Item, RefTup2,
    format::BatchMetadata,
    reader::{ArchivedLayerReader, ArchivedSeekKey, Error},
    writer::{
        LayerItemFactory, LayerKeyFactory, Parameters, Writer1, write_layer_with_factory,
        write_nested_layer_with_factories,
    },
};
use crate::{
    DynZWeight, Runtime,
    dynamic::{DowncastTrait, DynData, DynUnit},
    trace::{
        Batch, BatchReader, BatchReaderFactories, Cursor, FileWSet, FileWSetFactories, MergeOutput,
        merge_untimed_cursors, test::run_in_circuit_with_storage,
    },
};
use feldera_storage::FileReader;
use std::{cmp::Ordering, sync::Arc};

struct I64Factory;

impl LayerKeyFactory<DynData> for I64Factory {
    fn default_key(&self) -> Box<DynData> {
        Box::new(0i64)
    }
}

// SAFETY: the factory uses the typed serializer for both fields and the scratch key.
unsafe impl LayerItemFactory<DynData, DynZWeight> for I64Factory {
    fn with_item<'a>(
        &self,
        key: &'a DynData,
        aux: &'a DynZWeight,
        write: &mut dyn FnMut(&dyn Item<'a, DynData, DynZWeight>),
    ) {
        write(&RefTup2(key.downcast_checked::<i64>(), &**aux));
    }

    unsafe fn read_item_key(&self, bytes: &[u8], offset: usize, target: &mut DynData) {
        let item = rkyv::check_archived_value::<RefTup2<i64, i64>>(bytes, offset).unwrap();
        *target.downcast_mut_checked::<i64>() = item.0;
    }
}

fn parameters() -> Parameters {
    Parameters {
        min_branch: 2,
        max_branch: 4,
        ..Parameters::default()
    }
}

fn pair(row: &(i64, i64)) -> (&DynData, &DynZWeight) {
    (&row.0, &row.1)
}

fn write_factory(rows: &[(i64, i64)]) -> Arc<dyn FileReader> {
    write_layer_with_factory(
        Arc::new(I64Factory),
        rows.iter().map(pair),
        Runtime::buffer_cache,
        &*Runtime::storage_backend().unwrap(),
        parameters(),
        BatchMetadata::default(),
    )
    .unwrap()
}

fn open_batch(file: &dyn FileReader) -> FileWSet<DynData, DynZWeight> {
    FileWSet::from_path(&FileWSetFactories::new::<i64, (), i64>(), file.path()).unwrap()
}

fn current(cursor: &mut impl Cursor<DynData, DynUnit, (), DynZWeight>) -> Option<(i64, i64)> {
    cursor
        .key_valid()
        .then(|| (*cursor.key().downcast_checked::<i64>(), **cursor.weight()))
}

#[test]
fn factory_file_matches_typed_iteration_and_seeks() {
    run_in_circuit_with_storage(|| {
        let rows: Vec<_> = (0..40).map(|i| (2 * i, i - 50)).collect();
        let file = write_factory(&rows);
        let storage = Runtime::storage_backend().unwrap();
        let raw =
            ArchivedLayerReader::open(Runtime::buffer_cache, &*storage, file.path(), 1).unwrap();
        for (i, expected) in rows.iter().enumerate() {
            let (block, offset) = raw.rows().nth(i as u64).unwrap().unwrap().item();
            let item = rkyv::check_archived_value::<RefTup2<i64, i64>>(&block, offset).unwrap();
            assert_eq!((item.0, item.1), *expected);
        }

        let mut writer = Writer1::new(
            &Factories::<DynData, DynZWeight>::new::<i64, i64>(),
            Runtime::buffer_cache,
            &*storage,
            parameters(),
            None,
        )
        .unwrap();
        for row in &rows {
            writer.write0(pair(row)).unwrap();
        }
        let (reference, _, _) = writer.close(BatchMetadata::default()).unwrap();
        let actual = open_batch(&*file);
        let expected = open_batch(&*reference);
        let mut a = actual.cursor();
        let mut b = expected.cursor();
        for &row in &rows {
            assert_eq!(current(&mut a), Some(row));
            assert_eq!(current(&mut a), current(&mut b));
            a.step_key();
            b.step_key();
        }
        assert!(!a.key_valid() && !b.key_valid());
        a.rewind_keys();
        b.rewind_keys();
        for target in [-1i64, 2, 5, 31, 64, 78, 90] {
            a.seek_key(&target);
            b.seek_key(&target);
            assert_eq!(
                current(&mut a),
                rows.iter().copied().find(|(key, _)| *key >= target)
            );
            assert_eq!(current(&mut a), current(&mut b));
        }
        a.fast_forward_keys();
        b.fast_forward_keys();
        for target in [79i64, 64, 31, 6, 1, -1] {
            a.seek_key_reverse(&target);
            b.seek_key_reverse(&target);
            assert_eq!(
                current(&mut a),
                rows.iter().copied().rfind(|(key, _)| *key <= target)
            );
            assert_eq!(current(&mut a), current(&mut b));
        }
    });
}

fn compare(target: i64, probe: ArchivedSeekKey<'_>) -> Result<Ordering, Error> {
    let block = probe.block();
    let key = if probe.is_index_bound() {
        *rkyv::check_archived_value::<i64>(&block, probe.offset()).unwrap()
    } else {
        rkyv::check_archived_value::<RefTup2<i64, i64>>(&block, probe.offset())
            .unwrap()
            .0
    };
    Ok(target.cmp(&key))
}

#[test]
fn archived_seek_stays_in_the_selected_child_group() {
    run_in_circuit_with_storage(|| {
        let storage = Runtime::storage_backend().unwrap();
        let groups: Vec<_> = (0..8)
            .map(|i| ((2 * i, 1), [(3, i), (6, i + 10), (9, i + 20)]))
            .collect();
        let file = write_nested_layer_with_factories(
            Arc::new(I64Factory),
            Arc::new(I64Factory),
            groups
                .iter()
                .map(|(outer, inner)| (pair(outer), inner.iter().map(pair))),
            Runtime::buffer_cache,
            &*storage,
            parameters(),
            BatchMetadata::default(),
        )
        .unwrap();
        let raw =
            ArchivedLayerReader::open(Runtime::buffer_cache, &*storage, file.path(), 2).unwrap();
        let rows = raw.rows();
        assert!(rows.seek(|key| compare(15, key)).unwrap().is_none());
        let outer = rows.seek(|key| compare(5, key)).unwrap().unwrap();
        assert_eq!(outer.row_number(), 3);
        let inner = outer.next_column().unwrap().unwrap();
        assert_eq!(inner.range(), 9..12);
        let child = inner.seek(|key| compare(4, key)).unwrap().unwrap();
        assert_eq!(child.row_number(), 10);
        let (block, offset) = child.item();
        let item = rkyv::check_archived_value::<RefTup2<i64, i64>>(&block, offset).unwrap();
        assert_eq!((item.0, item.1), (6, 13));
        assert!(child.next_column().unwrap().is_none());
        assert!(inner.seek(|key| compare(10, key)).unwrap().is_none());
    });
}

#[test]
fn merge_factory_files_consolidates_signed_weights() {
    #[derive(Debug, PartialEq)]
    enum Update {
        Key(i64),
        Value,
        Weight(i64),
    }

    impl MergeOutput<DynData, DynUnit, (), DynZWeight> for Vec<Update> {
        fn push_key(&mut self, key: &DynData) {
            self.push(Update::Key(*key.downcast_checked::<i64>()));
        }

        fn push_val(&mut self, _: &DynUnit) {
            self.push(Update::Value);
        }

        fn push_time_diff(&mut self, _: &(), weight: &DynZWeight) {
            self.push(Update::Weight(**weight));
        }
    }

    run_in_circuit_with_storage(|| {
        let left = write_factory(&[(1, 2), (2, -3), (4, 7)]);
        let right = write_factory(&[(2, 3), (3, -1), (4, 2)]);
        let batches = [open_batch(&*left), open_batch(&*right)];
        let cursors = batches
            .iter()
            .map(|batch| batch.merge_cursor(None, None))
            .collect();
        let mut output = Vec::new();
        merge_untimed_cursors(cursors, Box::new(0i64) as Box<DynZWeight>, &mut output);
        use Update::{Key, Value, Weight};
        assert_eq!(
            output,
            [
                Weight(2),
                Value,
                Key(1),
                Weight(-1),
                Value,
                Key(3),
                Weight(9),
                Value,
                Key(4)
            ]
        );
    });
}
