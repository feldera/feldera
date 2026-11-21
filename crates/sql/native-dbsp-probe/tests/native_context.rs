//! Real DBSP dynamic traits with a new owned-view factory boundary, not a claim
//! that unchanged DBSP batches can yet consume these rows or restore checkpoints.
#![cfg(target_os = "linux")]
#[path = "native_context/aggregate_circuit.rs"]
mod aggregate_circuit;
#[path = "circuit_fixture/aggregates.rs"]
mod aggregate_fixture;
mod circuit_fixture;
#[path = "native_context/mod.rs"]
mod context;
#[path = "native_context/layer.rs"]
mod layer;
#[path = "native_context/merge.rs"]
mod merge;
#[path = "native_context/wide.rs"]
mod wide;
use context::{ArchivedView, Layout, NativeFactory, NativeRow};
use dbsp::{
    dynamic::{ArchiveTrait, ClonableTrait, DynData},
    storage::file::ArchivedItem,
    utils::{Tup1, Tup2},
};
use feldera_native_probe::{ArchiveField, ArchiveInteger};
use rkyv::Archived;
use std::{
    hash::{Hash, Hasher},
    mem::{align_of, offset_of, size_of},
    sync::Arc,
};
use xxhash_rust::xxh3::Xxh3Default;

type ArchivedData = <DynData as ArchiveTrait>::Archived;

struct NativeItem {
    key: ArchivedView,
    aux: ArchivedView,
}

impl<'a> ArchivedItem<'a, DynData, DynData> for NativeItem {
    fn fst(&self) -> &ArchivedData {
        self.key.as_archived()
    }

    fn snd(&self) -> &ArchivedData {
        self.aux.as_archived()
    }

    fn split(&self) -> (&ArchivedData, &ArchivedData) {
        (self.fst(), self.snd())
    }

    fn equal(&self, other: &dyn ArchivedItem<'a, DynData, DynData>) -> bool {
        let (key, aux) = other.split();
        (self.fst(), self.snd()) == (key, aux)
    }
}

#[test]
fn reopened_layer_items_and_index_bounds_retain_blocks_and_layouts() {
    use dbsp::storage::{
        backend::StorageBackend,
        buffer_cache::BufferCache,
        file::{
            Factories,
            format::BatchMetadata,
            reader::{ArchivedSeekKey, Error, Reader},
            writer::{Parameters, Writer1, write_layer_with_factory},
        },
    };
    use feldera_types::config::{StorageConfig, StorageOptions};

    thread_local! {
        static CACHE: std::cell::RefCell<Option<Arc<BufferCache>>> =
            const { std::cell::RefCell::new(None) };
    }
    fn cache() -> Option<Arc<BufferCache>> {
        CACHE.with_borrow(Clone::clone)
    }
    let fresh_cache = || {
        CACHE.with_borrow_mut(|cache| {
            *cache = Some(Arc::new(BufferCache::new(1024 * 1024)));
        })
    };

    let directory = tempfile::tempdir().unwrap();
    let backend = <dyn StorageBackend>::new(
        &StorageConfig {
            path: directory.path().to_string_lossy().into_owned(),
            cache: Default::default(),
        },
        &StorageOptions::default(),
    )
    .unwrap();
    fresh_cache();
    let factories = Factories::<DynData, DynData>::new::<Tup2<i32, i64>, Tup1<i64>>();
    let mut writer = Writer1::new(
        &factories,
        cache,
        backend.as_ref(),
        Parameters::default().with_min_key_data_block(4096),
        None,
    )
    .unwrap();
    let rows: Vec<_> = (-256..=256)
        .map(|value| (Tup2(value, i64::from(value) * 3), Tup1(-i64::from(value))))
        .collect();
    for (key, aux) in &rows {
        writer.write0((key, aux)).unwrap();
    }
    let path = writer.path().clone();
    let (file, _, _) = writer.close(BatchMetadata::default()).unwrap();
    file.mark_for_checkpoint();
    drop(file);

    let parameters = Parameters::default().with_min_key_data_block(4096);
    let layer_factory = Arc::new(layer::NativeLayerFactory::integer_pair());
    let owned_rows: Vec<_> = rows
        .iter()
        .map(|(key, aux)| {
            (
                layer_factory
                    .key()
                    .row(vec![Some(key.0.into()), Some(key.1.into())]),
                layer_factory.aux().row(vec![Some(aux.0.into())]),
            )
        })
        .collect();
    let native_file = write_layer_with_factory(
        layer_factory.clone(),
        owned_rows
            .iter()
            .map(|(key, aux)| (key.as_ref(), aux.as_ref())),
        cache,
        backend.as_ref(),
        parameters,
        BatchMetadata::default(),
    )
    .unwrap();
    let native_path = native_file.path().clone();
    let item_writes = layer_factory
        .items_written
        .load(std::sync::atomic::Ordering::Relaxed);
    let item_key_reads = layer_factory
        .item_keys_read
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(item_writes >= rows.len() && item_key_reads >= 4);
    assert_eq!(
        backend.read(&path).unwrap().as_slice(),
        backend.read(&native_path).unwrap().as_slice()
    );
    drop(owned_rows);
    let written_key = Arc::downgrade(&layer_factory.key().layout);
    let written_aux = Arc::downgrade(&layer_factory.aux().layout);
    drop(layer_factory);
    assert!(written_key.upgrade().is_none() && written_aux.upgrade().is_none());
    native_file.mark_for_checkpoint();
    drop(native_file);

    fresh_cache();
    let reader = Reader::<(&'static DynData, &'static DynData, ())>::open(
        &[&factories.any_factories()],
        cache,
        backend.as_ref(),
        &native_path,
    )
    .unwrap();
    let file_bytes = reader.byte_size().unwrap();
    assert!(file_bytes > 0);
    let key_factory = pair();
    let aux_factory = single();
    let key_owner = Arc::downgrade(&key_factory.layout);
    let aux_owner = Arc::downgrade(&aux_factory.layout);
    let mut items = Vec::new();
    let mut blocks = Vec::new();
    let mut bounds = Vec::new();
    let mut item_probes = 0;
    let mut index_probes = 0;
    {
        // SAFETY: the reader's factories match the writer's key and value types.
        let mut cursor = unsafe { reader.rows().first() }.unwrap();
        while let Some((block, item_offset)) = cursor.archived_item_block() {
            let key = key_factory
                .archived_in_block(block.clone(), item_offset + layer::KEY_OFFSET)
                .unwrap();
            let aux = aux_factory
                .archived_in_block(block.clone(), item_offset + layer::AUX_OFFSET)
                .unwrap();
            blocks.push(Arc::downgrade(&block));
            items.push(NativeItem { key, aux });
            // SAFETY: the cursor continues to traverse the same typed file.
            unsafe { cursor.move_next() }.unwrap();
        }
        for row in [0, 1, 255, 256, 512] {
            // SAFETY: the cursor moves within the same typed file.
            unsafe { cursor.move_to_row(row) }.unwrap();
            let index_path = cursor.archived_index_bound_blocks().unwrap();
            assert!(!index_path.is_empty());
            for boundary in index_path {
                assert!(boundary.rows.contains(&row));
                let endpoints = [boundary.rows.start, boundary.rows.end - 1];
                for (index, offset) in endpoints.into_iter().zip(boundary.key_offsets) {
                    let key = key_factory
                        .archived_in_block(boundary.block.clone(), offset)
                        .unwrap();
                    blocks.push(Arc::downgrade(&boundary.block));
                    bounds.push((index as usize, key));
                }
            }
        }

        let item_key_offset = layer::KEY_OFFSET;
        // SAFETY: the factories belong to the file's original key and aux types.
        let mut native = unsafe { reader.rows().before() };
        let mut classic = native.clone();
        for target in [
            Tup2(-200, -600),
            Tup2(-200, -601),
            Tup2(42, 127),
            Tup2(300, 0),
        ] {
            let search_key = key_factory.row(vec![Some(target.0.into()), Some(target.1.into())]);
            let mut compare = |probe: ArchivedSeekKey<'_>| {
                let offset = if probe.is_index_bound() {
                    index_probes += 1;
                    probe.offset()
                } else {
                    item_probes += 1;
                    probe.offset() + item_key_offset
                };
                let key = key_factory
                    .archived_in_block(probe.block(), offset)
                    .unwrap();
                Ok(key.as_archived().cmp_target(search_key.as_ref()).reverse())
            };
            // SAFETY: the cursor reads the same typed file that the writer produced.
            unsafe {
                classic.advance_to_value_or_larger(&target).unwrap();
                native
                    .advance_to_first_ge_with_locations(&mut compare)
                    .unwrap();
            }
            assert_eq!(native.absolute_position(), classic.absolute_position());
            assert!(native.key() == classic.key());

            let row = reader.rows().locate_first_with(&mut compare).unwrap();
            let expected_row = rows.iter().position(|(key, _)| key >= &target);
            assert_eq!(row.as_ref().map(|(row, _, _)| *row as usize), expected_row);
            if let Some((row, block, offset)) = row {
                let key = key_factory
                    .archived_in_block(block, offset + item_key_offset)
                    .unwrap();
                let expected = &rows[row as usize].0;
                let mut materialized = key_factory.default_box();
                key.as_archived().deserialize(materialized.as_mut());
                let expected =
                    key_factory.row(vec![Some(expected.0.into()), Some(expected.1.into())]);
                assert!(materialized == expected);
            }
        }
        assert!(item_probes > 0 && index_probes > 0);

        // SAFETY: the cursor's original factories still match the file.
        unsafe {
            cursor.move_to_row(12).unwrap();
            assert!(matches!(
                cursor.advance_to_first_ge_with_locations(|_| Err(Error::Unsupported)),
                Err(Error::Unsupported)
            ));
        }
        assert!(!cursor.has_value());
    }
    assert_eq!(items.len(), rows.len());
    drop(reader);
    drop(backend);
    drop(CACHE.with_borrow_mut(Option::take));
    drop(key_factory);
    drop(aux_factory);

    assert!(blocks.iter().all(|block| block.upgrade().is_some()));
    let key_factory = NativeFactory {
        layout: key_owner.upgrade().unwrap(),
    };
    let aux_factory = NativeFactory {
        layout: aux_owner.upgrade().unwrap(),
    };
    let mut key = key_factory.default_box();
    let mut aux = aux_factory.default_box();
    for (item, (expected_key, expected_aux)) in items.iter().zip(rows.iter().copied()) {
        let (archived_key, archived_aux) = item.split();
        archived_key.deserialize(key.as_mut());
        archived_aux.deserialize(aux.as_mut());
        assert_eq!(key.default_hash(), hash(&expected_key));
        assert_eq!(aux.default_hash(), hash(&expected_aux));
        let expected_key = key_factory.row(vec![
            Some(expected_key.0.into()),
            Some(expected_key.1.into()),
        ]);
        let expected_aux = aux_factory.row(vec![Some(expected_aux.0.into())]);
        assert!(key.as_ref() == expected_key.as_ref());
        assert!(aux.as_ref() == expected_aux.as_ref());
    }
    assert!(!items[0].equal(&items[1]));
    assert!(items.windows(2).all(|pair| pair[0].fst() < pair[1].fst()));
    for (index, key) in &bounds {
        assert!(key.as_archived() == items[*index].fst());
    }
    let bound_count = bounds.len();
    drop(bounds);
    drop(items);
    drop((key, aux, key_factory, aux_factory));
    assert!(blocks.iter().all(|block| block.upgrade().is_none()));
    assert!(key_owner.upgrade().is_none() && aux_owner.upgrade().is_none());
    println!(
        "file_bytes={file_bytes} owned_items={} owned_key_bounds={bound_count} item_probes={item_probes} index_probes={index_probes} item_writes={item_writes} item_key_reads={item_key_reads}",
        rows.len()
    );
}

#[test]
fn reopened_nested_layer_preserves_groups_and_descriptor_seeks() {
    use dbsp::{
        storage::{
            backend::StorageBackend,
            buffer_cache::BufferCache,
            file::{
                Factories,
                format::BatchMetadata,
                reader::{ArchivedLayerReader, Error, Reader},
                writer::{Parameters, Writer2, write_nested_layer_with_factories},
            },
        },
        utils::Tup2,
    };
    use feldera_types::config::{StorageConfig, StorageOptions};
    use std::sync::atomic::Ordering::Relaxed;

    fn cache() -> Option<Arc<BufferCache>> {
        thread_local! {
            static CACHE: Arc<BufferCache> = Arc::new(BufferCache::new(4 * 1024 * 1024));
        }
        Some(CACHE.with(Arc::clone))
    }

    type Column = (&'static DynData, &'static DynData, ());
    type Nested = (&'static DynData, &'static DynData, Column);
    let directory = tempfile::tempdir().unwrap();
    let backend = <dyn StorageBackend>::new(
        &StorageConfig {
            path: directory.path().to_string_lossy().into_owned(),
            cache: Default::default(),
        },
        &StorageOptions::default(),
    )
    .unwrap();
    let factories = Factories::<DynData, DynData>::new::<Tup2<i32, i64>, Tup1<i64>>();
    let parameters = || Parameters::default().with_min_key_data_block(4096);
    let mut classic = Writer2::new(
        &factories,
        &factories,
        cache,
        backend.as_ref(),
        parameters(),
        None,
    )
    .unwrap();
    let factory = || Arc::new(layer::NativeLayerFactory::integer_pair());
    let outer = factory();
    let inner = factory();
    let outer_owner = Arc::downgrade(&outer.key().layout);
    let inner_owner = Arc::downgrade(&inner.key().layout);
    let native_row =
        |factory: &layer::NativeLayerFactory, key: &Tup2<i32, i64>, aux: &Tup1<i64>| {
            (
                factory
                    .key()
                    .row(vec![Some(key.0.into()), Some(key.1.into())]),
                factory.aux().row(vec![Some(aux.0.into())]),
            )
        };

    let mut rows = Vec::new();
    let mut group_starts = Vec::new();
    let mut child_count = 0_u64;
    for parent in 0..153_i32 {
        group_starts.push(child_count);
        let mut children = Vec::new();
        for child in 0..(10 + parent % 31) {
            let (key, aux) = (Tup2(child, i64::from(parent)), Tup1(-i64::from(child)));
            classic.write1((&key, &aux)).unwrap();
            children.push(native_row(&inner, &key, &aux));
            child_count += 1;
        }
        let (key, aux) = (Tup2(parent, -i64::from(parent)), Tup1(i64::from(parent)));
        classic.write0((&key, &aux)).unwrap();
        rows.push((native_row(&outer, &key, &aux), children));
    }
    let (classic_file, _, _) = classic.close(BatchMetadata::default()).unwrap();
    let native_file = write_nested_layer_with_factories(
        outer.clone(),
        inner.clone(),
        rows.iter().map(|((key, aux), children)| {
            (
                (key.as_ref(), aux.as_ref()),
                children
                    .iter()
                    .map(|(key, aux)| (key.as_ref(), aux.as_ref())),
            )
        }),
        cache,
        backend.as_ref(),
        parameters(),
        BatchMetadata::default(),
    )
    .unwrap();
    assert!(outer.items_written.load(Relaxed) >= rows.len());
    assert!(inner.items_written.load(Relaxed) >= child_count as usize);
    assert!(inner.item_keys_read.load(Relaxed) > 0);
    assert_eq!(
        backend.read(classic_file.path()).unwrap().as_slice(),
        backend.read(native_file.path()).unwrap().as_slice()
    );
    drop((rows, outer, inner));
    assert!(outer_owner.upgrade().is_none() && inner_owner.upgrade().is_none());

    let path = native_file.path().clone();
    native_file.mark_for_checkpoint();
    drop(native_file);
    assert!(matches!(
        ArchivedLayerReader::open(cache, backend.as_ref(), &path, 1),
        Err(Error::WrongNumberOfColumns {
            actual: 2,
            expected: 1
        })
    ));
    let reader = ArchivedLayerReader::open(cache, backend.as_ref(), &path, 2).unwrap();
    let typed = Reader::<Nested>::open(
        &[&factories.any_factories(), &factories.any_factories()],
        cache,
        backend.as_ref(),
        &path,
    )
    .unwrap();
    assert_eq!(typed.n_rows(1), child_count);
    let parents = reader.rows();
    assert_eq!(parents.range(), 0..153);
    assert!(parents.nth(153).unwrap().is_none());
    let key_factory = pair();
    let aux_factory = single();
    let mut archive_probes = 0;
    for (parent, start) in group_starts.into_iter().enumerate() {
        let cursor = parents.nth(parent as u64).unwrap().unwrap();
        let classic_group = typed
            .rows()
            .nth(parent as u64)
            .unwrap()
            .next_column()
            .unwrap();
        let group = cursor.next_column().unwrap().unwrap();
        let len = 10 + parent as u64 % 31;
        assert_eq!(group.range(), start..start + len);
        assert_eq!(classic_group.len(), len);
        assert!(group.nth(len).unwrap().is_none());
        let mut end = group.nth(len - 1).unwrap().unwrap();
        assert!(!end.advance().unwrap());
        assert_eq!(end.row_number(), start + len - 1);
        for child in [0, len - 1, len] {
            let target = key_factory.row(vec![Some(i128::from(child)), Some(parent as i128)]);
            let found = group
                .seek(|candidate| {
                    archive_probes += 1;
                    let offset = candidate.offset()
                        + if candidate.is_index_bound() {
                            0
                        } else {
                            layer::KEY_OFFSET
                        };
                    let key = key_factory
                        .archived_in_block(candidate.block(), offset)
                        .unwrap();
                    Ok(key.as_archived().cmp_target(target.as_ref()).reverse())
                })
                .unwrap();
            if child == len {
                assert!(found.is_none());
                continue;
            }
            let row = found.unwrap();
            assert_eq!(row.row_number(), start + child);
            assert!(row.next_column().unwrap().is_none());
            let (block, offset) = row.item();
            let aux = aux_factory
                .archived_in_block(block, offset + layer::AUX_OFFSET)
                .unwrap();
            let expected = aux_factory.row(vec![Some(-i128::from(child))]);
            assert!(aux.as_archived().cmp_target(expected.as_ref()).is_eq());
        }
    }
    println!("nested-layer: 153 groups {child_count} child rows {archive_probes} archive probes");
}

fn required(integer: ArchiveInteger, offset: usize) -> ArchiveField {
    ArchiveField {
        integer,
        payload_offset: offset,
        null_tag_offset: None,
    }
}
fn single() -> NativeFactory {
    type A = Archived<Tup1<i64>>;
    NativeFactory {
        layout: Layout::compile(
            1,
            size_of::<A>(),
            align_of::<A>(),
            vec![required(ArchiveInteger::I64, offset_of!(A, 0))],
        ),
    }
}
fn pair() -> NativeFactory {
    type A = Archived<Tup2<i32, i64>>;
    NativeFactory {
        layout: Layout::compile(
            2,
            size_of::<A>(),
            align_of::<A>(),
            vec![
                required(ArchiveInteger::I32, offset_of!(A, 0)),
                required(ArchiveInteger::I64, offset_of!(A, 1)),
            ],
        ),
    }
}
fn hash(value: &impl Hash) -> u64 {
    let mut h = Xxh3Default::default();
    value.hash(&mut h);
    h.finish()
}

#[test]
fn native_data_clone_move_and_two_layouts_keep_context() {
    let first = single();
    let second = pair();
    let mut a = first.row(vec![Some(-7)]);
    let mut b = first.default_box();
    a.clone_to(b.as_mut());
    assert!(a.as_ref() == b.as_ref());
    let clone = a.clone();
    a.move_to(b.as_mut());
    assert_eq!(a.default_hash(), hash(&Tup1(0_i64)));
    assert_eq!(b.default_hash(), hash(&Tup1(-7_i64)));
    assert!(clone.as_ref() == b.as_ref());
    let pair = second.row(vec![Some(i32::MIN.into()), Some(i64::MAX.into())]);
    assert_eq!(pair.default_hash(), hash(&Tup2(i32::MIN, i64::MAX)));
    let mismatch = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        first.archive(pair.as_ref())
    }));
    assert!(mismatch.is_err());
    assert_eq!(pair.default_hash(), hash(&Tup2(i32::MIN, i64::MAX)));
}

#[test]
fn real_archived_traits_compare_decode_and_retain_storage_and_native_library() {
    let factory = pair();
    let weak = Arc::downgrade(&factory.layout);
    let rows = [
        (i32::MIN, i64::MIN),
        (-1, 9),
        (0, -1),
        (0, 0),
        (0, 0),
        (i32::MAX, i64::MAX),
    ];
    let live: Vec<_> = rows
        .iter()
        .map(|(a, b)| factory.row(vec![Some((*a).into()), Some((*b).into())]))
        .collect();
    let archived: Vec<_> = live
        .iter()
        .zip(rows)
        .map(|(value, (a, b))| {
            let bytes = factory.archive(value.as_ref());
            assert_eq!(
                bytes.as_ref(),
                rkyv::to_bytes::<_, 256>(&Tup2(a, b)).unwrap().as_slice()
            );
            factory.archived(bytes, 0).unwrap()
        })
        .collect();
    for (i, archive) in archived.iter().enumerate() {
        let erased: &<DynData as dbsp::dynamic::ArchiveTrait>::Archived = archive.as_archived();
        let mut decoded = factory.default_box();
        erased.deserialize(decoded.as_mut());
        assert_eq!(decoded.default_hash(), live[i].default_hash());
        assert!(erased.eq_target(live[i].as_ref()));
        for (j, other) in archived.iter().enumerate() {
            assert_eq!(erased.cmp(other.as_archived()), rows[i].cmp(&rows[j]));
            assert_eq!(erased.cmp_target(live[j].as_ref()), rows[i].cmp(&rows[j]));
        }
    }
    drop(live);
    drop(factory);
    assert!(
        weak.upgrade().is_some(),
        "archive owns descriptor and loaded encoder"
    );
    // Construct a fresh live target using only the view-retained descriptor,
    // deserialize, and invoke its native encoder after the original factory died.
    let recovered = NativeFactory {
        layout: weak.upgrade().unwrap(),
    };
    let mut row = recovered.default_box();
    archived[1].as_archived().deserialize(row.as_mut());
    assert_eq!(
        recovered.archive(row.as_ref()).as_ref(),
        rkyv::to_bytes::<_, 256>(&Tup2(-1_i32, 9_i64))
            .unwrap()
            .as_slice()
    );
    drop(row);
    drop(recovered);
    drop(archived);
    assert!(
        weak.upgrade().is_none(),
        "no leaked descriptor or permanent registry"
    );
}

#[test]
fn reference_archive_file_reopens_with_fresh_native_context() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("rows.bin");
    let values = [Tup2(-8_i32, i64::MIN), Tup2(0, 0), Tup2(i32::MAX, i64::MAX)];
    let bytes: Vec<_> = values
        .iter()
        .flat_map(|v| rkyv::to_bytes::<_, 256>(v).unwrap().to_vec())
        .collect();
    std::fs::write(&file, &bytes).unwrap();
    let factory = pair();
    let weak = Arc::downgrade(&factory.layout);
    drop(factory);
    assert!(weak.upgrade().is_none());
    let fresh = pair();
    let storage: Arc<[u8]> = std::fs::read(&file).unwrap().into();
    for (index, value) in values.iter().enumerate() {
        let view = fresh
            .archived(
                storage.clone(),
                index * size_of::<Archived<Tup2<i32, i64>>>(),
            )
            .unwrap();
        let mut row = fresh.default_box();
        view.as_archived().deserialize(row.as_mut());
        assert_eq!(row.default_hash(), hash(value));
        assert_eq!(
            fresh.archive(row.as_ref()).as_ref(),
            rkyv::to_bytes::<_, 256>(value).unwrap().as_slice()
        );
    }
    assert!(fresh.archived(storage.clone(), usize::MAX).is_err());
    assert!(fresh.archived(storage, bytes.len() - 1).is_err());
}

#[test]
fn optional_native_archive_orders_nulls_and_rejects_invalid_tags() {
    type A = Archived<Tup1<Option<i64>>>;
    let sample = rkyv::option::ArchivedOption::Some(0_i64);
    let rkyv::option::ArchivedOption::Some(value) = &sample else {
        unreachable!()
    };
    let payload = std::ptr::from_ref(value) as usize - std::ptr::from_ref(&sample) as usize;
    let tag = offset_of!(A, 0);
    let factory = NativeFactory {
        layout: Layout::compile(
            3,
            size_of::<A>(),
            align_of::<A>(),
            vec![ArchiveField {
                integer: ArchiveInteger::I64,
                payload_offset: tag + payload,
                null_tag_offset: Some(tag),
            }],
        ),
    };
    let values = [None, Some(i64::MIN), Some(0), Some(i64::MAX)];
    for left in values {
        let live = factory.row(vec![left.map(i128::from)]);
        let bytes = factory.archive(live.as_ref());
        assert_eq!(
            bytes.as_ref(),
            rkyv::to_bytes::<_, 256>(&Tup1(left)).unwrap().as_slice()
        );
        let archived = factory.archived(bytes.clone(), 0).unwrap();
        assert_eq!(live.default_hash(), hash(&Tup1(left)));
        for right in values {
            assert_eq!(
                archived
                    .as_archived()
                    .cmp_target(factory.row(vec![right.map(i128::from)]).as_ref()),
                left.cmp(&right)
            );
        }
        let mut corrupt = bytes.to_vec();
        corrupt[tag] = 2;
        assert!(factory.archived(corrupt.into(), 0).is_err());
    }
    // No row or decoder silently acquires a different descriptor through Default.
    let default = factory.default_box();
    assert_eq!(
        default.as_any().downcast_ref::<NativeRow>().unwrap().values,
        vec![None]
    );
}

#[test]
fn actual_dbsp_serializer_and_mutable_decoder_use_native_row_context() {
    use dbsp::storage::file::SerializerInner;
    use rkyv::ser::Serializer;
    let factory = pair();
    let row = factory.row(vec![Some(-123), Some(i64::MAX.into())]);
    let mut position = 0;
    let bytes = SerializerInner::to_fbuf_with_thread_local(|serializer| {
        serializer.write(&[99])?;
        position = row.serialize(serializer)?;
        Ok::<_, <dbsp::trace::DbspSerializer<'_> as rkyv::Fallible>::Error>(())
    });
    assert_eq!(position, align_of::<Archived<Tup2<i32, i64>>>());
    assert_eq!(
        &bytes[position..],
        rkyv::to_bytes::<_, 256>(&Tup2(-123_i32, i64::MAX))
            .unwrap()
            .as_slice()
    );
    let mut decoded = factory.default_box();
    // SAFETY: the exact descriptor serialized this row at the retained position;
    // the byte comparison above also establishes the reference archive contract.
    unsafe {
        decoded.deserialize_from_bytes(&bytes, position);
    }
    assert!(row.as_ref() == decoded.as_ref());
}
