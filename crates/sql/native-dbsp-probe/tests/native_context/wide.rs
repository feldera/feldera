use super::{Layout, NativeFactory, hash, layer, single};
use dbsp::{
    dynamic::DynData,
    storage::{
        backend::StorageBackend,
        buffer_cache::BufferCache,
        file::{
            Factories,
            format::BatchMetadata,
            reader::ArchivedLayerReader,
            writer::{Parameters, Writer1, write_layer_with_factory},
        },
    },
    utils::Tup1,
};
use feldera_fxp::Fixed;
use feldera_native_probe::{ArchiveField, ArchiveInteger};
use feldera_types::config::{StorageConfig, StorageOptions};
use rkyv::{Archived, option::ArchivedOption};
use std::sync::Arc;

type Decimal = Fixed<38, 2>;
type Key = Tup1<Option<Decimal>>;
type Aux = Tup1<i64>;

fn decimal() -> NativeFactory {
    type Archive = Archived<Key>;
    let sample = ArchivedOption::Some(Decimal::ZERO);
    let ArchivedOption::Some(payload) = &sample else {
        unreachable!()
    };
    let delta = std::ptr::from_ref(payload) as usize - std::ptr::from_ref(&sample) as usize;
    let tag = std::mem::offset_of!(Archive, 0);
    NativeFactory {
        layout: Layout::compile(
            4,
            size_of::<Archive>(),
            align_of::<Archive>(),
            vec![ArchiveField {
                integer: ArchiveInteger::I128,
                payload_offset: tag + delta,
                null_tag_offset: Some(tag),
            }],
        ),
    }
}

fn cache() -> Option<Arc<BufferCache>> {
    thread_local! {
        static CACHE: Arc<BufferCache> = Arc::new(BufferCache::new(4 * 1024 * 1024));
    }
    Some(CACHE.with(Arc::clone))
}

#[test]
fn nullable_decimal_layer_roundtrips_wide_mantissas() {
    const ITEM: layer::ItemLayout = layer::ItemLayout::for_rkyv::<Key, Aux>();
    let directory = tempfile::tempdir().unwrap();
    let backend = <dyn StorageBackend>::new(
        &StorageConfig {
            path: directory.path().to_string_lossy().into_owned(),
            cache: Default::default(),
        },
        &StorageOptions::default(),
    )
    .unwrap();
    let parameters = || Parameters::default().with_min_key_data_block(4096);
    let factories = Factories::<DynData, DynData>::new::<Key, Aux>();
    let mut classic =
        Writer1::new(&factories, cache, backend.as_ref(), parameters(), None).unwrap();
    let native = Arc::new(
        // SAFETY: decimal and single encode the Key and Aux types used by ITEM.
        unsafe { layer::NativeLayerFactory::new(decimal(), single(), ITEM) },
    );
    let owner = Arc::downgrade(&native.key().layout);

    let mut rows = Vec::new();
    let decimals = std::iter::once(None)
        .chain((-256_i64..256).map(|position| Some(Decimal::for_i64(position * (i64::MAX / 256)))));
    for (i, value) in decimals.enumerate() {
        let (key, aux) = (Tup1(value), Tup1(i as i64 - 512));
        classic.write0((&key, &aux)).unwrap();
        let descriptor_key = native.key().row(vec![value.map(|value| value.mantissa())]);
        assert_eq!(descriptor_key.default_hash(), hash(&key));
        rows.push((descriptor_key, native.aux().row(vec![Some(aux.0.into())])));
    }
    let (classic_file, _, _) = classic.close(BatchMetadata::default()).unwrap();
    let file = write_layer_with_factory(
        native.clone(),
        rows.iter().map(|(key, aux)| (key.as_ref(), aux.as_ref())),
        cache,
        backend.as_ref(),
        parameters(),
        BatchMetadata::default(),
    )
    .unwrap();
    assert_eq!(
        backend.read(classic_file.path()).unwrap().as_slice(),
        backend.read(file.path()).unwrap().as_slice()
    );
    drop((rows, native));
    assert!(owner.upgrade().is_none());
    let path = file.path().clone();
    file.mark_for_checkpoint();
    drop(file);

    let reader = ArchivedLayerReader::open(cache, backend.as_ref(), &path, 1).unwrap();
    let group = reader.rows();
    let key_factory = decimal();
    let mut bounds = 0;
    assert_eq!(group.range(), 0..513);
    for (expected_row, value) in [
        (0, None),
        (1, Some(Decimal::for_i64(-256 * (i64::MAX / 256)))),
        (257, Some(Decimal::ZERO)),
        (512, Some(Decimal::for_i64(255 * (i64::MAX / 256)))),
    ] {
        let expected = value.map(|value| value.mantissa());
        let target = key_factory.row(vec![expected]);
        let found = group
            .seek(|probe| {
                let offset = if probe.is_index_bound() {
                    bounds += 1;
                    probe.offset()
                } else {
                    probe.offset() + ITEM.key_offset
                };
                let key = key_factory
                    .archived_in_block(probe.block(), offset)
                    .unwrap();
                Ok(key.as_archived().cmp_target(target.as_ref()).reverse())
            })
            .unwrap()
            .unwrap();
        assert_eq!(found.row_number(), expected_row);
        let (block, offset) = found.item();
        let key = key_factory
            .archived_in_block(block, offset + ITEM.key_offset)
            .unwrap();
        assert_eq!(key.values(), vec![expected]);
    }
    assert!(bounds > 0);
    println!("decimal layer: 513 rows, 513 live hash comparisons, {bounds} index probes");
}
