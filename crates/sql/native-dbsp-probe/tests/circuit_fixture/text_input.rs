use dbsp::utils::{Tup1, Tup2};
use feldera_native_dbsp_probe::StringRow;
use feldera_native_probe::{CompiledStringArchive, StringArchiveLayout, StringArchives};
use feldera_sqllib::SqlString;
use rkyv::Archived;
use std::sync::{Arc, Weak};

pub(super) fn text_archive() -> CompiledStringArchive {
    type Archive = Archived<StringRow>;
    let sample = rkyv::to_bytes::<_, 256>(&Tup1(Some(SqlString::from_ref("x")))).unwrap();
    // SAFETY: the aligned bytes were just serialized for exactly StringRow.
    let root = unsafe { rkyv::archived_root::<StringRow>(&sample) };
    let header =
        std::ptr::from_ref(root.0.as_ref().unwrap()) as usize - std::ptr::from_ref(root) as usize;
    let layout = StringArchiveLayout::new(
        size_of::<Archive>(),
        header,
        Some(std::mem::offset_of!(Archive, 0)),
    )
    .unwrap();
    CompiledStringArchive::compile(layout).unwrap()
}

pub(super) struct ArchivedStringBatch {
    layout: Arc<CompiledStringArchive>,
    archive: StringArchives,
    weights: Vec<i64>,
}

impl std::fmt::Debug for ArchivedStringBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArchivedStringBatch")
            .field("rows", &self.weights.len())
            .finish()
    }
}

impl ArchivedStringBatch {
    pub(super) fn new(rows: &[Tup2<StringRow, i64>]) -> Self {
        let strings: Vec<_> = rows
            .iter()
            .map(|Tup2(row, _)| row.0.as_ref().map(SqlString::str))
            .collect();
        let layout = Arc::new(text_archive());
        let archive = layout.encode(&strings).unwrap();
        let weights = rows.iter().map(|Tup2(_, weight)| *weight).collect();
        Self {
            layout,
            archive,
            weights,
        }
    }

    pub(super) fn owner(&self) -> Weak<CompiledStringArchive> {
        Arc::downgrade(&self.layout)
    }

    pub(super) fn values(&self) -> impl Iterator<Item = (Option<&str>, i64)> + '_ {
        self.archive
            .rows()
            .zip(&self.weights)
            .map(|((bytes, root), weight)| {
                (self.layout.decode_single(bytes, root).unwrap(), *weight)
            })
    }
}
