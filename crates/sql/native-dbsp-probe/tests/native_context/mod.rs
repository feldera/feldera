//! Isolated factory API experiment. Archived views own their shared storage and
//! descriptor; this deliberately does not implement the existing borrowed Factory API.
use dbsp::{
    dynamic::{
        Clonable, Data, DeserializableDyn, DeserializeDyn, DowncastTrait, DynData, SerializeDyn,
    },
    storage::buffer_cache::FBuf,
    trace::{DbspSerializer, Deserializer},
    utils::SupportsRoaring,
};
use feldera_native_probe::{ArchiveField, ArchiveInteger, ArchiveLayout, CompiledArchive};
use rkyv::{Fallible, ser::Serializer};
use size_of::{Context, SizeOf};
use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};
use xxhash_rust::xxh3::Xxh3Default;

pub struct Layout {
    pub id: u64,
    fields: Vec<ArchiveField>,
    size: usize,
    alignment: usize,
    encoder: CompiledArchive,
}
impl fmt::Debug for Layout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Layout")
            .field("id", &self.id)
            .field("fields", &self.fields)
            .finish()
    }
}
impl SizeOf for Layout {
    fn size_of_children(&self, context: &mut Context) {
        context
            .add_vectorlike(
                self.fields.len(),
                self.fields.capacity(),
                std::mem::size_of::<ArchiveField>(),
            )
            .add_distinct_allocation();
    }
}
impl Layout {
    pub fn compile(id: u64, size: usize, alignment: usize, fields: Vec<ArchiveField>) -> Arc<Self> {
        assert!(alignment.is_power_of_two());
        assert!(
            fields.iter().all(|field| matches!(
                field.integer,
                ArchiveInteger::I32 | ArchiveInteger::I64 | ArchiveInteger::I128
            )),
            "the signed context test accepts INTEGER, BIGINT and DECIMAL mantissas"
        );
        let encoder =
            CompiledArchive::compile(ArchiveLayout::new(size, fields.clone()).unwrap()).unwrap();
        Arc::new(Self {
            id,
            fields,
            size,
            alignment,
            encoder,
        })
    }
    fn defaults(&self) -> Vec<Option<i128>> {
        self.fields
            .iter()
            .map(|field| {
                if field.null_tag_offset.is_some() {
                    None
                } else {
                    Some(0)
                }
            })
            .collect()
    }
    fn encode(&self, values: &[Option<i128>]) -> Vec<u8> {
        let mut row = vec![0; self.size];
        self.encoder.encode_wide_row_into(values, &mut row).unwrap();
        row
    }
    fn decode(&self, bytes: &[u8], position: usize) -> Result<Vec<Option<i128>>, &'static str> {
        let end = position
            .checked_add(self.size)
            .ok_or("archive offset overflow")?;
        let row = bytes
            .get(position..end)
            .ok_or("archive row outside storage")?;
        self.encoder
            .decode_row(row)
            .map_err(|_| "invalid fixed-width archive")
    }
    fn hash(&self, values: &[Option<i128>], hasher: &mut dyn Hasher) {
        for (field, value) in self.fields.iter().zip(values) {
            let optional = field.null_tag_offset.is_some();
            match field.integer {
                ArchiveInteger::I32 => {
                    hash_value(value.map(|n| i32::try_from(n).unwrap()), optional, hasher);
                }
                ArchiveInteger::I64 => {
                    hash_value(value.map(|n| i64::try_from(n).unwrap()), optional, hasher);
                }
                ArchiveInteger::I128 => hash_value(*value, optional, hasher),
                ArchiveInteger::Bool | ArchiveInteger::U32 | ArchiveInteger::U64 => {
                    unreachable!("signed context layouts are validated before use");
                }
            }
        }
    }
}

fn hash_value<T: Hash>(value: Option<T>, optional: bool, hasher: &mut dyn Hasher) {
    let mut hasher = hasher;
    if optional {
        value.hash(&mut hasher);
    } else {
        value.unwrap().hash(&mut hasher);
    }
}

#[derive(Clone, Debug)]
pub struct NativeRow {
    pub layout: Arc<Layout>,
    pub values: Vec<Option<i128>>,
}
impl NativeRow {
    fn same_layout(&self, other: &Self) {
        assert!(
            Arc::ptr_eq(&self.layout, &other.layout),
            "different native descriptors"
        );
    }
}
impl PartialEq for NativeRow {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for NativeRow {}
impl PartialOrd for NativeRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for NativeRow {
    fn cmp(&self, other: &Self) -> Ordering {
        self.same_layout(other);
        self.values.cmp(&other.values)
    }
}
impl SupportsRoaring for NativeRow {}
impl SizeOf for NativeRow {
    fn size_of_children(&self, context: &mut Context) {
        self.values.size_of_children(context);
        self.layout.size_of_children(context);
    }
}
impl Clonable for NativeRow {
    unsafe fn raw_clone_to(&self, dst: *mut u8) {
        // SAFETY: Clonable's caller guarantees a valid, exclusive NativeRow target.
        let target = unsafe { &mut *dst.cast::<NativeRow>() };
        self.same_layout(target);
        target.values.clone_from(&self.values);
    }
    unsafe fn raw_move_to(&mut self, dst: *mut u8) {
        // SAFETY: Clonable's caller guarantees a valid, exclusive NativeRow target.
        let target = unsafe { &mut *dst.cast::<NativeRow>() };
        self.same_layout(target);
        target.values = std::mem::replace(&mut self.values, self.layout.defaults());
    }
    unsafe fn raw_clone_from(&mut self, src: *const u8) {
        // SAFETY: Clonable's caller guarantees a valid NativeRow source.
        let source = unsafe { &*src.cast::<NativeRow>() };
        self.same_layout(source);
        self.values.clone_from(&source.values);
    }
}
impl SerializeDyn for NativeRow {
    fn serialize(
        &self,
        serializer: &mut DbspSerializer,
    ) -> Result<usize, <DbspSerializer<'_> as Fallible>::Error> {
        let bytes = self.layout.encode(&self.values);
        serializer.align(self.layout.alignment)?;
        let position = serializer.pos();
        serializer.write(&bytes)?;
        Ok(position)
    }
}
impl DeserializableDyn for NativeRow {
    unsafe fn deserialize_from_bytes_with(
        &mut self,
        bytes: &[u8],
        pos: usize,
        _deserializer: &mut Deserializer,
    ) {
        self.values = self
            .layout
            .decode(bytes, pos)
            .expect("invalid native archive");
    }
}
impl Data for NativeRow {
    fn default_hash(&self) -> u64 {
        let mut state = Xxh3Default::default();
        self.dyn_hash(&mut state);
        state.finish()
    }
    fn dyn_hash(&self, hasher: &mut dyn Hasher) {
        self.layout.hash(&self.values, hasher);
    }
    fn as_data(&self) -> &dyn Data {
        self
    }
    fn as_data_mut(&mut self) -> &mut dyn Data {
        self
    }
}

/// Owns storage rather than returning a reference tied only to its bytes.
/// Arc retention is the proposed engine API boundary, not a stored pointer.
#[derive(Clone, Debug)]
pub struct ArchivedView {
    layout: Arc<Layout>,
    storage: Storage,
    position: usize,
}

#[derive(Clone, Debug)]
enum Storage {
    Bytes(Arc<[u8]>),
    Block(Arc<FBuf>),
}

impl Storage {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Bytes(bytes) => bytes,
            Self::Block(block) => block.as_slice(),
        }
    }
}

impl ArchivedView {
    pub fn as_archived(&self) -> &<DynData as dbsp::dynamic::ArchiveTrait>::Archived {
        self
    }
    pub(super) fn values(&self) -> Vec<Option<i128>> {
        self.layout
            .decode(self.storage.bytes(), self.position)
            .unwrap()
    }
}
impl PartialEq for ArchivedView {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for ArchivedView {}
impl PartialOrd for ArchivedView {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for ArchivedView {
    fn cmp(&self, other: &Self) -> Ordering {
        assert!(
            Arc::ptr_eq(&self.layout, &other.layout),
            "different native descriptors"
        );
        self.values().cmp(&other.values())
    }
}
impl DeserializeDyn<DynData> for ArchivedView {
    fn deserialize_with(&self, target: &mut DynData, _deserializer: &mut Deserializer) {
        let target = target.downcast_mut_checked::<NativeRow>();
        assert!(
            Arc::ptr_eq(&self.layout, &target.layout),
            "different native descriptors"
        );
        target.values = self.values();
    }
    fn eq_target(&self, target: &DynData) -> bool {
        self.cmp_target(target) == Ordering::Equal
    }
    fn cmp_target(&self, target: &DynData) -> Ordering {
        let target = target.downcast_checked::<NativeRow>();
        assert!(
            Arc::ptr_eq(&self.layout, &target.layout),
            "different native descriptors"
        );
        self.values().cmp(&target.values)
    }
}

#[derive(Clone)]
pub struct NativeFactory {
    pub layout: Arc<Layout>,
}
impl NativeFactory {
    pub fn default_box(&self) -> Box<DynData> {
        Box::new(NativeRow {
            layout: self.layout.clone(),
            values: self.layout.defaults(),
        })
    }
    pub fn row(&self, values: Vec<Option<i128>>) -> Box<DynData> {
        self.layout.encode(&values); // reject width, nullability and integer overflow
        Box::new(NativeRow {
            layout: self.layout.clone(),
            values,
        })
    }
    pub fn archive(&self, row: &DynData) -> Arc<[u8]> {
        let row = row.downcast_checked::<NativeRow>();
        assert!(
            Arc::ptr_eq(&self.layout, &row.layout),
            "different native descriptors"
        );
        self.layout.encode(&row.values).into()
    }
    pub fn archived(
        &self,
        storage: Arc<[u8]>,
        position: usize,
    ) -> Result<ArchivedView, &'static str> {
        self.archived_owned(Storage::Bytes(storage), position)
    }

    pub fn archived_in_block(
        &self,
        storage: Arc<FBuf>,
        position: usize,
    ) -> Result<ArchivedView, &'static str> {
        self.archived_owned(Storage::Block(storage), position)
    }

    fn archived_owned(
        &self,
        storage: Storage,
        position: usize,
    ) -> Result<ArchivedView, &'static str> {
        self.layout.decode(storage.bytes(), position)?;
        Ok(ArchivedView {
            layout: self.layout.clone(),
            storage,
            position,
        })
    }
}
