//! Layer-file adapter for runtime-described fixed-width rows.

use super::NativeFactory;
use dbsp::{
    DBData,
    dynamic::{DynData, SerializeDyn},
    storage::file::{
        Item, RefTup2,
        writer::{LayerItemFactory, LayerKeyFactory},
    },
    trace::DbspSerializer,
    utils::{Tup1, Tup2},
};
use rkyv::{Archived, Fallible, ser::Serializer};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

pub const INTEGER_ITEM: ItemLayout = ItemLayout::for_rkyv::<Tup2<i32, i64>, Tup1<i64>>();

pub const KEY_OFFSET: usize = INTEGER_ITEM.key_offset;
pub const AUX_OFFSET: usize = INTEGER_ITEM.aux_offset;

#[derive(Clone, Copy)]
pub struct ItemLayout {
    pub key_offset: usize,
    pub aux_offset: usize,
    size: usize,
    alignment: usize,
}

impl ItemLayout {
    pub const fn for_rkyv<K: DBData, A: DBData>() -> Self {
        type ItemArchive<K, A> = Archived<RefTup2<'static, K, A>>;
        Self {
            key_offset: std::mem::offset_of!(ItemArchive<K, A>, 0),
            aux_offset: std::mem::offset_of!(ItemArchive<K, A>, 1),
            size: size_of::<ItemArchive<K, A>>(),
            alignment: align_of::<ItemArchive<K, A>>(),
        }
    }
}

pub struct NativeLayerFactory {
    key: NativeFactory,
    aux: NativeFactory,
    pub items_written: AtomicUsize,
    pub item_keys_read: AtomicUsize,
    item: ItemLayout,
}

impl NativeLayerFactory {
    /// # Safety
    ///
    /// The descriptors must encode the concrete key and auxiliary types used
    /// to build `item`. Both field archives must be self-contained.
    pub unsafe fn new(key: NativeFactory, aux: NativeFactory, item: ItemLayout) -> Self {
        Self {
            key,
            aux,
            items_written: AtomicUsize::new(0),
            item_keys_read: AtomicUsize::new(0),
            item,
        }
    }

    pub fn integer_pair() -> Self {
        // SAFETY: pair and single encode the key and auxiliary types of INTEGER_ITEM.
        unsafe { Self::new(super::pair(), super::single(), INTEGER_ITEM) }
    }

    pub fn key(&self) -> &NativeFactory {
        &self.key
    }

    pub fn aux(&self) -> &NativeFactory {
        &self.aux
    }

    pub fn item(&self) -> ItemLayout {
        self.item
    }
}

struct NativeLayerItem<'a, 'factory> {
    factory: &'factory NativeLayerFactory,
    key: &'a DynData,
    aux: &'a DynData,
}

impl SerializeDyn for NativeLayerItem<'_, '_> {
    fn serialize(
        &self,
        serializer: &mut DbspSerializer,
    ) -> Result<usize, <DbspSerializer<'_> as Fallible>::Error> {
        let key = self.factory.key.archive(self.key);
        let aux = self.factory.aux.archive(self.aux);
        let item = self.factory.item;
        let mut bytes = vec![0; item.size];
        bytes[item.key_offset..item.key_offset + key.len()].copy_from_slice(&key);
        bytes[item.aux_offset..item.aux_offset + aux.len()].copy_from_slice(&aux);
        serializer.align(item.alignment)?;
        let position = serializer.pos();
        serializer.write(&bytes)?;
        self.factory.items_written.fetch_add(1, Relaxed);
        Ok(position)
    }
}

impl<'a> Item<'a, DynData, DynData> for NativeLayerItem<'a, '_> {
    fn from_refs(&mut self, key: &'a DynData, aux: &'a DynData) {
        self.key = key;
        self.aux = aux;
    }
}

impl LayerKeyFactory<DynData> for NativeLayerFactory {
    fn default_key(&self) -> Box<DynData> {
        self.key.default_box()
    }
}

// SAFETY: new requires matching field descriptors. They stay paired with
// the typed RefTup2 layout for the lifetime of this factory.
unsafe impl LayerItemFactory<DynData, DynData> for NativeLayerFactory {
    fn with_item<'a>(
        &self,
        key: &'a DynData,
        aux: &'a DynData,
        write: &mut dyn FnMut(&dyn Item<'a, DynData, DynData>),
    ) {
        write(&NativeLayerItem {
            factory: self,
            key,
            aux,
        });
    }

    unsafe fn read_item_key(&self, bytes: &[u8], offset: usize, target: &mut DynData) {
        // SAFETY: the caller supplies an item produced by this layout. The
        // writer creates the target key through this factory's default_key.
        unsafe { target.deserialize_from_bytes(bytes, offset + self.item.key_offset) };
        self.item_keys_read.fetch_add(1, Relaxed);
    }
}
