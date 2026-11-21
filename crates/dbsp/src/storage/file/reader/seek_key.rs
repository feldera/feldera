use super::{Arc, DataTrait, FBuf, Factories};

/// Location of a probed key in a layer file.
pub struct ArchivedSeekKey<'a> {
    pub(super) block: &'a Arc<FBuf>,
    pub(super) offset: usize,
    pub(super) is_index_bound: bool,
}

impl<'a> ArchivedSeekKey<'a> {
    /// Retain the block that holds the key.
    #[cfg(feature = "native-abi")]
    pub fn block(&self) -> Arc<FBuf> {
        self.block.clone()
    }

    /// Offset of the item in a data block or the key in an index block.
    #[cfg(feature = "native-abi")]
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Whether the offset points directly to an index key.
    #[cfg(feature = "native-abi")]
    pub fn is_index_bound(&self) -> bool {
        self.is_index_bound
    }

    /// # Safety
    ///
    /// The factories must match the key and item types used to write the file.
    pub(super) unsafe fn archived<K, A>(self, factories: &Factories<K, A>) -> &'a K::Archived
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        // SAFETY: the key location came from a data or index block's offset
        // map. The caller supplies factories for that block's archive types.
        unsafe {
            if self.is_index_bound {
                factories
                    .key_factory
                    .archived_value(self.block, self.offset)
            } else {
                factories
                    .item_factory
                    .archived_value(self.block, self.offset)
                    .fst()
            }
        }
    }
}
