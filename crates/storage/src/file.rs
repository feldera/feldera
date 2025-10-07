use std::sync::atomic::{AtomicU64, Ordering};

/// A unique identifier for a [crate::FileReader] or [crate::FileWriter].
///
/// The buffer cache uses this ID for indexing.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FileId(u64);

impl FileId {
    /// Creates a fresh unique identifier.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        static NEXT_FILE_ID: AtomicU64 = AtomicU64::new(0);
        Self(NEXT_FILE_ID.fetch_add(1, Ordering::Relaxed))
    }

    pub fn after(&self) -> Self {
        Self(self.0 + 1)
    }
}
