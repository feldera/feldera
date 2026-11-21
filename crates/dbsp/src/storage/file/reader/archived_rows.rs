//! Read layer-file row locations when the caller supplies the archive layout.

use super::{
    Arc, ArchivedSeekKey, BufferCache, CorruptionError, Error, FBuf, LayerFile, Ordering, Path,
    Range, ReadAhead, StorageBackend, StoragePath,
};

/// A layer file opened without Rust factories for its keys or values.
pub struct ArchivedLayerReader(LayerFile);

impl ArchivedLayerReader {
    /// Opens a file and checks its column count against the caller's layout.
    pub fn open(
        cache: fn() -> Option<Arc<BufferCache>>,
        storage: &dyn StorageBackend,
        path: &StoragePath,
        columns: usize,
    ) -> Result<Self, Error> {
        LayerFile::open(cache, storage.open(path)?, columns).map(Self)
    }

    /// Returns the top-level row group.
    pub fn rows(&self) -> ArchivedRows<'_> {
        ArchivedRows {
            layer: &self.0,
            column: 0,
            rows: 0..self.0.columns[0].n_rows,
        }
    }
}

/// Row locations in one column, restricted to a single row group.
pub struct ArchivedRows<'a> {
    layer: &'a LayerFile,
    column: usize,
    rows: Range<u64>,
}

impl<'a> ArchivedRows<'a> {
    /// Returns the range of absolute row numbers covered by the group.
    pub fn range(&self) -> Range<u64> {
        self.rows.clone()
    }

    /// Finds a row by its zero-based position within the group.
    pub fn nth(&self, index: u64) -> Result<Option<ArchivedLayerRow<'a>>, Error> {
        if index >= self.rows.end - self.rows.start {
            return Ok(None);
        }
        let row = self.rows.start + index;
        let root = self.layer.columns[self.column]
            .root
            .clone()
            .ok_or(CorruptionError::MissingRow(row))?;
        let path =
            Path::for_row_from_ancestor(&self.layer.file, ReadAhead::OFF, Vec::new(), root, row)?;
        Ok(Some(self.row(path)))
    }

    /// Finds the first key at least as large as the target.
    ///
    /// `compare` orders the target relative to the supplied archived key.
    pub fn seek(
        &self,
        mut compare: impl FnMut(ArchivedSeekKey<'_>) -> Result<Ordering, Error>,
    ) -> Result<Option<ArchivedLayerRow<'a>>, Error> {
        let root = self.layer.columns[self.column].root.as_ref();
        let path = Path::best_match(
            &self.layer.file,
            root,
            &self.rows,
            &mut compare,
            Ordering::Less,
        )?;
        Ok(path.map(|path| self.row(path)))
    }

    fn row(&self, path: Path) -> ArchivedLayerRow<'a> {
        ArchivedLayerRow {
            layer: self.layer,
            column: self.column,
            end: self.rows.end,
            path,
        }
    }
}

/// An archived row and its position in the file.
pub struct ArchivedLayerRow<'a> {
    layer: &'a LayerFile,
    column: usize,
    end: u64,
    path: Path,
}

impl<'a> ArchivedLayerRow<'a> {
    /// Returns the absolute row number within this column.
    pub fn row_number(&self) -> u64 {
        self.path.row
    }

    /// Advances within this row group, reusing the current data and index blocks.
    ///
    /// Returns `false` at the end and leaves the row in place.
    pub fn advance(&mut self) -> Result<bool, Error> {
        let next = self.path.row + 1;
        if next == self.end {
            return Ok(false);
        }
        self.path
            .move_to_row(&self.layer.file, ReadAhead::OFF, next)?;
        Ok(true)
    }

    /// Returns the backing block and the item's byte offset within it.
    pub fn item(&self) -> (Arc<FBuf>, usize) {
        let index = (self.path.row - self.path.data.first_row) as usize;
        let item = self.path.data.seek_key(index);
        (item.block(), item.offset())
    }

    /// Returns the associated row group if the file has another column.
    pub fn next_column(&self) -> Result<Option<ArchivedRows<'a>>, Error> {
        let column = self.column + 1;
        let Some(child) = self.layer.columns.get(column) else {
            return Ok(None);
        };
        let rows = self.path.row_group()?;
        if rows.end > child.n_rows {
            return Err(CorruptionError::InvalidRowGroup {
                location: self.path.data.location,
                index: (self.path.row - self.path.data.first_row) as usize,
                start: rows.start,
                end: rows.end,
            }
            .into());
        }
        Ok(Some(ArchivedRows {
            layer: self.layer,
            column,
            rows,
        }))
    }
}
