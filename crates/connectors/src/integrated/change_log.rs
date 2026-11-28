//! CDC rows (row plus `i`, `d`, or `u`) of an output batch, for Delta and Iceberg output.

use crate::SerCursor;
use crate::catalog::{ArrowRowSink, CursorWithPolarity};
use crate::format::MAX_DUPLICATES;
use crate::util::{IndexedOperationType, indexed_operation_type};
use anyhow::{Error as AnyError, anyhow};
use feldera_adapterlib::catalog::SerCursorFlattened;
use feldera_types::program_schema::{Relation, SqlIdentifier};

/// Why [ChangeLog::fill] failed.
pub(crate) enum FillError {
    /// Encoding failed. A retry fails the same way.
    Rows(AnyError),
    /// Reading the batch failed. A retry can succeed.
    Read(AnyError),
}

/// Cursor over the changes of one key range of a batch.
pub(crate) struct ChangeLog<'a> {
    cursor: Box<dyn SerCursor + 'a>,
    view: &'a SqlIdentifier,
    /// Name of the connector's index, if any. Each key is then one row.
    index: Option<&'a SqlIdentifier>,
    /// Copies of the current row left to append, negative for deletes. Without an index only.
    weight: i64,
}

impl<'a> ChangeLog<'a> {
    /// `is_index` means that `key_schema` is the connector's index.
    pub(crate) fn new(
        cursor: Box<dyn SerCursor + 'a>,
        view: &'a SqlIdentifier,
        key_schema: Option<&'a Relation>,
        is_index: bool,
    ) -> Self {
        let index = key_schema.filter(|_| is_index).map(|schema| &schema.name);
        let cursor: Box<dyn SerCursor + 'a> = if index.is_some() {
            cursor
        } else if key_schema.is_some() {
            Box::new(CursorWithPolarity::new(Box::new(SerCursorFlattened::new(
                cursor,
            ))))
        } else {
            Box::new(CursorWithPolarity::new(cursor))
        };
        Self {
            cursor,
            view,
            index,
            weight: 0,
        }
    }

    /// Appends rows to `rows` and operations to `ops` until `ops` holds `max` entries.
    /// Returns `false` at the end of the range. A key that violates the index is skipped
    /// and reported to `violation`.
    pub(crate) fn fill(
        &mut self,
        rows: &mut dyn ArrowRowSink,
        ops: &mut Vec<&'static str>,
        max: usize,
        violation: &mut dyn FnMut(AnyError),
    ) -> Result<bool, FillError> {
        match self.index {
            Some(index) => self.fill_indexed(index, rows, ops, max, violation),
            None => self.fill_weighted(rows, ops, max),
        }
    }

    fn fill_indexed(
        &mut self,
        index: &SqlIdentifier,
        rows: &mut dyn ArrowRowSink,
        ops: &mut Vec<&'static str>,
        max: usize,
        violation: &mut dyn FnMut(AnyError),
    ) -> Result<bool, FillError> {
        let cursor = &mut self.cursor;
        while cursor.key_valid() && ops.len() < max {
            match indexed_operation_type(self.view, index, cursor.as_mut()) {
                Ok(None) => {}
                Ok(Some(op)) => {
                    cursor.rewind_vals();
                    let op = match op {
                        IndexedOperationType::Insert => "i",
                        IndexedOperationType::Delete => "d",
                        IndexedOperationType::Upsert => {
                            // Write the new version, which has positive weight.
                            assert!(cursor.val_valid());
                            if cursor.weight() < 0 {
                                cursor.step_val();
                            }
                            assert!(cursor.val_valid());
                            "u"
                        }
                    };
                    cursor.append_val_to_arrow(rows).map_err(FillError::Rows)?;
                    ops.push(op);
                }
                Err(e) => violation(e),
            }
            cursor.step_key().map_err(FillError::Read)?;
        }
        Ok(cursor.key_valid())
    }

    fn fill_weighted(
        &mut self,
        rows: &mut dyn ArrowRowSink,
        ops: &mut Vec<&'static str>,
        max: usize,
    ) -> Result<bool, FillError> {
        let cursor = &mut self.cursor;
        while ops.len() < max {
            if self.weight == 0 {
                if !cursor.key_valid() {
                    return Ok(false);
                }
                if cursor.val_valid() {
                    self.weight = cursor.weight();
                    if !(-MAX_DUPLICATES..=MAX_DUPLICATES).contains(&self.weight) {
                        return Err(FillError::Rows(anyhow!(
                            "Unable to output record with very large weight {}. \
                             Consider adjusting your SQL queries to avoid duplicate output records, \
                             e.g., using 'SELECT DISTINCT'.",
                            self.weight
                        )));
                    }
                }
                if self.weight == 0 {
                    cursor.step_key().map_err(FillError::Read)?;
                    continue;
                }
            }
            cursor.append_key_to_arrow(rows).map_err(FillError::Rows)?;
            if self.weight > 0 {
                ops.push("i");
                self.weight -= 1;
            } else {
                ops.push("d");
                self.weight += 1;
            }
            if self.weight == 0 {
                cursor.step_key().map_err(FillError::Read)?;
            }
        }
        Ok(true)
    }
}
