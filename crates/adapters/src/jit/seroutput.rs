use std::sync::Arc;

use anyhow::Result as AnyResult;
use dataflow_jit::{codegen::json::SerializeFn, row::Row};
use dbsp::{
    trace::{BatchReader, Cursor},
    OrdZSet, OutputHandle,
};
use pipeline_types::format::json::JsonFlavor;

use crate::{
    catalog::{RecordFormat, SerCollectionHandle, SerCursor},
    ControllerError, SerBatch,
};

type RowZSet = OrdZSet<Row, i32>;

/// `SerBatch` implementation backed by a `RowZSet` with attached
/// serialization functions for all supported formats.
struct SerZSet {
    zset: RowZSet,
    default_json: SerializeFn,
    snowflake_json: SerializeFn,
}

impl SerZSet {
    fn new(zset: RowZSet, default_json: SerializeFn, snowflake_json: SerializeFn) -> Self {
        Self {
            zset,
            default_json,
            snowflake_json,
        }
    }
}

impl SerBatch for SerZSet {
    fn key_count(&self) -> usize {
        self.zset.key_count()
    }

    fn len(&self) -> usize {
        self.zset.len()
    }

    fn cursor<'a>(
        &'a self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn SerCursor + 'a>, ControllerError> {
        match record_format {
            RecordFormat::Csv => todo!(),
            RecordFormat::Json(JsonFlavor::Snowflake) => Ok(Box::new(SerZSetCursor::new(
                self.zset.cursor(),
                self.snowflake_json,
            ))),
            RecordFormat::Json(_) => Ok(Box::new(SerZSetCursor::new(
                self.zset.cursor(),
                self.default_json,
            ))),
        }
    }
}

/// [`SerCursor`] implementation backed by a regular cursor and
/// a serializer function for the format that this cursor is configured
/// with.
struct SerZSetCursor<'a> {
    cursor: <RowZSet as BatchReader>::Cursor<'a>,
    serfn: SerializeFn,
}

impl<'a> SerZSetCursor<'a> {
    fn new(cursor: <RowZSet as BatchReader>::Cursor<'a>, serfn: SerializeFn) -> Self {
        Self { cursor, serfn }
    }
}

impl<'a> SerCursor for SerZSetCursor<'a> {
    fn key_valid(&self) -> bool {
        self.cursor.key_valid()
    }

    fn val_valid(&self) -> bool {
        self.cursor.val_valid()
    }

    fn serialize_key(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        unsafe { (self.serfn)(self.cursor.key().as_ptr(), dst) };
        Ok(())
    }

    fn serialize_key_weight(&mut self, _dst: &mut Vec<u8>) -> AnyResult<()> {
        todo!()
    }

    fn serialize_val(&mut self, _dst: &mut Vec<u8>) -> AnyResult<()> {
        todo!()
    }

    fn weight(&mut self) -> i64 {
        self.cursor.weight() as i64
    }

    fn step_key(&mut self) {
        self.cursor.step_key();
    }

    fn step_val(&mut self) {
        self.cursor.step_val();
    }

    fn rewind_keys(&mut self) {
        self.cursor.rewind_keys()
    }

    fn rewind_vals(&mut self) {
        self.cursor.rewind_vals()
    }
}

/// [`SerCollectionHandle`](`crate::SerCollectionHandle`) implementation
/// backed by an [`OutputHandle`] and attached serialization functionsl for all
/// supported formats.
#[derive(Clone)]
pub struct SerZSetHandle {
    handle: OutputHandle<RowZSet>,
    default_json: SerializeFn,
    snowflake_json: SerializeFn,
}

impl SerZSetHandle {
    pub fn new(
        handle: OutputHandle<OrdZSet<Row, i32>>,
        default_json: SerializeFn,
        snowflake_json: SerializeFn,
    ) -> Self {
        Self {
            handle,
            default_json,
            snowflake_json,
        }
    }
}

impl SerCollectionHandle for SerZSetHandle {
    fn take_from_worker(&self, worker: usize) -> Option<Box<dyn SerBatch>> {
        self.handle.take_from_worker(worker).map(|batch| {
            Box::new(SerZSet::new(batch, self.default_json, self.snowflake_json))
                as Box<dyn SerBatch>
        })
    }

    fn take_from_all(&self) -> Vec<Arc<dyn SerBatch>> {
        self.handle
            .take_from_all()
            .into_iter()
            .map(|batch| {
                Arc::new(SerZSet::new(batch, self.default_json, self.snowflake_json))
                    as Arc<dyn SerBatch>
            })
            .collect()
    }

    fn consolidate(&self) -> Box<dyn SerBatch> {
        let batch = self.handle.consolidate();
        Box::new(SerZSet::new(batch, self.default_json, self.snowflake_json))
    }

    fn fork(&self) -> Box<dyn SerCollectionHandle> {
        Box::new(self.clone())
    }
}
