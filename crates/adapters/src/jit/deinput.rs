use anyhow::Result as AnyResult;
use dataflow_jit::facade::{DeCollectionStream as JitDeCollectionStream, JsonZSetHandle};

use crate::{
    catalog::RecordFormat, format::JsonFlavor, ControllerError, DeCollectionHandle,
    DeCollectionStream,
};

impl<JS> DeCollectionStream for JS
where
    JS: JitDeCollectionStream + 'static,
{
    fn insert(&mut self, data: &[u8]) -> AnyResult<()> {
        self.push(data, 1)
    }
    fn delete(&mut self, data: &[u8]) -> AnyResult<()> {
        self.push(data, -1)
    }

    fn reserve(&mut self, reservation: usize) {
        JitDeCollectionStream::reserve(self, reservation);
    }

    fn flush(&mut self) {
        JitDeCollectionStream::flush(self);
    }

    fn clear_buffer(&mut self) {
        JitDeCollectionStream::clear_buffer(self);
    }

    fn fork(&self) -> Box<dyn DeCollectionStream> {
        Box::new(self.clone())
    }
}

/// [`DeCollectionHandle`] implementation using pre-compiled deserializers
/// for all supported formats.
pub struct DeZSetHandles {
    default_json: JsonZSetHandle,
    debezium_mysql_json: JsonZSetHandle,
}

impl DeZSetHandles {
    pub fn new(default_json: JsonZSetHandle, debezium_mysql_json: JsonZSetHandle) -> Self {
        Self {
            default_json,
            debezium_mysql_json,
        }
    }
}

impl DeCollectionHandle for DeZSetHandles {
    fn configure_deserializer(
        &self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn DeCollectionStream>, ControllerError> {
        match record_format {
            RecordFormat::Json(JsonFlavor::Default) => Ok(Box::new(self.default_json.clone())),
            RecordFormat::Json(JsonFlavor::DebeziumMySql) => {
                Ok(Box::new(self.debezium_mysql_json.clone()))
            }
            RecordFormat::Csv => {
                todo!()
            }
        }
    }
}
