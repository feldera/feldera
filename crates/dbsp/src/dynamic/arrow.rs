use crate::DBData;
use arrow::array::{ArrayBuilder, ArrayRef, BinaryArray, BinaryBuilder};
use arrow::datatypes::DataType;
use rkyv::{archived_root, de::deserializers::SharedDeserializeMap, Deserialize};

use crate::storage::file::to_bytes;

pub trait ArrowSupport {
    type ArrayBuilderType: ArrayBuilder + Default + 'static;

    fn arrow_data_type() -> DataType;
}

impl<T: DBData> ArrowSupport for T {
    type ArrayBuilderType = BinaryBuilder;

    fn arrow_data_type() -> DataType {
        DataType::Binary
    }
}

pub trait ArrowSupportDyn {
    fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder);
    fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize);
}

impl<T: DBData> ArrowSupportDyn for T {
    fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder) {
        let builder = builder
            .as_any_mut()
            .downcast_mut::<BinaryBuilder>()
            .expect("BinaryBuilder expected for DBData serialization");
        let buffer = to_bytes(self).expect("rkyv serialization must succeed");
        builder.append_value(buffer.as_slice());
    }

    fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize) {
        let accessor = array
            .as_ref()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("BinaryArray expected for DBData deserialization");
        let archived = unsafe { archived_root::<T>(accessor.value(index)) };
        *self = archived
            .deserialize(&mut SharedDeserializeMap::new())
            .expect("rkyv deserialization must succeed");
    }
}
