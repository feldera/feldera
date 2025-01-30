use crate::dynamic::LeanVec;
use crate::operator::group::WithCustomOrd;
use crate::utils::{Tup1, Tup2, Tup3, Tup4};
use crate::DBData;
use arrow::array::{ArrayBuilder, ArrayRef, Int64Array, Int64Builder, UInt64Array, UInt64Builder};
use arrow::datatypes::DataType;

pub trait ArrowSupport {
    type ArrayBuilderType: ArrayBuilder + ?Sized;
    fn arrow_data_type(&self) -> DataType;
}

impl<T: DBData> ArrowSupport for T {
    type ArrayBuilderType = UInt64Builder;
    fn arrow_data_type(&self) -> DataType {
        DataType::UInt64
    }
}

pub trait ArrowSupportDyn {
    fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder);
    fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize);
}

impl ArrowSupportDyn for i64 {
    fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder) {
        let builder_i64 = builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
        builder_i64.append_value(*self);
        eprintln!("builder_i64.len() = {}", builder_i64.len());
    }

    fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize) {
        let accessor = array
            .as_ref()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("Builder has wrong type {:?}", array));
        *self = accessor.value(index);
    }
}

impl ArrowSupportDyn for u64 {
    fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder) {
        let builder_i64 = builder
            .as_any_mut()
            .downcast_mut::<UInt64Builder>()
            .unwrap();
        builder_i64.append_value(*self);
        eprintln!("builder_i64.len() = {}", builder_i64.len());
    }

    fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize) {
        let accessor = array
            .as_ref()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap_or_else(|| panic!("Array has wrong type {:?}", array));
        *self = accessor.value(index);
    }
}

macro_rules! impl_arrow_format {
    ($($t:ty),*) => {
        $(
            impl ArrowSupportDyn for $t {
                fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder) {
                    unimplemented!()
                }

                fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize) {
                    /*let accessor = array
                        .as_ref()
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap_or_else(|| panic!("Builder has wrong type {:?}", array));
                    *self = accessor.value(index);*/
                    unimplemented!()
                }
            }
        )*
    };
}

impl_arrow_format!(u8, u16, u32, i8, i16, i32, f32, f64, bool, (), String, char);

impl<T1: ArrowSupportDyn> ArrowSupportDyn for Tup1<T1> {
    fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder) {
        unimplemented!()
    }

    fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize) {
        unimplemented!()
    }
}

impl<T1: ArrowSupportDyn, T2: ArrowSupportDyn> ArrowSupportDyn for Tup2<T1, T2> {
    fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder) {
        unimplemented!()
    }

    fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize) {
        unimplemented!()
    }
}

impl<T1: ArrowSupportDyn, T2: ArrowSupportDyn, T3: ArrowSupportDyn> ArrowSupportDyn
    for Tup3<T1, T2, T3>
{
    fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder) {
        unimplemented!()
    }

    fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize) {
        unimplemented!()
    }
}

impl<T1: ArrowSupportDyn, T2: ArrowSupportDyn, T3: ArrowSupportDyn, T4: ArrowSupportDyn>
    ArrowSupportDyn for Tup4<T1, T2, T3, T4>
{
    fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder) {
        unimplemented!()
    }

    fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize) {
        unimplemented!()
    }
}

impl<T1: ArrowSupportDyn, T2: ArrowSupportDyn> ArrowSupportDyn for (T1, T2) {
    fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder) {
        unimplemented!()
    }

    fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize) {
        unimplemented!()
    }
}
impl<T1: ArrowSupportDyn, T2: ArrowSupportDyn, T3: ArrowSupportDyn> ArrowSupportDyn
    for (T1, T2, T3)
{
    fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder) {
        unimplemented!()
    }

    fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize) {
        unimplemented!()
    }
}

impl<T: ArrowSupportDyn> ArrowSupportDyn for Vec<T> {
    fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder) {
        unimplemented!()
    }

    fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize) {
        unimplemented!()
    }
}

impl<T: ArrowSupportDyn> ArrowSupportDyn for LeanVec<T> {
    fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder) {
        unimplemented!()
    }

    fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize) {
        unimplemented!()
    }
}

impl<T> ArrowSupportDyn for Option<T>
where
    T: ArrowSupportDyn,
{
    fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder) {
        unimplemented!()
    }

    fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize) {
        unimplemented!()
    }
}

impl<T> ArrowSupportDyn for Box<T>
where
    T: ArrowSupportDyn,
{
    fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder) {
        unimplemented!()
    }

    fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize) {
        unimplemented!()
    }
}

impl<T: ArrowSupportDyn, F> ArrowSupportDyn for WithCustomOrd<T, F> {
    fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder) {
        unimplemented!()
    }

    fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize) {
        unimplemented!()
    }
}
