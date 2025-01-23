use crate::dynamic::LeanVec;
use crate::operator::group::WithCustomOrd;
use crate::utils::Tup2;
use arrow::array::{ArrayBuilder, ArrayRef, Int32Array, Int32Builder};

pub trait ArrowFormat {
    fn new_builder(&self) -> Box<dyn ArrayBuilder>;
    fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder);
    fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize);
}

macro_rules! impl_arrow_format {
    ($($t:ty),*) => {
        $(
            impl ArrowFormat for $t {
                fn new_builder(&self) -> Box<dyn ArrayBuilder> {
                    //Box::new(Int32Builder::new())
                    unimplemented!()
                }

                fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder) {
                    //let builder_i32 = builder.as_any_mut().downcast_mut::<Int32Builder>().unwrap();
                    //builder_i32.append_value(*self);
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

impl_arrow_format!(u8, u32, u64, i8, i32, i64, f32, f64, bool, (), String);

impl<T1: ArrowFormat, T2: ArrowFormat> ArrowFormat for Tup2<T1, T2> {
    fn new_builder(&self) -> Box<dyn ArrayBuilder> {
        unimplemented!()
    }

    fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder) {
        unimplemented!()
    }

    fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize) {
        unimplemented!()
    }
}

impl<T: ArrowFormat> ArrowFormat for Vec<T> {
    fn new_builder(&self) -> Box<dyn ArrayBuilder> {
        unimplemented!()
    }

    fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder) {
        unimplemented!()
    }

    fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize) {
        unimplemented!()
    }
}

impl<T: ArrowFormat> ArrowFormat for LeanVec<T> {
    fn new_builder(&self) -> Box<dyn ArrayBuilder> {
        unimplemented!()
    }

    fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder) {
        unimplemented!()
    }

    fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize) {
        unimplemented!()
    }
}

impl<T> ArrowFormat for Option<T>
where
    T: ArrowFormat,
{
    fn new_builder(&self) -> Box<dyn ArrayBuilder> {
        unimplemented!()
    }

    fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder) {
        unimplemented!()
    }

    fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize) {
        unimplemented!()
    }
}

impl<T> ArrowFormat for Box<T>
where
    T: ArrowFormat,
{
    fn new_builder(&self) -> Box<dyn ArrayBuilder> {
        unimplemented!()
    }

    fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder) {
        unimplemented!()
    }

    fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize) {
        unimplemented!()
    }
}

impl<T: ArrowFormat, F> ArrowFormat for WithCustomOrd<T, F> {
    fn new_builder(&self) -> Box<dyn ArrayBuilder> {
        unimplemented!()
    }

    fn serialize_into_arrow_builder(&self, builder: &mut dyn ArrayBuilder) {
        unimplemented!()
    }

    fn deserialize_from_arrow(&mut self, array: &ArrayRef, index: usize) {
        unimplemented!()
    }
}
