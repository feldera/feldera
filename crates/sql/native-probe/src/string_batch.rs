use crate::Error;
use cranelift_codegen::ir::{InstBuilder, MemFlags, Value, types};
use cranelift_frontend::FunctionBuilder;
use std::{
    marker::PhantomData,
    mem::{offset_of, size_of},
};

#[repr(C)]
pub(crate) struct StringInput<'a> {
    data: *const u8,
    len: u64,
    present: u64,
    source: PhantomData<&'a str>,
}

impl<'a> StringInput<'a> {
    pub(crate) fn prepare(values: &[Option<&'a str>]) -> Result<Vec<Self>, Error> {
        if values.len() > 65_536 {
            return Err(Error::InvalidBatch(
                "native string batch exceeds 65536 rows".into(),
            ));
        }
        Ok(values
            .iter()
            .map(|value| {
                let text = value.unwrap_or("");
                Self {
                    data: text.as_ptr(),
                    len: text.len() as u64,
                    present: u64::from(value.is_some()),
                    source: PhantomData,
                }
            })
            .collect())
    }

    pub(crate) fn load(
        builder: &mut FunctionBuilder<'_>,
        input: Value,
        row: Value,
    ) -> (Value, Value, Value) {
        let offset = builder.ins().imul_imm(row, size_of::<Self>() as i64);
        let record = builder.ins().iadd(input, offset);
        let mut load = |field| {
            builder
                .ins()
                .load(types::I64, MemFlags::new(), record, field)
        };
        (
            load(offset_of!(Self, data) as i32),
            load(offset_of!(Self, len) as i32),
            load(offset_of!(Self, present) as i32),
        )
    }
}
