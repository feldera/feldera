//! Batched UTF-8 truncation and leading trim for the selected string map.

use crate::{Error, codegen, runtime::link_object, string_batch::StringInput};
use cranelift_codegen::ir::{AbiParam, InstBuilder, MemFlags, condcodes::IntCC, types};
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_module::{Linkage, Module};
use dlopen2::wrapper::{Container, WrapperApi};
use std::mem::{offset_of, size_of};
use tempfile::TempDir;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StringSlices {
    pub first_end: usize,
    pub trimmed_start: usize,
}

#[derive(WrapperApi)]
struct StringProjectionApi {
    feldera_project_strings:
        unsafe extern "C" fn(input: *const StringInput<'_>, rows: usize, output: *mut StringSlices),
}

pub struct CompiledStringProjection {
    library: Container<StringProjectionApi>,
    _directory: TempDir,
}

impl CompiledStringProjection {
    pub fn compile(leading_pattern: &str) -> Result<Self, Error> {
        if !(1..=32).contains(&leading_pattern.len()) {
            return Err(Error::InvalidProgram(
                "trim pattern must contain 1 to 32 bytes".into(),
            ));
        }
        let (directory, path) = link_object(object(leading_pattern.as_bytes())?)?;
        // SAFETY: this emitter exports the exact private API and has no
        // constructors or callbacks. The container owns the loaded library.
        let library =
            unsafe { Container::load(&path) }.map_err(|error| Error::Compile(error.to_string()))?;
        Ok(Self {
            library,
            _directory: directory,
        })
    }

    pub fn project(&self, values: &[Option<&str>]) -> Result<Vec<StringSlices>, Error> {
        let inputs = StringInput::prepare(values)?;
        let mut output = vec![StringSlices::default(); inputs.len()];
        // SAFETY: the input borrows keep every string live. The native loop
        // checks the remaining bytes before each fixed pattern comparison and
        // writes one output struct per row. NULL and empty rows read no bytes.
        unsafe {
            self.library.feldera_project_strings(
                inputs.as_ptr(),
                inputs.len(),
                output.as_mut_ptr(),
            );
        }
        Ok(output)
    }
}

fn object(pattern: &[u8]) -> Result<Vec<u8>, Error> {
    let mut module = codegen::module()?;
    let mut context = module.make_context();
    context.func.signature.params = vec![AbiParam::new(types::I64); 3];
    let function = module
        .declare_function(
            "feldera_project_strings",
            Linkage::Export,
            &context.func.signature,
        )
        .map_err(|error| Error::Compile(error.to_string()))?;
    let mut builder_context = FunctionBuilderContext::new();
    let mut builder = FunctionBuilder::new(&mut context.func, &mut builder_context);
    emit(&mut builder, pattern);
    builder.seal_all_blocks();
    builder.finalize();
    module
        .define_function(function, &mut context)
        .map_err(|error| Error::Compile(error.to_string()))?;
    module
        .finish()
        .emit()
        .map_err(|error| Error::Compile(error.to_string()))
}

fn emit(builder: &mut FunctionBuilder<'_>, pattern: &[u8]) {
    let entry = builder.create_block();
    builder.append_block_params_for_function_params(entry);
    builder.switch_to_block(entry);
    let input = builder.block_params(entry)[0];
    let rows = builder.block_params(entry)[1];
    let output = builder.block_params(entry)[2];
    let header = builder.create_block();
    let read = builder.create_block();
    let first = builder.create_block();
    let trim = builder.create_block();
    let check = builder.create_block();
    let store = builder.create_block();
    let done = builder.create_block();
    builder.append_block_param(header, types::I64);
    builder.append_block_param(trim, types::I64);
    builder.append_block_param(store, types::I64);
    builder.append_block_param(store, types::I64);
    let zero = builder.ins().iconst(types::I64, 0);
    builder.ins().jump(header, &[zero.into()]);
    builder.switch_to_block(header);
    let row = builder.block_params(header)[0];
    let more = builder.ins().icmp(IntCC::UnsignedLessThan, row, rows);
    builder.ins().brif(more, read, &[], done, &[]);
    builder.switch_to_block(read);
    // The batch descriptor uses length zero for NULL.
    let (data, len, _) = StringInput::load(builder, input, row);
    builder
        .ins()
        .brif(len, first, &[], store, &[zero.into(), zero.into()]);
    builder.switch_to_block(first);
    let head = builder.ins().load(types::I8, MemFlags::new(), data, 0);
    // A valid UTF-8 leading byte encodes a scalar of one to four bytes.
    let mut width = builder.ins().iconst(types::I64, 1);
    for (threshold, next_width) in [(0xc0, 2), (0xe0, 3), (0xf0, 4)] {
        let large = builder
            .ins()
            .icmp_imm(IntCC::UnsignedGreaterThanOrEqual, head, threshold);
        let next_width = builder.ins().iconst(types::I64, next_width);
        width = builder.ins().select(large, next_width, width);
    }
    builder.ins().jump(trim, &[zero.into()]);
    builder.switch_to_block(trim);
    let skip = builder.block_params(trim)[0];
    let remaining = builder.ins().isub(len, skip);
    let enough = builder.ins().icmp_imm(
        IntCC::UnsignedGreaterThanOrEqual,
        remaining,
        pattern.len() as i64,
    );
    builder
        .ins()
        .brif(enough, check, &[], store, &[width.into(), skip.into()]);
    builder.switch_to_block(check);
    let start = builder.ins().iadd(data, skip);
    for (index, &expected) in pattern.iter().enumerate() {
        let next = builder.create_block();
        let byte = builder
            .ins()
            .load(types::I8, MemFlags::new(), start, index as i32);
        let equal = builder
            .ins()
            .icmp_imm(IntCC::Equal, byte, i64::from(expected));
        builder
            .ins()
            .brif(equal, next, &[], store, &[width.into(), skip.into()]);
        builder.switch_to_block(next);
    }
    let next_prefix = builder.ins().iadd_imm(skip, pattern.len() as i64);
    builder.ins().jump(trim, &[next_prefix.into()]);
    builder.switch_to_block(store);
    let first_end = builder.block_params(store)[0];
    let trimmed_start = builder.block_params(store)[1];
    let offset = builder
        .ins()
        .imul_imm(row, size_of::<StringSlices>() as i64);
    let destination = builder.ins().iadd(output, offset);
    builder.ins().store(
        MemFlags::new(),
        first_end,
        destination,
        offset_of!(StringSlices, first_end) as i32,
    );
    builder.ins().store(
        MemFlags::new(),
        trimmed_start,
        destination,
        offset_of!(StringSlices, trimmed_start) as i32,
    );
    let next = builder.ins().iadd_imm(row, 1);
    builder.ins().jump(header, &[next.into()]);
    builder.switch_to_block(done);
    builder.ins().return_(&[]);
}
