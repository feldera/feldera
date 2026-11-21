//! Batch evaluation of a strict UTF-8 range from the exported filter IR.

use crate::{Error, codegen, runtime::link_object, string_batch::StringInput};
use cranelift_codegen::ir::{
    AbiParam, Block, InstBuilder, MemFlags, Value, condcodes::IntCC, types,
};
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_module::{Linkage, Module};
use dlopen2::wrapper::{Container, WrapperApi};
use tempfile::TempDir;

#[derive(WrapperApi)]
struct StringRangeApi {
    feldera_filter_string_range:
        unsafe extern "C" fn(input: *const StringInput<'_>, rows: usize, keep: *mut u8),
}

pub struct CompiledStringRange {
    library: Container<StringRangeApi>,
    _directory: TempDir,
}

impl CompiledStringRange {
    /// Compile `value > lower AND value < upper`, with NULL filtered out.
    pub fn compile(lower: &str, upper: &str) -> Result<Self, Error> {
        if lower.len() > 32 || upper.len() > 32 {
            return Err(Error::InvalidProgram(
                "string range bounds exceed 32 bytes".into(),
            ));
        }
        let (directory, path) = link_object(object(lower.as_bytes(), upper.as_bytes())?)?;
        // SAFETY: the object is emitted below with this exact signature and
        // has no constructors or callbacks. The container owns the library.
        let library =
            unsafe { Container::load(&path) }.map_err(|error| Error::Compile(error.to_string()))?;
        Ok(Self {
            library,
            _directory: directory,
        })
    }

    pub fn filter(&self, values: &[Option<&str>]) -> Result<Vec<u8>, Error> {
        let inputs = StringInput::prepare(values)?;
        let mut keep = vec![0; inputs.len()];
        // SAFETY: inputs and output remain live and disjoint for the call.
        // Each native byte load follows a corresponding length check. NULL
        // rows skip comparisons, and the loop writes one byte per row.
        unsafe {
            self.library.feldera_filter_string_range(
                inputs.as_ptr(),
                inputs.len(),
                keep.as_mut_ptr(),
            );
        }
        Ok(keep)
    }
}

fn object(lower: &[u8], upper: &[u8]) -> Result<Vec<u8>, Error> {
    let mut module = codegen::module()?;
    let mut context = module.make_context();
    context.func.signature.params = vec![AbiParam::new(types::I64); 3];
    let id = module
        .declare_function(
            "feldera_filter_string_range",
            Linkage::Export,
            &context.func.signature,
        )
        .map_err(|error| Error::Compile(error.to_string()))?;
    let mut builder_context = FunctionBuilderContext::new();
    let mut builder = FunctionBuilder::new(&mut context.func, &mut builder_context);
    emit_filter(&mut builder, lower, upper);
    builder.seal_all_blocks();
    builder.finalize();
    module
        .define_function(id, &mut context)
        .map_err(|error| Error::Compile(error.to_string()))?;
    module
        .finish()
        .emit()
        .map_err(|error| Error::Compile(error.to_string()))
}

struct Comparison<'a> {
    bound: &'a [u8],
    accept_less: bool,
    accept: Block,
    reject: Block,
}

fn emit_comparison(
    builder: &mut FunctionBuilder<'_>,
    data: Value,
    len: Value,
    test: Comparison<'_>,
) {
    let (condition, short) = if test.accept_less {
        (IntCC::UnsignedLessThan, test.accept)
    } else {
        (IntCC::UnsignedGreaterThan, test.reject)
    };
    for (offset, &limit) in test.bound.iter().enumerate() {
        let load = builder.create_block();
        let compare = builder.create_block();
        let next = builder.create_block();
        let in_bounds = builder
            .ins()
            .icmp_imm(IntCC::UnsignedGreaterThan, len, offset as i64);
        builder.ins().brif(in_bounds, load, &[], short, &[]);
        builder.switch_to_block(load);
        let byte = builder
            .ins()
            .load(types::I8, MemFlags::new(), data, offset as i32);
        let equal = builder.ins().icmp_imm(IntCC::Equal, byte, i64::from(limit));
        builder.ins().brif(equal, next, &[], compare, &[]);
        builder.switch_to_block(compare);
        let matches = builder.ins().icmp_imm(condition, byte, i64::from(limit));
        builder
            .ins()
            .brif(matches, test.accept, &[], test.reject, &[]);
        builder.switch_to_block(next);
    }
    if test.accept_less {
        builder.ins().jump(test.reject, &[]);
    } else {
        let longer =
            builder
                .ins()
                .icmp_imm(IntCC::UnsignedGreaterThan, len, test.bound.len() as i64);
        builder
            .ins()
            .brif(longer, test.accept, &[], test.reject, &[]);
    }
}

fn emit_filter(builder: &mut FunctionBuilder<'_>, lower: &[u8], upper: &[u8]) {
    let entry = builder.create_block();
    builder.append_block_params_for_function_params(entry);
    builder.switch_to_block(entry);
    let input = builder.block_params(entry)[0];
    let rows = builder.block_params(entry)[1];
    let output = builder.block_params(entry)[2];
    let header = builder.create_block();
    let read = builder.create_block();
    let test_lower = builder.create_block();
    let test_upper = builder.create_block();
    let accept = builder.create_block();
    let reject = builder.create_block();
    let store = builder.create_block();
    let done = builder.create_block();
    builder.append_block_param(header, types::I64);
    builder.append_block_param(store, types::I8);
    let zero = builder.ins().iconst(types::I64, 0);
    builder.ins().jump(header, &[zero.into()]);
    builder.switch_to_block(header);
    let row = builder.block_params(header)[0];
    let more = builder.ins().icmp(IntCC::UnsignedLessThan, row, rows);
    builder.ins().brif(more, read, &[], done, &[]);
    builder.switch_to_block(read);
    let (data, len, present) = StringInput::load(builder, input, row);
    builder.ins().brif(present, test_lower, &[], reject, &[]);
    builder.switch_to_block(test_lower);
    emit_comparison(
        builder,
        data,
        len,
        Comparison {
            bound: lower,
            accept_less: false,
            accept: test_upper,
            reject,
        },
    );
    builder.switch_to_block(test_upper);
    emit_comparison(
        builder,
        data,
        len,
        Comparison {
            bound: upper,
            accept_less: true,
            accept,
            reject,
        },
    );
    builder.switch_to_block(accept);
    let yes = builder.ins().iconst(types::I8, 1);
    builder.ins().jump(store, &[yes.into()]);
    builder.switch_to_block(reject);
    let no = builder.ins().iconst(types::I8, 0);
    builder.ins().jump(store, &[no.into()]);
    builder.switch_to_block(store);
    let keep = builder.block_params(store)[0];
    let destination = builder.ins().iadd(output, row);
    builder.ins().store(MemFlags::new(), keep, destination, 0);
    let next = builder.ins().iadd_imm(row, 1);
    builder.ins().jump(header, &[next.into()]);
    builder.switch_to_block(done);
    builder.ins().return_(&[]);
}
