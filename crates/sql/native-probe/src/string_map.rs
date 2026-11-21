//! Native batch dispatch for nullable strings and host SQL functions.

use crate::{Error, codegen, runtime::link_object, string_batch::StringInput};
use cranelift_codegen::ir::{AbiParam, InstBuilder, types};
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_module::{Linkage, Module};
use dlopen2::wrapper::{Container, WrapperApi};
use std::{
    any::Any,
    ffi::c_void,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
};
use tempfile::TempDir;

type ProjectRow = extern "C" fn(*mut c_void, usize) -> u8;

#[derive(WrapperApi)]
struct StringMapApi {
    feldera_map_present_strings: unsafe extern "C" fn(
        input: *const StringInput<'_>,
        rows: usize,
        context: *mut c_void,
        project: ProjectRow,
    ) -> usize,
}

pub struct CompiledNullableStringMap {
    library: Container<StringMapApi>,
    _directory: TempDir,
}

impl CompiledNullableStringMap {
    pub fn compile() -> Result<Self, Error> {
        let (directory, path) = link_object(object()?)?;
        // SAFETY: This is the library emitted below, with the exact signature
        // above and no initializers. The container owns the loaded code.
        let library =
            unsafe { Container::load(&path) }.map_err(|error| Error::Compile(error.to_string()))?;
        Ok(Self {
            library,
            _directory: directory,
        })
    }

    pub fn evaluate<T, F: FnMut(&str) -> Option<T>>(
        &self,
        values: &[Option<&str>],
        project: F,
    ) -> Result<Vec<Option<T>>, Error> {
        let inputs = StringInput::prepare(values)?;
        let mut context = MapContext {
            values,
            project,
            output: values.iter().map(|_| None).collect(),
            panic: None,
        };
        // SAFETY: The inputs and context stay live and disjoint until the
        // synchronous native call returns. The generated loop visits each
        // in-bounds non-NULL row once and never stores the callback or context.
        // project_row catches panics and reports failure without unwinding.
        let failure = unsafe {
            self.library.feldera_map_present_strings(
                inputs.as_ptr(),
                inputs.len(),
                (&mut context as *mut MapContext<'_, T, F>).cast(),
                project_row::<T, F>,
            )
        };
        if let Some(panic) = context.panic {
            resume_unwind(panic);
        }
        if failure != 0 {
            return Err(Error::InvalidNativeResult);
        }
        Ok(context.output)
    }
}

struct MapContext<'a, T, F> {
    values: &'a [Option<&'a str>],
    project: F,
    output: Vec<Option<T>>,
    panic: Option<Box<dyn Any + Send>>,
}

extern "C" fn project_row<T, F: FnMut(&str) -> Option<T>>(context: *mut c_void, row: usize) -> u8 {
    // SAFETY: evaluate passes a valid exclusive pointer to a context with this
    // exact type. The native loop never calls project_row concurrently.
    let context = unsafe { &mut *context.cast::<MapContext<'_, T, F>>() };
    match catch_unwind(AssertUnwindSafe(|| {
        let value = context.values[row].expect("native callback receives a present string");
        context.output[row] = (context.project)(value);
    })) {
        Ok(()) => 0,
        Err(panic) => {
            context.panic = Some(panic);
            1
        }
    }
}

fn object() -> Result<Vec<u8>, Error> {
    let mut module = codegen::module()?;
    let mut context = module.make_context();
    context.func.signature.params = vec![AbiParam::new(types::I64); 4];
    context.func.signature.returns = vec![AbiParam::new(types::I64)];
    let function = module
        .declare_function(
            "feldera_map_present_strings",
            Linkage::Export,
            &context.func.signature,
        )
        .map_err(|error| Error::Compile(error.to_string()))?;
    let mut callback = module.make_signature();
    callback.params = vec![AbiParam::new(types::I64); 2];
    callback.returns = vec![AbiParam::new(types::I8)];
    let mut builder_context = FunctionBuilderContext::new();
    let mut builder = FunctionBuilder::new(&mut context.func, &mut builder_context);
    let callback = builder.import_signature(callback);

    let entry = builder.create_block();
    let header = builder.create_block();
    let check = builder.create_block();
    let call = builder.create_block();
    let advance = builder.create_block();
    let failed = builder.create_block();
    let done = builder.create_block();
    builder.append_block_params_for_function_params(entry);
    builder.append_block_param(header, types::I64);

    builder.switch_to_block(entry);
    let [input, rows, context_ptr, project] = builder.block_params(entry) else {
        unreachable!("fixed entry signature");
    };
    let (input, rows, context_ptr, project) = (*input, *rows, *context_ptr, *project);
    let zero = builder.ins().iconst(types::I64, 0);
    builder.ins().jump(header, &[zero.into()]);

    builder.switch_to_block(header);
    let row = builder.block_params(header)[0];
    let more = builder.ins().icmp(
        cranelift_codegen::ir::condcodes::IntCC::UnsignedLessThan,
        row,
        rows,
    );
    builder.ins().brif(more, check, &[], done, &[]);

    builder.switch_to_block(check);
    let (_, _, present) = StringInput::load(&mut builder, input, row);
    builder.ins().brif(present, call, &[], advance, &[]);

    builder.switch_to_block(call);
    let call = builder
        .ins()
        .call_indirect(callback, project, &[context_ptr, row]);
    let status = builder.inst_results(call)[0];
    builder.ins().brif(status, failed, &[], advance, &[]);

    builder.switch_to_block(advance);
    let next = builder.ins().iadd_imm(row, 1);
    builder.ins().jump(header, &[next.into()]);

    builder.switch_to_block(failed);
    let failure = builder.ins().iadd_imm(row, 1);
    builder.ins().return_(&[failure]);

    builder.switch_to_block(done);
    builder.ins().return_(&[zero]);
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
