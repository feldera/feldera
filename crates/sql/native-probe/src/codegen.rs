//! All generated memory accesses follow the validated Program/Batch contract.
//! The sole exported function processes the entire batch and returns either zero
//! or the one-based index of the first overflowing row. It makes no callbacks.

use cranelift_codegen::ir::{AbiParam, InstBuilder, MemFlags, Value, condcodes::IntCC, types};
use cranelift_codegen::settings::{self, Configurable};
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_module::{Linkage, Module};
use cranelift_object::{ObjectBuilder, ObjectModule};

use crate::{Arithmetic, Error, Expr, Program};

pub(crate) const SYMBOL: &str = "feldera_batch";

pub(crate) fn object(program: &Program) -> Result<Vec<u8>, Error> {
    let mut module = module()?;
    let compile_error = |error: String| Error::Compile(error);
    let mut context = module.make_context();
    // const i64*, const u64*, usize, i64*, u8*, i64[3]* -> u64, native C ABI.
    context.func.signature.params = vec![AbiParam::new(types::I64); 6];
    context
        .func
        .signature
        .returns
        .push(AbiParam::new(types::I64));
    let function = module
        .declare_function(SYMBOL, Linkage::Export, &context.func.signature)
        .map_err(|e| compile_error(e.to_string()))?;
    let mut builder_context = FunctionBuilderContext::new();
    let mut builder = FunctionBuilder::new(&mut context.func, &mut builder_context);
    emit_batch(&mut builder, program);
    builder.seal_all_blocks();
    builder.finalize();
    module
        .define_function(function, &mut context)
        .map_err(|e| compile_error(e.to_string()))?;
    module
        .finish()
        .emit()
        .map_err(|e| compile_error(e.to_string()))
}

pub(crate) fn module() -> Result<ObjectModule, Error> {
    let compile_error = |error: String| Error::Compile(error);
    let mut flags = settings::builder();
    flags
        .set("is_pic", "true")
        .map_err(|e| compile_error(e.to_string()))?;
    flags
        .set("opt_level", "speed")
        .map_err(|e| compile_error(e.to_string()))?;
    let isa = cranelift_native::builder()
        .map_err(|e| compile_error(e.into()))?
        .finish(settings::Flags::new(flags))
        .map_err(|e| compile_error(e.to_string()))?;
    if isa.pointer_type() != types::I64 {
        return Err(compile_error("the prototype requires a 64-bit host".into()));
    }
    let builder = ObjectBuilder::new(
        isa,
        "feldera-native-probe",
        cranelift_module::default_libcall_names(),
    )
    .map_err(|e| compile_error(e.to_string()))?;
    Ok(ObjectModule::new(builder))
}

fn emit_batch(builder: &mut FunctionBuilder<'_>, program: &Program) {
    let entry = builder.create_block();
    builder.append_block_params_for_function_params(entry);
    builder.switch_to_block(entry);
    let arguments = builder.block_params(entry).to_vec();
    let header = builder.create_block();
    let body = builder.create_block();
    let done = builder.create_block();
    builder.append_block_param(header, types::I64);
    let zero = builder.ins().iconst(types::I64, 0);
    builder.ins().jump(header, &[zero.into()]);

    builder.switch_to_block(header);
    let row = builder.block_params(header)[0];
    let more = builder
        .ins()
        .icmp(IntCC::UnsignedLessThan, row, arguments[2]);
    builder.ins().brif(more, body, &[], done, &[]);
    builder.switch_to_block(body);
    let row_bytes = builder.ins().imul_imm(row, (program.columns * 8) as i64);
    let row_values = builder.ins().iadd(arguments[0], row_bytes);
    let mask_offset = builder.ins().imul_imm(row, 8);
    let mask_address = builder.ins().iadd(arguments[1], mask_offset);
    let null_mask = builder
        .ins()
        .load(types::I64, MemFlags::new(), mask_address, 0);
    let mut emitter = ExpressionEmitter {
        builder,
        program,
        row,
        row_values,
        null_mask,
        error_record: arguments[5],
    };
    let value = emitter.emit(program.expressions.len() - 1);
    let output_address = builder.ins().iadd(arguments[3], mask_offset);
    builder
        .ins()
        .store(MemFlags::new(), value.payload, output_address, 0);
    let null_address = builder.ins().iadd(arguments[4], row);
    builder
        .ins()
        .store(MemFlags::new(), value.is_null, null_address, 0);
    let next_row = builder.ins().iadd_imm(row, 1);
    builder.ins().jump(header, &[next_row.into()]);
    builder.switch_to_block(done);
    builder.ins().return_(&[zero]);
}

#[derive(Clone, Copy)]
struct NullableValue {
    payload: Value,
    is_null: Value,
}

struct ExpressionEmitter<'a, 'b> {
    builder: &'a mut FunctionBuilder<'b>,
    program: &'a Program,
    row: Value,
    row_values: Value,
    null_mask: Value,
    error_record: Value,
}

impl ExpressionEmitter<'_, '_> {
    fn constant(&mut self, value: Option<i64>) -> NullableValue {
        NullableValue {
            payload: self.builder.ins().iconst(types::I64, value.unwrap_or(0)),
            is_null: self
                .builder
                .ins()
                .iconst(types::I8, i64::from(value.is_none())),
        }
    }

    fn emit(&mut self, index: usize) -> NullableValue {
        match &self.program.expressions[index] {
            Expr::Literal(value) => self.constant(*value),
            Expr::Column(column) => {
                let payload = self.builder.ins().load(
                    types::I64,
                    MemFlags::new(),
                    self.row_values,
                    (*column * 8) as i32,
                );
                let bit = self.builder.ins().ushr_imm(self.null_mask, *column as i64);
                let bit = self.builder.ins().band_imm(bit, 1);
                let is_null = self.builder.ins().ireduce(types::I8, bit);
                NullableValue { payload, is_null }
            }
            Expr::Arithmetic { op, left, right } => {
                // Evaluate both arguments before NULL propagation, as sqllib
                // function arguments do. An overflow in either child is an error.
                let left = self.emit(*left);
                let right = self.emit(*right);
                self.arithmetic(*op, left, right)
            }
            Expr::Compare { op, left, right } => {
                let left = self.emit(*left);
                let right = self.emit(*right);
                let condition = match op {
                    crate::Comparison::GreaterThan => IntCC::SignedGreaterThan,
                    crate::Comparison::NotEqual => IntCC::NotEqual,
                };
                let comparison = self
                    .builder
                    .ins()
                    .icmp(condition, left.payload, right.payload);
                NullableValue {
                    payload: self.builder.ins().uextend(types::I64, comparison),
                    is_null: self.builder.ins().bor(left.is_null, right.is_null),
                }
            }
            Expr::Coalesce(children) => self.coalesce(children),
        }
    }

    fn arithmetic(
        &mut self,
        op: Arithmetic,
        left: NullableValue,
        right: NullableValue,
    ) -> NullableValue {
        let calculate = self.builder.create_block();
        let merge = self.builder.create_block();
        self.builder.append_block_param(merge, types::I64);
        self.builder.append_block_param(merge, types::I8);
        let null = self.constant(None);
        let is_null = self.builder.ins().bor(left.is_null, right.is_null);
        self.builder.ins().brif(
            is_null,
            merge,
            &[null.payload.into(), null.is_null.into()],
            calculate,
            &[],
        );
        self.builder.switch_to_block(calculate);
        let (payload, overflow) = match op {
            Arithmetic::Add => self
                .builder
                .ins()
                .sadd_overflow(left.payload, right.payload),
            Arithmetic::Subtract => self
                .builder
                .ins()
                .ssub_overflow(left.payload, right.payload),
            Arithmetic::Multiply => self
                .builder
                .ins()
                .smul_overflow(left.payload, right.payload),
        };
        let failed = self.builder.create_block();
        let not_null = self.builder.ins().iconst(types::I8, 0);
        self.builder.ins().brif(
            overflow,
            failed,
            &[],
            merge,
            &[payload.into(), not_null.into()],
        );
        self.builder.switch_to_block(failed);
        let operation = self.builder.ins().iconst(types::I64, op as i64);
        self.builder
            .ins()
            .store(MemFlags::new(), operation, self.error_record, 0);
        self.builder
            .ins()
            .store(MemFlags::new(), left.payload, self.error_record, 8);
        self.builder
            .ins()
            .store(MemFlags::new(), right.payload, self.error_record, 16);
        let status = self.builder.ins().iadd_imm(self.row, 1);
        self.builder.ins().return_(&[status]);
        self.builder.switch_to_block(merge);
        NullableValue {
            payload: self.builder.block_params(merge)[0],
            is_null: self.builder.block_params(merge)[1],
        }
    }

    fn coalesce(&mut self, children: &[usize]) -> NullableValue {
        let merge = self.builder.create_block();
        self.builder.append_block_param(merge, types::I64);
        self.builder.append_block_param(merge, types::I8);
        for &child in children {
            let value = self.emit(child);
            let next = self.builder.create_block();
            self.builder.ins().brif(
                value.is_null,
                next,
                &[],
                merge,
                &[value.payload.into(), value.is_null.into()],
            );
            self.builder.switch_to_block(next);
        }
        let null = self.constant(None);
        self.builder
            .ins()
            .jump(merge, &[null.payload.into(), null.is_null.into()]);
        self.builder.switch_to_block(merge);
        NullableValue {
            payload: self.builder.block_params(merge)[0],
            is_null: self.builder.block_params(merge)[1],
        }
    }
}
