use crate::codegen::NativeLayout;
use cranelift::{
    codegen::ir::{FuncRef, Inst},
    prelude::{types, Block, FunctionBuilder, InstBuilder, IntCC, MemFlags, Type, Value},
};
use std::{cmp::Ordering, slice, str};

pub(crate) trait FunctionBuilderExt {
    /// Seals the current basic block
    ///
    /// Panics if there's not currently a block
    fn seal_current(&mut self);

    // Creates an entry point block, adds function parameters and switches to the
    // created block
    fn create_entry_block(&mut self) -> Block;

    /// Creates an i8 value containing `true`
    fn true_byte(&mut self) -> Value;

    /// Creates an i8 value containing `false`
    fn false_byte(&mut self) -> Value;

    /// Calls `func` with the given arguments, returning its return value
    ///
    /// Panics if `func` doesn't return any values
    fn call_fn(&mut self, func: FuncRef, args: &[Value]) -> Value;

    fn value_def(&self, value: Value) -> Inst;

    fn value_type(&self, value: Value) -> Type;

    fn float_zero(&mut self, ty: Type) -> Value;

    fn float_one(&mut self, ty: Type) -> Value;

    fn float_pi(&mut self, ty: Type) -> Value;

    fn float_nan(&mut self, ty: Type) -> Value;

    fn const_u128(&mut self, value: u128) -> Value;
}

impl FunctionBuilderExt for FunctionBuilder<'_> {
    fn seal_current(&mut self) {
        self.seal_block(self.current_block().unwrap());
    }

    fn create_entry_block(&mut self) -> Block {
        let entry = self.create_block();
        self.switch_to_block(entry);
        self.append_block_params_for_function_params(entry);
        entry
    }

    fn true_byte(&mut self) -> Value {
        self.ins().iconst(types::I8, true as i64)
    }

    fn false_byte(&mut self) -> Value {
        self.ins().iconst(types::I8, false as i64)
    }

    fn call_fn(&mut self, func: FuncRef, args: &[Value]) -> Value {
        let call = self.ins().call(func, args);
        self.func.dfg.first_result(call)
    }

    fn value_def(&self, value: Value) -> Inst {
        self.func.dfg.value_def(value).unwrap_inst()
    }

    fn value_type(&self, value: Value) -> Type {
        self.func.dfg.value_type(value)
    }

    fn float_zero(&mut self, ty: Type) -> Value {
        match ty {
            types::F32 => self.ins().f32const(0.0),
            types::F64 => self.ins().f64const(0.0),
            other => unreachable!(
                "called `FunctionBuilderExt::float_zero()` with the non-float type {other}",
            ),
        }
    }

    fn float_one(&mut self, ty: Type) -> Value {
        match ty {
            types::F32 => self.ins().f32const(1.0),
            types::F64 => self.ins().f64const(1.0),
            other => unreachable!(
                "called `FunctionBuilderExt::float_one()` with the non-float type {other}",
            ),
        }
    }

    fn float_pi(&mut self, ty: Type) -> Value {
        match ty {
            types::F32 => self.ins().f32const(core::f32::consts::PI),
            types::F64 => self.ins().f64const(core::f64::consts::PI),
            other => unreachable!(
                "called `FunctionBuilderExt::float_pi()` with the non-float type {other}",
            ),
        }
    }

    fn float_nan(&mut self, ty: Type) -> Value {
        match ty {
            types::F32 => self.ins().f32const(f32::NAN),
            types::F64 => self.ins().f64const(f64::NAN),
            other => unreachable!(
                "called `FunctionBuilderExt::float_nan()` with the non-float type {other}",
            ),
        }
    }

    fn const_u128(&mut self, value: u128) -> Value {
        let bytes = value.to_ne_bytes();
        let low = u64::from_ne_bytes(bytes[..8].try_into().unwrap());
        let high = u64::from_ne_bytes(bytes[8..].try_into().unwrap());

        let low = self.ins().iconst(types::I64, low as i64);
        let high = self.ins().iconst(types::I64, high as i64);
        self.ins().iconcat(low, high)
    }
}

/// Checks if the given row is currently null, returns zero for non-null and
/// non-zero for null
// TODO: If we make sure that memory is zeroed (or at the very least that
// padding bytes are zeroed), we can simplify checks for null flags that are the
// only occupant of their given bitset. This'd allow us to go from
// `x = load; x1 = and x, 1` to just `x = load` for what should be a fairly
// common case. We could also do our best to distribute null flags across
// padding bytes when possible to try and make that happy path occur as much
// as possible
pub(super) fn column_non_null(
    column: usize,
    row_ptr: Value,
    layout: &NativeLayout,
    builder: &mut FunctionBuilder<'_>,
    readonly: bool,
) -> Value {
    debug_assert!(layout.is_nullable(column));

    // Create the flags for the load
    let mut flags = MemFlags::trusted();
    if readonly {
        flags.set_readonly();
    }

    if layout.column_type_of(column).is_string() {
        let ptr_ty = builder.value_type(row_ptr);
        let offset = layout.offset_of(column) as i32;

        let string = builder.ins().load(ptr_ty, flags, row_ptr, offset);
        builder.ins().icmp_imm(IntCC::Equal, string, 0)
    } else {
        let (bitset_ty, bitset_offset, bit_idx) = layout.nullability_of(column);
        let bitset_ty = bitset_ty.clif_type();

        // Load the bitset containing the given column's nullability
        let bitset = builder
            .ins()
            .load(bitset_ty, flags, row_ptr, bitset_offset as i32);

        builder.ins().band_imm(bitset, 1i64 << bit_idx)
    }
}

pub(super) fn set_column_null(
    is_null: Value,
    column: usize,
    dest: Value,
    dest_flags: MemFlags,
    layout: &NativeLayout,
    builder: &mut FunctionBuilder<'_>,
) {
    // If the value is null, set the cloned value to null
    // FIXME: This will panic if called on a nullable string column,
    // `.nullability_of()` doesn't handle string null niches
    let (bitset_ty, bitset_offset, bit_idx) = layout.nullability_of(column);
    let bitset_ty = bitset_ty.clif_type();

    let bitset = if layout.bitset_occupants(column) == 1 {
        let null_ty = builder.value_type(is_null);
        match bitset_ty.bytes().cmp(&null_ty.bytes()) {
            Ordering::Less => builder.ins().ireduce(bitset_ty, is_null),
            Ordering::Equal => is_null,
            Ordering::Greater => builder.ins().uextend(bitset_ty, is_null),
        }
    } else {
        // Load the bitset's current value
        let current_bitset = builder
            .ins()
            .load(bitset_ty, dest_flags, dest, bitset_offset as i32);

        let mask = 1 << bit_idx;
        let bitset_with_null = builder.ins().bor_imm(current_bitset, mask);
        let bitset_with_non_null = builder.ins().band_imm(current_bitset, !mask);

        builder
            .ins()
            .select(is_null, bitset_with_null, bitset_with_non_null)
    };

    // Store the newly modified bitset back into the row
    builder
        .ins()
        .store(dest_flags, bitset, dest, bitset_offset as i32);
}

#[inline(always)]
pub(crate) unsafe fn str_from_raw_parts<'a>(ptr: *const u8, len: usize) -> &'a str {
    let bytes = unsafe { slice::from_raw_parts(ptr, len) };
    debug_assert!(str::from_utf8(bytes).is_ok());
    unsafe { str::from_utf8_unchecked(bytes) }
}
