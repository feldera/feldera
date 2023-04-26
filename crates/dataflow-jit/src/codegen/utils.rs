use cranelift::{
    codegen::ir::{FuncRef, Inst},
    prelude::{types, Block, FunctionBuilder, InstBuilder, MemFlags, Type, Value},
};

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
}

/// Based off of rust's [`f32::total_cmp()`] and [`f64::total_cmp()`]
/// implementations
///
/// ```rust,ignore
/// // f32::total_cmp()
/// pub fn total_cmp(&self, other: &Self) -> Ordering {
///     let mut left = self.to_bits() as i32;
///     let mut right = other.to_bits() as i32;
///
///     // In case of negatives, flip all the bits except the sign
///     // to achieve a similar layout as two's complement integers
///     //
///     // Why does this work? IEEE 754 floats consist of three fields:
///     // Sign bit, exponent and mantissa. The set of exponent and mantissa
///     // fields as a whole have the property that their bitwise order is
///     // equal to the numeric magnitude where the magnitude is defined.
///     // The magnitude is not normally defined on NaN values, but
///     // IEEE 754 totalOrder defines the NaN values also to follow the
///     // bitwise order. This leads to order explained in the doc comment.
///     // However, the representation of magnitude is the same for negative
///     // and positive numbers – only the sign bit is different.
///     // To easily compare the floats as signed integers, we need to
///     // flip the exponent and mantissa bits in case of negative numbers.
///     // We effectively convert the numbers to "two's complement" form.
///     //
///     // To do the flipping, we construct a mask and XOR against it.
///     // We branchlessly calculate an "all-ones except for the sign bit"
///     // mask from negative-signed values: right shifting sign-extends
///     // the integer, so we "fill" the mask with sign bits, and then
///     // convert to unsigned to push one more zero bit.
///     // On positive values, the mask is all zeros, so it's a no-op.
///     left ^= (((left >> 31) as u32) >> 1) as i32;
///     right ^= (((right >> 31) as u32) >> 1) as i32;
///
///     left.cmp(&right)
/// }
/// ```
///
/// [`f32::total_cmp()`]: https://doc.rust-lang.org/std/primitive.f32.html#method.total_cmp
/// [`f64::total_cmp()`]: https://doc.rust-lang.org/std/primitive.f64.html#method.total_cmp
pub(super) fn normalize_float(float: Value, builder: &mut FunctionBuilder<'_>) -> Value {
    let ty = builder.func.dfg.value_type(float);
    let (int_ty, first_shift) = if ty == types::F32 {
        (types::I32, 31)
    } else if ty == types::F64 {
        (types::I64, 63)
    } else {
        unreachable!("normalize_float() can only be called on f32 and f64: {ty}")
    };

    // float.to_bits()
    // TODO: Should we apply any flags to this?
    let int = builder.ins().bitcast(int_ty, MemFlags::new(), float);

    // left >> {31, 63}
    let shifted = builder.ins().sshr_imm(int, first_shift);
    // ((left >> {31, 63}) as {u32, u64}) >> 1
    let shifted = builder.ins().ushr_imm(shifted, 1);

    // left ^= shifted
    builder.ins().bxor(int, shifted)
}
