use cranelift::{
    codegen::ir::FuncRef,
    prelude::{types, Block, FunctionBuilder, InstBuilder, Value},
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
}
