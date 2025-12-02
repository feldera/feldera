//! Prototype LLVM-backed JIT helpers.

use std::{
    ffi::c_void,
    fmt, ptr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use inkwell::{
    context::Context,
    execution_engine::ExecutionEngine,
    module::Module,
    types::{BasicMetadataTypeEnum, BasicType},
    AddressSpace, OptimizationLevel,
};
use thiserror::Error;

/// Signature of the generated JIT functions.
pub type JitFn = unsafe extern "C" fn(*mut c_void) -> *mut c_void;

/// A minimal raw batch wrapper.  The pointer is opaque to DBSP; JIT'd code owns
/// the layout and interpretation.
#[derive(Copy, Clone, Eq, PartialEq)]
#[repr(transparent)]
pub struct RawJitBatch {
    ptr: *mut c_void,
}

impl RawJitBatch {
    /// Create a batch that points to `ptr`.
    ///
    /// # Safety
    ///
    /// `ptr` must remain valid for as long as the batch is used by the circuit.
    pub const unsafe fn from_raw(ptr: *mut c_void) -> Self {
        Self { ptr }
    }

    /// A null pointer batch (the default).
    pub const fn null() -> Self {
        Self {
            ptr: ptr::null_mut(),
        }
    }

    /// Expose the inner pointer.
    pub const fn as_ptr(self) -> *mut c_void {
        self.ptr
    }
}

unsafe impl Send for RawJitBatch {}
unsafe impl Sync for RawJitBatch {}

impl Default for RawJitBatch {
    fn default() -> Self {
        Self::null()
    }
}

impl fmt::Debug for RawJitBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RawJitBatch")
            .field(&(self.ptr as usize))
            .finish()
    }
}

/// Errors surfaced while building or running JIT code.
#[derive(Debug, Error)]
pub enum JitError {
    #[error("LLVM module verification failed: {0}")]
    Verify(String),
    #[error("LLVM instruction builder failed: {0}")]
    Build(String),
    #[error("failed to create execution engine: {0}")]
    Engine(String),
    #[error("failed to find symbol `{symbol}`: {error}")]
    Lookup { symbol: String, error: String },
}

/// Lightweight owner for LLVM artifacts created per compiled function.
#[doc(hidden)]
pub struct SharedJitModule {
    module: *mut Module<'static>,
    execution_engine: *mut ExecutionEngine<'static>,
}

impl SharedJitModule {
    fn new(module: Module<'static>, execution_engine: ExecutionEngine<'static>) -> Self {
        Self {
            module: Box::into_raw(Box::new(module)),
            execution_engine: Box::into_raw(Box::new(execution_engine)),
        }
    }
}

unsafe impl Send for SharedJitModule {}
unsafe impl Sync for SharedJitModule {}

impl Drop for SharedJitModule {
    fn drop(&mut self) {
        unsafe {
            if !self.execution_engine.is_null() {
                drop(Box::from_raw(self.execution_engine));
                self.execution_engine = ptr::null_mut();
            }
            if !self.module.is_null() {
                drop(Box::from_raw(self.module));
                self.module = ptr::null_mut();
            }
        }
    }
}

/// Handle to a compiled JIT function.
#[derive(Clone)]
pub struct JitFunction {
    symbol: Arc<str>,
    func: JitFn,
    _keepalive: Arc<SharedJitModule>,
}

impl JitFunction {
    /// This is not meant to be a public API, but `LlvmCircuitJit` needs it.
    #[doc(hidden)]
    pub fn new(symbol: Arc<str>, func: JitFn, keepalive: SharedJitModule) -> Self {
        Self {
            symbol,
            func,
            _keepalive: Arc::new(keepalive),
        }
    }

    /// Human-readable symbol of the compiled function.
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// Obtain the raw function pointer.
    pub fn raw(&self) -> JitFn {
        self.func
    }

    /// Invoke the compiled function on `batch`.
    pub fn invoke(&self, batch: RawJitBatch) -> RawJitBatch {
        let ptr = unsafe { (self.func)(batch.as_ptr()) };
        unsafe { RawJitBatch::from_raw(ptr) }
    }
}

/// Microscopic LLVM JIT wrapper that fabricates pointer-manipulating kernels.
pub struct LlvmCircuitJit {
    context: &'static Context,
    module_prefix: Arc<str>,
    counter: AtomicUsize,
    optimization: OptimizationLevel,
}

impl LlvmCircuitJit {
    /// Create a new JIT with modules prefixed by `module_prefix`.
    pub fn new(module_prefix: impl Into<String>) -> Self {
        let ctx = Box::leak(Box::new(Context::create()));
        Self {
            context: ctx,
            module_prefix: module_prefix.into().into(),
            counter: AtomicUsize::new(0),
            optimization: OptimizationLevel::None,
        }
    }

    pub fn context(&self) -> &'static Context {
        self.context
    }

    /// Compile a function that interprets the raw pointer as an `i32` buffer and
    /// adds all `increments` to the pointed-to value in sequence.
    pub fn compile_add_pipeline(&self, increments: &[i32]) -> Result<JitFunction, JitError> {
        let symbol_name = format!(
            "{}_fn_{}",
            self.module_prefix,
            self.counter.fetch_add(1, Ordering::Relaxed)
        );
        let module_name = format!("{}_module", symbol_name);
        let module = self.context.create_module(&module_name);
        let builder = self.context.create_builder();

        let ptr_ty = self.context.ptr_type(AddressSpace::default());
        let fn_ty = ptr_ty.fn_type(&[ptr_ty.into()], false);
        let function = module.add_function(&symbol_name, fn_ty, None);
        let entry = self.context.append_basic_block(function, "entry");
        builder.position_at_end(entry);

        let raw_arg = function.get_first_param().unwrap().into_pointer_value();
        let i32_ty = self.context.i32_type();
        let typed_ptr = builder
            .build_bit_cast(
                raw_arg,
                self.context.ptr_type(AddressSpace::default()),
                "batch_ptr",
            )
            .map_err(|e| JitError::Build(e.to_string()))?
            .into_pointer_value();

        let mut acc = builder
            .build_load(i32_ty, typed_ptr, "current")
            .map_err(|e| JitError::Build(e.to_string()))?
            .into_int_value();

        for (idx, increment) in increments.iter().copied().enumerate() {
            let llvm_inc = i32_ty.const_int(increment as u64, true);
            acc = builder
                .build_int_add(acc, llvm_inc, &format!("add_{idx}"))
                .map_err(|e| JitError::Build(e.to_string()))?;
        }

        builder
            .build_store(typed_ptr, acc)
            .map_err(|e| JitError::Build(e.to_string()))?;
        builder
            .build_return(Some(&raw_arg))
            .map_err(|e| JitError::Build(e.to_string()))?;

        module
            .verify()
            .map_err(|e| JitError::Verify(e.to_string()))?;

        let execution_engine = module
            .create_jit_execution_engine(self.optimization)
            .map_err(|e| JitError::Engine(e.to_string()))?;

        let address = execution_engine
            .get_function_address(&symbol_name)
            .map_err(|e| JitError::Lookup {
                symbol: symbol_name.clone(),
                error: e.to_string(),
            })?;

        let func: JitFn = unsafe { std::mem::transmute(address) };
        Ok(JitFunction::new(
            Arc::<str>::from(symbol_name),
            func,
            SharedJitModule::new(module, execution_engine),
        ))
    }

    /// Compile a JIT function that wraps a call to an arbitrary function pointer.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `target_fn_ptr` is a valid function pointer
    /// that will remain valid for the lifetime of the returned `JitFunction`.
    /// The signature of the `target_fn_ptr` must match what the generated LLVM
    /// IR expects. This is a sharp tool for advanced FFI scenarios.
    pub unsafe fn compile_ffi_call(
        &self,
        target_fn_ptr: usize,
        param_types: &[inkwell::types::BasicTypeEnum<'static>],
        return_type: inkwell::types::BasicTypeEnum<'static>,
        return_struct_type: Option<inkwell::types::StructType<'static>>,
    ) -> Result<JitFunction, JitError> {
        let symbol_name = format!(
            "{}_fn_{}",
            self.module_prefix,
            self.counter.fetch_add(1, Ordering::Relaxed)
        );
        let module_name = format!("{}_module", symbol_name);
        let module = self.context.create_module(&module_name);
        let builder = self.context.create_builder();

        let metadata_param_types: Vec<BasicMetadataTypeEnum> =
            param_types.iter().map(|&t| t.into()).collect();

        // Create the JIT function's signature.
        let fn_ty = if let Some(struct_type) = return_struct_type {
            struct_type.fn_type(&metadata_param_types, false)
        } else {
            return_type.fn_type(&metadata_param_types, false)
        };
        let function = module.add_function(&symbol_name, fn_ty, None);
        let entry = self.context.append_basic_block(function, "entry");
        builder.position_at_end(entry);

        // Get the function pointer for the target C function.
        let ptr_ty = self.context.ptr_type(AddressSpace::default());
        let target_fn_ptr_val = self
            .context
            .i64_type()
            .const_int(target_fn_ptr as u64, false);
        let target_fn = builder
            .build_int_to_ptr(target_fn_ptr_val, ptr_ty, "target_fn_ptr")
            .unwrap();

        // Build the call instruction.
        let params: Vec<_> = function
            .get_param_iter()
            .map(|param| param.into())
            .collect();
        let call = builder
            .build_indirect_call(fn_ty, target_fn, &params, "call")
            .unwrap();

        // Build the return instruction. A function returns void if `get_return_type()` is `None`.
        if fn_ty.get_return_type().is_some() {
            let return_value = call.try_as_basic_value().unwrap_basic();
            builder.build_return(Some(&return_value)).unwrap();
        } else {
            builder.build_return(None).unwrap();
        }

        module
            .verify()
            .map_err(|e| JitError::Verify(e.to_string()))?;

        let execution_engine = module
            .create_jit_execution_engine(self.optimization)
            .map_err(|e| JitError::Engine(e.to_string()))?;

        let address = execution_engine
            .get_function_address(&symbol_name)
            .map_err(|e| JitError::Lookup {
                symbol: symbol_name.clone(),
                error: e.to_string(),
            })?;

        let func: JitFn = std::mem::transmute(address);
        Ok(JitFunction::new(
            Arc::<str>::from(symbol_name),
            func,
            SharedJitModule::new(module, execution_engine),
        ))
    }

    /// This is a temporary function to compile a circuit builder.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `add_input_stream_fn_ptr` and `invoke_jit_fn_ptr` are valid
    /// function pointers that will remain valid for the lifetime of the returned `JitFunction`.
    /// The signatures of these function pointers must match what the generated LLVM IR expects.
    pub unsafe fn compile_circuit_builder(
        &self,
        add_input_stream_fn_ptr: usize,
        invoke_jit_fn_ptr: usize,
        return_struct_type: Option<inkwell::types::StructType<'static>>,
    ) -> Result<JitFunction, JitError> {
        let symbol_name = format!(
            "{}_fn_{}",
            self.module_prefix,
            self.counter.fetch_add(1, Ordering::Relaxed)
        );
        let module_name = format!("{}_module", symbol_name);
        let module = self.context.create_module(&module_name);
        let builder = self.context.create_builder();

        let ptr_type = self.context.ptr_type(AddressSpace::default());

        // JIT function signature
        let fn_type = return_struct_type.unwrap().fn_type(
            &[ptr_type.into(), ptr_type.into()],
            false,
        );
        let function = module.add_function(&symbol_name, fn_type, None);
        let entry = self.context.append_basic_block(function, "entry");
        builder.position_at_end(entry);

        // Call `add_input_stream_helper`
        let add_input_stream_fn_type =
            return_struct_type.unwrap().fn_type(&[ptr_type.into()], false);
        let add_input_stream_fn_ptr_val = self
            .context
            .i64_type()
            .const_int(add_input_stream_fn_ptr as u64, false);
        let add_input_stream_fn = builder
            .build_int_to_ptr(
                add_input_stream_fn_ptr_val,
                ptr_type,
                "add_input_stream_fn_ptr",
            )
            .unwrap();
        let circuit_arg = function.get_first_param().unwrap();
        let stream_handle_pair = builder
            .build_indirect_call(
                add_input_stream_fn_type,
                add_input_stream_fn,
                &[circuit_arg.into()],
                "call_add_input_stream",
            )
            .unwrap()
            .try_as_basic_value()
            .unwrap_basic()
            .into_struct_value();

        // Call `invoke_jit_helper`
        let invoke_jit_fn_type = ptr_type.fn_type(&[ptr_type.into()], false);
        let invoke_jit_fn_ptr_val = self
            .context
            .i64_type()
            .const_int(invoke_jit_fn_ptr as u64, false);
        let invoke_jit_fn = builder
            .build_int_to_ptr(invoke_jit_fn_ptr_val, ptr_type, "invoke_jit_fn_ptr")
            .unwrap();

        let stream_ptr = builder
            .build_extract_value(stream_handle_pair, 0, "stream_ptr")
            .unwrap();
        let handle_ptr = builder
            .build_extract_value(stream_handle_pair, 1, "handle_ptr")
            .unwrap();
        let data_pipeline_ptr = function.get_nth_param(1).unwrap();

        // Create `StreamAndJitFunction` struct
        let stream_and_jit_function_type =
            self.context.struct_type(&[ptr_type.into(), ptr_type.into()], false);
        let stream_and_jit_function_ptr = builder
            .build_alloca(stream_and_jit_function_type, "arg_struct")
            .unwrap();
        let stream_field_ptr = builder
            .build_struct_gep(
                stream_and_jit_function_type,
                stream_and_jit_function_ptr,
                0,
                "stream_field_ptr",
            )
            .unwrap();
        let jit_function_field_ptr = builder
            .build_struct_gep(
                stream_and_jit_function_type,
                stream_and_jit_function_ptr,
                1,
                "jit_function_field_ptr",
            )
            .unwrap();
        builder.build_store(stream_field_ptr, stream_ptr).unwrap();
        builder
            .build_store(jit_function_field_ptr, data_pipeline_ptr)
            .unwrap();

        let output_stream_ptr = builder
            .build_indirect_call(
                invoke_jit_fn_type,
                invoke_jit_fn,
                &[stream_and_jit_function_ptr.into()],
                "call_invoke_jit",
            )
            .unwrap()
            .try_as_basic_value()
            .unwrap_basic();

        // Return the final stream and handle
        let final_pair = return_struct_type.unwrap().const_zero();
        let final_pair = builder
            .build_insert_value(final_pair, output_stream_ptr, 0, "final_pair")
            .unwrap();
        let final_pair = builder
            .build_insert_value(final_pair, handle_ptr, 1, "final_pair")
            .unwrap();
        builder.build_return(Some(&final_pair)).unwrap();

        module
            .verify()
            .map_err(|e| JitError::Verify(e.to_string()))?;

        let execution_engine = module
            .create_jit_execution_engine(self.optimization)
            .map_err(|e| JitError::Engine(e.to_string()))?;

        let address = execution_engine
            .get_function_address(&symbol_name)
            .map_err(|e| JitError::Lookup {
                symbol: symbol_name.clone(),
                error: e.to_string(),
            })?;

        let func: JitFn = std::mem::transmute(address);
        Ok(JitFunction::new(
            Arc::<str>::from(symbol_name),
            func,
            SharedJitModule::new(module, execution_engine),
        ))
    }
}
