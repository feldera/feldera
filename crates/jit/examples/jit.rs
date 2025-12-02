use dbsp::{circuit::Stream, operator::InputHandle, RootCircuit, Runtime};
use jit::{JitFunction, LlvmCircuitJit, RawJitBatch};
use std::{ffi::c_void, mem};

/// A C-compatible struct to pass a stream and a handle across the FFI boundary.
#[repr(C)]
struct StreamHandlePair {
    stream: *mut c_void,
    handle: *mut c_void,
}

/// A C-compatible struct to pass a stream and a JIT function across the FFI boundary.
#[repr(C)]
struct StreamAndJitFunction {
    stream: *mut c_void,
    jit_function: *mut c_void,
}

/// An `extern "C"` function that wraps the `add_input_stream` call.
///
/// # Safety
///
/// This function is highly unsafe. It casts the `circuit` pointer and leaks the
/// created stream and handle to be passed back to the JIT-compiled caller.
/// The caller is responsible for reconstituting and managing the memory of
/// these objects.
#[no_mangle]
unsafe extern "C" fn add_input_stream_helper(circuit: *mut c_void) -> StreamHandlePair {
    let circuit = &mut *(circuit as *mut RootCircuit);
    let (stream, handle): (Stream<RootCircuit, RawJitBatch>, InputHandle<RawJitBatch>) =
        circuit.add_input_stream::<RawJitBatch>();

    StreamHandlePair {
        stream: Box::into_raw(Box::new(stream)).cast(),
        handle: Box::into_raw(Box::new(handle)).cast(),
    }
}

/// An `extern "C"` function that wraps the `invoke_jit` call.
///
/// # Safety
///
/// This function is highly unsafe. It casts pointers and leaks the created stream.
#[no_mangle]
unsafe extern "C" fn invoke_jit_helper(input: *mut StreamAndJitFunction) -> *mut c_void {
    let stream = &*(*(input)).stream.cast::<Stream<RootCircuit, RawJitBatch>>();
    let jit_function = &*(*(input)).jit_function.cast::<JitFunction>();
    let output_stream = stream.invoke_jit("llvm-add", jit_function.clone());
    Box::into_raw(Box::new(output_stream)).cast()
}

fn main() -> anyhow::Result<()> {
    // ---- Part 1: JIT-compile a simple data-parallel pipeline ----
    let jit = LlvmCircuitJit::new("jit_demo");
    let data_pipeline = jit.compile_add_pipeline(&[1, 2, 3])?;

    // ---- Part 2: JIT-compile a circuit construction function ----
    let context = jit.context();
    let ptr_type = context.ptr_type(inkwell::AddressSpace::default());
    let pair_struct_type = context.struct_type(&[ptr_type.into(), ptr_type.into()], false);

    let circuit_builder = unsafe {
        jit.compile_circuit_builder(
            add_input_stream_helper as usize,
            invoke_jit_helper as usize,
            Some(pair_struct_type),
        )?
    };

    type CircuitBuilderFn = unsafe extern "C" fn(*mut c_void, *mut c_void) -> StreamHandlePair;
    let circuit_builder_fn: CircuitBuilderFn = unsafe { mem::transmute(circuit_builder.raw()) };

    // ---- Part 3: Build and run the DBSP circuit ----
    let (mut dbsp, (input_handle, scratch_handle)) = Runtime::init_circuit(1, move |circuit| {
        // Use the JIT-compiled function to create the input stream and attach the JIT-compiled
        // pipeline.
        let circuit_ptr = circuit as *mut _ as *mut c_void;
        let data_pipeline_ptr = &data_pipeline as *const _ as *mut c_void;
        let pair = unsafe { circuit_builder_fn(circuit_ptr, data_pipeline_ptr) };

        // Reconstitute the stream and handle. This is the counterpart to the memory
        // leak in the helper function.
        let stream: Box<Stream<RootCircuit, RawJitBatch>> =
            unsafe { Box::from_raw(pair.stream.cast()) };
        let handle: Box<InputHandle<RawJitBatch>> = unsafe { Box::from_raw(pair.handle.cast()) };

        // Now, use the reconstituted stream to continue building the circuit.
        stream.inspect(|batch| unsafe {
            let ptr = batch.as_ptr() as *const i32;
            if !ptr.is_null() {
                println!("JIT produced value: {}", *ptr);
            }
        });

        // Create another input for testing.
        let (scratch_stream, scratch_handle) = circuit.add_input_stream::<i32>();
        scratch_stream.inspect(|val| println!("Scratch input received: {val}"));

        Ok((*handle, scratch_handle))
    })?;

    // ---- Part 4: Execute the circuit ----
    let mut first = 10_i32;
    let mut second = -5_i32;
    let batches = [
        unsafe { RawJitBatch::from_raw((&mut first as *mut i32).cast()) },
        unsafe { RawJitBatch::from_raw((&mut second as *mut i32).cast()) },
    ];

    for batch in batches {
        input_handle.set_for_all(batch);
        dbsp.transaction().expect("circuit transaction succeeds");
    }
    println!("Host view after JIT execution: first = {first}, second = {second}");

    // Demonstrate that the circuit is still live.
    input_handle.set_for_all(batches[0]);
    scratch_handle.set_for_all(100);
    dbsp.transaction().expect("circuit transaction succeeds");

    dbsp.kill().unwrap();

    Ok(())
}
