use dbsp::Runtime;
use jit::{LlvmCircuitJit, RawJitBatch};

fn main() -> anyhow::Result<()> {
    let jit = LlvmCircuitJit::new("jit_demo");
    let compiled = jit.compile_add_pipeline(&[1, 2, 3])?;
    let mut scratch = 0_i32;
    let scratch_batch = unsafe { RawJitBatch::from_raw((&mut scratch as *mut i32).cast()) };
    compiled.invoke(scratch_batch);
    println!("Direct JIT invocation mutated scratch to {scratch}");

    let compiled_for_circuit = compiled.clone();

    let (mut dbsp, input_handle) = Runtime::init_circuit(1, move |circuit| {
        let (stream, handle) = circuit.add_input_stream::<RawJitBatch>();

        stream
            .invoke_jit("llvm-add", compiled_for_circuit.clone())
            .inspect(|batch| unsafe {
                let ptr = batch.as_ptr() as *const i32;
                if !ptr.is_null() {
                    println!("JIT produced value {ptr_value}", ptr_value = *ptr);
                }
            });

        Ok(handle)
    })?;

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

    dbsp.kill().unwrap();

    Ok(())
}