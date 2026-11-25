//! Propagate the results of live closure analysis through the expanded graph.
use crate::{BridgeError, MAX_BYTES, operator_number};
use feldera_sql_ir::Circuit;
use feldera_sql_opt::propagate_waterlines;
use jni::{
    EnvUnowned, JValue,
    errors::ThrowRuntimeExAndDefault,
    jni_sig, jni_str,
    objects::{JByteArray, JClass, JObject},
};

// SAFETY: the signature matches RustCircuitPass.propagateWaterlines. Each
// callback's local frame owns its byte array. EnvUnowned preserves pending
// Java exceptions and contains panics at the native boundary.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_dbsp_sqlCompiler_compiler_backend_RustCircuitPass_propagateWaterlines<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    topology: JByteArray<'local>,
    step: JObject<'local>,
) {
    env.with_env(|env| -> Result<(), BridgeError> {
        if topology.len(env)? > MAX_BYTES {
            return Err(BridgeError::Optimizer(
                "Monotonicity topology exceeds 64 MiB".into(),
            ));
        }
        let graph = Circuit::from_json_bytes(&env.convert_byte_array(&topology)?)?;
        propagate_waterlines(&graph, |id, transfer| {
            let bytes = serde_json::to_vec(&transfer)
                .map_err(|error| BridgeError::Optimizer(error.to_string()))?;
            let bounded = env.with_local_frame(4, |env| {
                let rule = env.byte_array_from_slice(&bytes)?;
                env.call_method(
                    &step,
                    jni_str!("apply"),
                    jni_sig!("(J[B)Z"),
                    &[
                        JValue::Long(operator_number(id) as i64),
                        JValue::Object(&rule),
                    ],
                )?
                .z()
            })?;
            Ok(bounded)
        })
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
