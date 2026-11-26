//! Propagate the results of live closure analysis through the expanded graph.
use crate::{check_transport_size, operator_number, BridgeError};
use feldera_sql_ir::Circuit;
use feldera_sql_opt::{propagate_waterlines, OutputWaterline, TransferOutcome};
use jni::{
    errors::ThrowRuntimeExAndDefault,
    jni_sig, jni_str,
    objects::{JByteArray, JClass, JObject},
    EnvUnowned, JValue,
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
        check_transport_size(topology.len(env)?, "Monotonicity", "topology", env)?;
        let graph = Circuit::from_json_bytes(&env.convert_byte_array(&topology)?)?;
        propagate_waterlines(&graph, |id, transfer| {
            let bytes = serde_json::to_vec(&transfer)
                .map_err(|error| BridgeError::Optimizer(error.to_string()))?;
            let bounded = env.with_local_frame(4, |env| {
                let rule = env.byte_array_from_slice(&bytes)?;
                env.call_method(
                    &step,
                    jni_str!("apply"),
                    jni_sig!("(J[B)I"),
                    &[
                        JValue::Long(operator_number(id) as i64),
                        JValue::Object(&rule),
                    ],
                )?
                .i()
            })?;
            // Wire values are explicit in RustCircuitPass.WaterlineResult.
            match bounded {
                0 => Ok(OutputWaterline::Absent.into()),
                1 => Ok(OutputWaterline::Row.into()),
                2 => Ok(OutputWaterline::Key.into()),
                3 => Ok(OutputWaterline::Value.into()),
                4 => Ok(OutputWaterline::KeyAndValue.into()),
                5 => Ok(TransferOutcome::TryGroupKey),
                _ => Err(BridgeError::Optimizer(format!(
                    "invalid output waterline {bounded}"
                ))),
            }
        })
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
