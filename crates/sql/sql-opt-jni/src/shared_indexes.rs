//! Return index groups and their visitor state. Java compares residual key
//! expressions in the same object namespace as the original circuit.
use crate::{BridgeError, MAX_BYTES};
use feldera_sql_ir::{Circuit, CircuitEditor, Error};
use jni::{
    errors::ThrowRuntimeExAndDefault,
    jni_sig, jni_str,
    objects::{JByteArray, JClass, JObject},
    EnvUnowned, JValue,
};
use serde_json::json;

fn optimizer_error(error: impl std::fmt::Display) -> BridgeError {
    BridgeError::Optimizer(format!("FindSharedIndexes: {error}"))
}

// SAFETY: the symbol and signature match the static native method on RustCircuitPass.
// EnvUnowned contains panics at the JVM boundary.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_dbsp_sqlCompiler_compiler_backend_RustCircuitPass_discoverIndexes<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    snapshot: JByteArray<'local>,
    next: i64,
    visited: JByteArray<'local>,
    equality: JObject<'local>,
) -> JByteArray<'local> {
    env.with_env(|env| -> Result<_, BridgeError> {
        if snapshot.len(env)?.saturating_add(visited.len(env)?) > MAX_BYTES {
            return Err(optimizer_error("input exceeds 64 MiB"));
        }
        let input = Circuit::from_json_bytes(&env.convert_byte_array(&snapshot)?)
            .map_err(optimizer_error)?;
        let visited =
            serde_json::from_slice(&env.convert_byte_array(&visited)?).map_err(optimizer_error)?;
        let mut next = u64::try_from(next).map_err(optimizer_error)?;
        let mut edit = CircuitEditor::new(input.clone());
        let groups =
            feldera_sql_opt::find_shared_indexes(&mut edit, &visited, &mut next, |edit, keys| {
                let keys = edit.circuit().additional_expressions(&input, &keys)?;
                let bytes = serde_json::to_vec(&keys).map_err(|e| Error::new(e.to_string()))?;
                if bytes.len() > MAX_BYTES {
                    return Err(Error::new("index key expressions exceed 64 MiB"));
                }
                env.with_local_frame(4, |env| -> jni::errors::Result<bool> {
                    let pair = env.byte_array_from_slice(&bytes)?;
                    env.call_method(
                        &equality,
                        jni_str!("equivalent"),
                        jni_sig!("([B)Z"),
                        &[JValue::Object(&pair)],
                    )?
                    .z()
                })
                .map_err(|e| Error::new(e.to_string()))
            })
            .map_err(optimizer_error)?;
        let bytes = serde_json::to_vec(&json!({
            "groups": groups.groups,
            "visited": groups.visited.difference(&visited).collect::<Vec<_>>(),
            "warnings": groups.warnings.into_iter().map(|w| json!({
                "range": w.position, "error_type": w.kind, "message": w.message
            })).collect::<Vec<_>>(),
            "next_generated_name": next,
        }))
        .map_err(optimizer_error)?;
        if bytes.len() > MAX_BYTES {
            return Err(optimizer_error("output exceeds 64 MiB"));
        }
        Ok(env.byte_array_from_slice(&bytes)?)
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
