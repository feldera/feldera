//! Return index groups and their visitor state. Java compares residual key
//! expressions in the same object namespace as the original circuit.
use crate::{BridgeError, MAX_BYTES, decode_live_circuit, live_equality, pass_warnings};
use feldera_sql_ir::{Circuit, CircuitEditor};
use jni::{
    EnvUnowned,
    errors::ThrowRuntimeExAndDefault,
    objects::{JByteArray, JClass, JObject},
};
use serde_json::{Value, json};

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
        let mut compared_all_keys = true;
        let groups =
            feldera_sql_opt::find_shared_indexes(&mut edit, &visited, &mut next, |edit, keys| {
                compared_all_keys = false;
                let keys = edit
                    .circuit()
                    .live_inner_roots(&input, &keys.map(Into::into))?;
                live_equality::compare(env, &equality, &serde_json::Value::Array(keys))
            })
            .map_err(optimizer_error)?;
        let bytes = serde_json::to_vec(&json!({
            "compared_all_keys": compared_all_keys,
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

fn replacement_error(message: impl std::fmt::Display) -> BridgeError {
    BridgeError::Optimizer(format!("ReplaceSharedIndexes: {message}"))
}

// SAFETY: the symbol and signature match RustCircuitPass.replaceIndexes.
// EnvUnowned contains panics and preserves exceptions from field comparisons.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_dbsp_sqlCompiler_compiler_backend_RustCircuitPass_replaceIndexes<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    snapshot: JByteArray<'local>,
    next: i64,
    candidates: JByteArray<'local>,
    equality: JObject<'local>,
) -> JByteArray<'local> {
    env.with_env(|env| -> Result<_, BridgeError> {
        if snapshot.len(env)?.saturating_add(candidates.len(env)?) > MAX_BYTES {
            return Err(replacement_error("input exceeds 64 MiB"));
        }
        let input = decode_live_circuit(&env.convert_byte_array(&snapshot)?).map_err(replacement_error)?;
        let groups: Vec<_> = serde_json::from_slice(&env.convert_byte_array(&candidates)?).map_err(replacement_error)?;
        let mut next = u64::try_from(next).map_err(replacement_error)?;
        let mut warnings = Vec::new();
        let mut native_columns = true;
        let output = feldera_sql_opt::replace_shared_indexes_with(input.clone(), &groups, &mut next, &mut warnings, |site| {
            native_columns = false;
            let request = live_equality::column_request(site.graph, &input, &site.fields.map(Into::into), site.warnings, site.next_name)?;
            live_equality::compare(env, &equality, &request).map(Some)
        }).map_err(replacement_error)?.ok_or_else(|| replacement_error("unanswered column comparison"))?;
        let mut reply = json!({"native_columns":native_columns, "diagnostics":pass_warnings(warnings),
            "next_generated_name":next, "unused_sources":[]});
        reply["circuit"] = output.changed_live_graph(&input).unwrap_or(Value::Null);
        let bytes = serde_json::to_vec(&reply).map_err(replacement_error)?;
        if bytes.len() > MAX_BYTES {
            return Err(replacement_error("output exceeds 64 MiB"));
        }
        Ok(env.byte_array_from_slice(&bytes)?)
    }).resolve::<ThrowRuntimeExAndDefault>()
}
