//! Return index groups and their visitor state. Java compares residual key
//! expressions in the same object namespace as the original circuit.
use crate::{BridgeError, MAX_BYTES, decode_live_circuit, live_equality, pass_warnings};
use feldera_sql_ir::{Circuit, CircuitEditor};
use feldera_sql_opt::{IndexDiscoveryError, KeyComparisonError, SharedIndexes};
use jni::{
    EnvUnowned,
    errors::ThrowRuntimeExAndDefault,
    objects::{JByteArray, JClass, JObject},
};
use serde_json::{Value, json};
use std::collections::BTreeSet;

fn optimizer_error(error: impl std::fmt::Display) -> BridgeError {
    BridgeError::Optimizer(format!("FindSharedIndexes: {error}"))
}

fn discovery_state(state: &SharedIndexes, visited: &BTreeSet<u64>, next: u64) -> Value {
    json!({
        "groups": state.groups,
        "visited": state.visited.difference(visited).collect::<Vec<_>>(),
        "warnings": pass_warnings(state.warnings.clone()),
        "next_generated_name": next,
    })
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
        let mut state = SharedIndexes::new(BTreeSet::clone(&visited));
        let discovery = state.discover(&mut edit, &mut next, |edit, keys, prefix, names| {
            compared_all_keys = false;
            let mut request = discovery_state(prefix, &visited, names);
            request["keys"] = Value::Array(
                edit.circuit()
                    .live_inner_roots(&input, &keys.map(Into::into))?,
            );
            live_equality::compare(env, &equality, &request)
        });
        let mut reply = discovery_state(&state, &visited, next);
        reply["compared_all_keys"] = compared_all_keys.into();
        match discovery {
            Ok(()) => {}
            Err(IndexDiscoveryError::Comparison(KeyComparisonError::Diagnostic(diagnostic))) => {
                reply["comparison_failure"] = json!(diagnostic)
            }
            Err(IndexDiscoveryError::FreeVariable(variable)) => {
                reply["binding_error"] = json!({"free": crate::inner_number(variable)});
            }
            Err(IndexDiscoveryError::ConflictingDeclarations {
                variable,
                previous,
                current,
            }) => {
                reply["binding_error"] = crate::binding_conflict(variable, previous, current);
            }
            Err(error) => return Err(optimizer_error(error)),
        }
        let bytes = serde_json::to_vec(&reply).map_err(optimizer_error)?;
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
        let input =
            decode_live_circuit(&env.convert_byte_array(&snapshot)?).map_err(replacement_error)?;
        let groups: Vec<_> = serde_json::from_slice(&env.convert_byte_array(&candidates)?)
            .map_err(replacement_error)?;
        let mut next = u64::try_from(next).map_err(replacement_error)?;
        let mut warnings = Vec::new();
        let mut native_columns = true;
        let output = feldera_sql_opt::replace_shared_indexes_with(
            input.clone(),
            &groups,
            &mut next,
            &mut warnings,
            |site| {
                native_columns = false;
                let request = live_equality::column_request(
                    site.graph,
                    &input,
                    &site.fields.map(Into::into),
                    site.warnings,
                    site.next_name,
                )?;
                live_equality::compare(env, &equality, &request).map(Some)
            },
        );
        let reply = live_equality::column_reply(output, &input, native_columns, next, warnings)
            .map_err(replacement_error)?;
        let bytes = serde_json::to_vec(&reply).map_err(replacement_error)?;
        if bytes.len() > MAX_BYTES {
            return Err(replacement_error("output exceeds 64 MiB"));
        }
        Ok(env.byte_array_from_slice(&bytes)?)
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
