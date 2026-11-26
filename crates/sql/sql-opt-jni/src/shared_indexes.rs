//! Compare shared-index expressions and return the completed visitor state.
use crate::{check_transport_size, decode_live_circuit, live_equality, pass_warnings, BridgeError};
use feldera_sql_ir::{Circuit, CircuitEditor};
use feldera_sql_opt::{IndexComparison, IndexDiscoveryError, KeyComparisonError, SharedIndexes};
use jni::{
    errors::ThrowRuntimeExAndDefault,
    objects::{JByteArray, JClass, JObject},
    EnvUnowned,
};
use serde_json::{json, Value};
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
    operands: JObject<'local>,
) -> JByteArray<'local> {
    env.with_env(|env| -> Result<_, BridgeError> {
        let size = snapshot.len(env)? + visited.len(env)?;
        check_transport_size(size, "FindSharedIndexes", "input", env)?;
        let input = Circuit::from_json_bytes(&env.convert_byte_array(&snapshot)?)
            .map_err(optimizer_error)?;
        let visited =
            serde_json::from_slice(&env.convert_byte_array(&visited)?).map_err(optimizer_error)?;
        let mut next = u64::try_from(next).map_err(optimizer_error)?;
        let mut edit = CircuitEditor::new(input.clone());
        let mut state = SharedIndexes::new(BTreeSet::clone(&visited));
        let discovery = state.discover(&mut edit, &mut next, |edit, keys| {
            live_equality::compare(
                "FindSharedIndexes",
                env,
                &operands,
                edit.circuit(),
                &input,
                &keys.map(Into::into),
            )
        });
        let mut reply = discovery_state(&state, &visited, next);
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
        check_transport_size(bytes.len(), "FindSharedIndexes", "output", env)?;
        Ok(env.byte_array_from_slice(&bytes)?)
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

/// An absent fixed-width list requests the final layout. A supplied list
/// requests the ordered partition used to choose compatible readers.
fn plan_fields(
    arena: &Circuit,
    functions: &[feldera_sql_ir::NodeId],
    fixed: Option<&[bool]>,
    next: &mut u64,
    warnings: &mut Vec<feldera_sql_expr::SimplifyWarning>,
    compare: &mut feldera_sql_opt::CompareColumn,
) -> Result<Value, KeyComparisonError> {
    use feldera_sql_ir::{Error, NodeId};
    let unanswered = || Error::new("live shared-index comparison returned no answer");
    if let Some(fixed) = fixed {
        let groups = feldera_sql_opt::split_shared_indexes_with(
            arena, functions, fixed, next, warnings, compare,
        )?
        .ok_or_else(unanswered)?;
        return Ok(json!({"groups": groups}));
    }
    let mut edit = CircuitEditor::new(arena.clone());
    let index =
        feldera_sql_opt::project_shared_index(&mut edit, functions, next, warnings, compare)?
            .ok_or_else(unanswered)?;
    let roots = [index.variable, index.key]
        .into_iter()
        .chain(index.layout.columns)
        .map(NodeId::from)
        .collect::<Vec<_>>();
    Ok(
        json!({"expressions": edit.circuit().live_inner_roots(arena, &roots)?,
              "layout": index.layout.positions, "nullable": index.nullable}),
    )
}

// SAFETY: the symbol and signature match RustCircuitPass.planIndexFields.
// EnvUnowned contains panics and preserves pending exceptions from the comparator.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_dbsp_sqlCompiler_compiler_backend_RustCircuitPass_planIndexFields<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    functions: JByteArray<'local>,
    next: i64,
    fixed: JByteArray<'local>,
    operands: JObject<'local>,
) -> JByteArray<'local> {
    env.with_env(|env| -> Result<_, BridgeError> {
        let error =
            |message: String| BridgeError::Optimizer(format!("shared-index layout: {message}"));
        let fixed_size = fixed.len(env)?;
        let pass = if fixed_size == b"null".len() && env.convert_byte_array(&fixed)? == b"null" {
            "SharedIndexes/WideFields"
        } else {
            "SharedIndexes/Split"
        };
        check_transport_size(functions.len(env)? + fixed_size, pass, "input", env)?;
        let input = Circuit::from_json(
            json!({"class":"DBSPCircuit", "id":0, "declarations":[], "allOperators":[]}),
        )
        .map_err(|e| error(e.to_string()))?;
        let (arena, roots) = crate::lowering_arguments(input, &env.convert_byte_array(&functions)?)
            .map_err(|e| error(e.to_string()))?;
        let fixed: Option<Vec<bool>> = serde_json::from_slice(&env.convert_byte_array(&fixed)?)
            .map_err(|e| error(e.to_string()))?;
        let mut next = u64::try_from(next).map_err(|e| error(e.to_string()))?;
        let mut warnings = Vec::new();
        let result = plan_fields(
            &arena,
            &roots,
            fixed.as_deref(),
            &mut next,
            &mut warnings,
            &mut |site: IndexComparison| {
                live_equality::compare(
                    pass,
                    env,
                    &operands,
                    site.graph,
                    &arena,
                    &site.fields.map(Into::into),
                )
                .map(Some)
            },
        );
        let mut reply = match result {
            Ok(reply) => reply,
            Err(KeyComparisonError::Diagnostic(diagnostic)) => {
                json!({"comparison_failure": diagnostic})
            }
            Err(KeyComparisonError::InvalidIr(e)) => return Err(error(e.to_string())),
        };
        reply["warnings"] = json!(pass_warnings(warnings));
        reply["next_generated_name"] = next.into();
        let bytes = serde_json::to_vec(&reply).map_err(|e| error(e.to_string()))?;
        check_transport_size(bytes.len(), pass, "output", env)?;
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
    operands: JObject<'local>,
) -> JByteArray<'local> {
    env.with_env(|env| -> Result<_, BridgeError> {
        let size = snapshot.len(env)? + candidates.len(env)?;
        check_transport_size(size, "ReplaceSharedIndexes", "input", env)?;
        let input =
            decode_live_circuit(&env.convert_byte_array(&snapshot)?).map_err(replacement_error)?;
        let groups: Vec<_> = serde_json::from_slice(&env.convert_byte_array(&candidates)?)
            .map_err(replacement_error)?;
        let mut next = u64::try_from(next).map_err(replacement_error)?;
        let mut warnings = Vec::new();
        let output = feldera_sql_opt::replace_shared_indexes_with(
            input.clone(),
            &groups,
            &mut next,
            &mut warnings,
            |site| {
                live_equality::compare(
                    "ReplaceSharedIndexes",
                    env,
                    &operands,
                    site.graph,
                    &input,
                    &site.fields.map(Into::into),
                )
                .map(Some)
            },
        );
        let reply = live_equality::column_reply(output, &input, next, warnings)
            .map_err(replacement_error)?;
        let bytes = serde_json::to_vec(&reply).map_err(replacement_error)?;
        check_transport_size(bytes.len(), "ReplaceSharedIndexes", "output", env)?;
        Ok(env.byte_array_from_slice(&bytes)?)
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
