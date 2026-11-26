//! Recover original operand metadata for a comparison of rewritten expressions.
use crate::{check_transport_size, decode_live_circuit, pass_warnings, BridgeError};
use feldera_sql_expr::SimplifyWarning;
use feldera_sql_ir::{Circuit, Error, NodeId};
use feldera_sql_opt::{ColumnRewriteError, KeyComparisonError};
use jni::{
    errors::ThrowRuntimeExAndDefault,
    jni_sig, jni_str,
    objects::{JByteArray, JClass, JObject},
    Env, EnvUnowned, JValue,
};
use serde_json::{json, Value};

pub(super) fn compare(
    pass: &str,
    env: &mut Env,
    operands: &JObject,
    graph: &Circuit,
    input: &Circuit,
    fields: &[NodeId],
) -> Result<bool, KeyComparisonError> {
    let roots = graph.live_inner_roots(input, fields)?;
    let bytes = serde_json::to_vec(&roots).map_err(|e| Error::new(e.to_string()))?;
    let bytes = env
        .with_local_frame(4, |env| -> jni::errors::Result<Vec<u8>> {
            check_transport_size(bytes.len(), pass, "comparison request", env)?;
            let request = env.byte_array_from_slice(&bytes)?;
            let result = env
                .call_method(
                    operands,
                    jni_str!("capture"),
                    jni_sig!("([B)[B"),
                    &[JValue::Object(&request)],
                )?
                .l()?;
            let result = JByteArray::cast_local(env, result)?;
            check_transport_size(result.len(env)?, pass, "comparison reply", env)?;
            env.convert_byte_array(&result)
        })
        .map_err(|e| Error::new(e.to_string()))?;
    let base = Circuit::from_json(
        json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]}),
    )?;
    let (arena, roots) =
        crate::lowering_arguments(base, &bytes).map_err(|e| Error::new(e.to_string()))?;
    feldera_sql_opt::compare_live_operands(&feldera_sql_ir::CircuitEditor::new(arena), &roots)
}

/// A failed lookup carries exactly the state accumulated at that column.
pub(super) fn column_reply(
    output: Result<Option<Circuit>, ColumnRewriteError>,
    input: &Circuit,
    next_name: u64,
    warnings: Vec<SimplifyWarning>,
) -> Result<Value, Error> {
    match output {
        Ok(Some(circuit)) => Ok(json!({
            "circuit": circuit.changed_live_graph(input),
            "diagnostics": pass_warnings(warnings),
            "next_generated_name": next_name,
            "unused_sources": [],
        })),
        Ok(None) => Err(Error::new("unanswered column comparison")),
        Err(ColumnRewriteError::Comparison {
            diagnostic,
            next_name,
            warnings,
        }) => Ok(json!({
            "comparison_failure": diagnostic,
            "warnings": pass_warnings(warnings),
            "next_generated_name": next_name,
        })),
        Err(ColumnRewriteError::InvalidIr(error)) => Err(error),
    }
}

fn fusion_error(error: impl std::fmt::Display) -> BridgeError {
    BridgeError::Optimizer(format!("Live/Fuse: {error}"))
}

// SAFETY: the symbol and signature match RustCircuitPass.fuseMaps. EnvUnowned
// contains panics at the JVM boundary and preserves a pending Java exception.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_dbsp_sqlCompiler_compiler_backend_RustCircuitPass_fuseMaps<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    snapshot: JByteArray<'local>,
    next: i64,
    clusters: JByteArray<'local>,
    operands: JObject<'local>,
) -> JByteArray<'local> {
    env.with_env(|env| -> Result<_, BridgeError> {
        check_transport_size(
            snapshot.len(env)? + clusters.len(env)?,
            "Live/Fuse",
            "input",
            env,
        )?;
        let circuit =
            decode_live_circuit(&env.convert_byte_array(&snapshot)?).map_err(fusion_error)?;
        let clusters =
            serde_json::from_slice(&env.convert_byte_array(&clusters)?).map_err(fusion_error)?;
        let mut next = u64::try_from(next).map_err(fusion_error)?;
        let mut warnings = Vec::new();
        let output = feldera_sql_opt::fuse_expensive_maps_with(
            circuit.clone(),
            clusters,
            &mut next,
            &mut warnings,
            |site| {
                let [left, right] = site.fields;
                compare(
                    "Live/Fuse",
                    env,
                    &operands,
                    site.graph,
                    site.input,
                    &[site.row.into(), left.into(), right.into()],
                )
                .map(Some)
            },
        );
        let result = column_reply(output, &circuit, next, warnings).map_err(fusion_error)?;
        let bytes = serde_json::to_vec(&result).map_err(fusion_error)?;
        check_transport_size(bytes.len(), "Live/Fuse", "output", env)?;
        Ok(env.byte_array_from_slice(&bytes)?)
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
