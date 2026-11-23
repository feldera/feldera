//! Delegate a single unresolved expression comparison to the live compiler.
use crate::{BridgeError, MAX_BYTES, decode_live_circuit, pass_warnings};
use feldera_sql_expr::SimplifyWarning;
use feldera_sql_ir::{Circuit, Error, NodeId};
use feldera_sql_opt::ColumnRewriteError;
use jni::{
    Env, EnvUnowned, JValue,
    errors::ThrowRuntimeExAndDefault,
    jni_sig, jni_str,
    objects::{JByteArray, JClass, JObject},
};
use serde_json::{Value, json};

pub(super) fn compare(env: &mut Env, equality: &JObject, payload: &Value) -> Result<bool, Error> {
    let bytes = serde_json::to_vec(payload).map_err(|e| Error::new(e.to_string()))?;
    if bytes.len() > MAX_BYTES {
        return Err(Error::new("expression comparison exceeds 64 MiB"));
    }
    env.with_local_frame(4, |env| -> jni::errors::Result<bool> {
        let args = env.byte_array_from_slice(&bytes)?;
        env.call_method(
            equality,
            jni_str!("equivalent"),
            jni_sig!("([B)Z"),
            &[JValue::Object(&args)],
        )?
        .z()
    })
    .map_err(|e| Error::new(e.to_string()))
}

/// Encode all operands with one ID namespace. On failure the Java callback
/// publishes the warnings and allocator position preceding this comparison.
pub(super) fn column_request(
    graph: &Circuit,
    input: &Circuit,
    fields: &[NodeId],
    warnings: &[SimplifyWarning],
    next_name: u64,
) -> Result<Value, Error> {
    let roots = graph.live_inner_roots(input, fields)?;
    Ok(
        json!({"roots":roots, "warnings":pass_warnings(warnings.to_vec()), "next_generated_name":next_name}),
    )
}

/// A failed lookup carries exactly the state accumulated at that column.
pub(super) fn column_reply(
    output: Result<Option<Circuit>, ColumnRewriteError>,
    input: &Circuit,
    native_columns: bool,
    next_name: u64,
    warnings: Vec<SimplifyWarning>,
) -> Result<Value, Error> {
    match output {
        Ok(Some(circuit)) => Ok(json!({
            "circuit": circuit.changed_live_graph(input),
            "native_columns": native_columns,
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
            "native_columns": native_columns,
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
    equality: JObject<'local>,
) -> JByteArray<'local> {
    env.with_env(|env| -> Result<_, BridgeError> {
        if snapshot.len(env)?.saturating_add(clusters.len(env)?) > MAX_BYTES {
            return Err(fusion_error("input exceeds 64 MiB"));
        }
        let circuit =
            decode_live_circuit(&env.convert_byte_array(&snapshot)?).map_err(fusion_error)?;
        let clusters =
            serde_json::from_slice(&env.convert_byte_array(&clusters)?).map_err(fusion_error)?;
        let mut next = u64::try_from(next).map_err(fusion_error)?;
        let mut warnings = Vec::new();
        let mut native_columns = true;
        let output = feldera_sql_opt::fuse_expensive_maps_with(
            circuit.clone(),
            clusters,
            &mut next,
            &mut warnings,
            |site| {
                native_columns = false;
                let [left, right] = site.fields;
                let request = column_request(
                    site.graph,
                    site.input,
                    &[site.row.into(), left.into(), right.into()],
                    site.warnings,
                    site.next_name,
                )?;
                compare(env, &equality, &request).map(Some)
            },
        );
        let result =
            column_reply(output, &circuit, native_columns, next, warnings).map_err(fusion_error)?;
        let bytes = serde_json::to_vec(&result).map_err(fusion_error)?;
        if bytes.len() > MAX_BYTES {
            return Err(fusion_error("output exceeds 64 MiB"));
        }
        Ok(env.byte_array_from_slice(&bytes)?)
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
