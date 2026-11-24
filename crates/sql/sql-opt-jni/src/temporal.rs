use super::fragment::Fragment;
use feldera_sql_ir::{Circuit, CircuitEditor, Error, NodeId, OperatorKind, OutputPort};
use feldera_sql_opt::TemporalFilterLowering;
use feldera_sql_opt::TemporalPredicate;
use feldera_sql_opt::limiters::{Stream, StreamKind};
use serde_json::{Value, json};
use std::collections::HashSet;

/// A live temporal analysis can report diagnostics and allocate variable names.
/// Publish the Rust prefix before invoking it, even if it throws.
fn analyze_live_pair(
    env: &mut jni::Env,
    analyzer: &jni::objects::JObject,
    input: &Circuit,
    pair: feldera_sql_opt::TemporalFilterPair<'_>,
) -> Result<Option<bool>, Error> {
    use jni::{JValue, jni_sig, jni_str, objects::JByteArray};
    #[derive(serde::Deserialize)]
    struct Decision {
        merge: bool,
        next_generated_name: u64,
    }

    let roots = pair
        .graph
        .live_inner_roots(input, &pair.closures.map(NodeId::from))?;
    let request = json!({
        "operators": pair.operators.map(super::operator_number),
        "closures": roots,
        "next_generated_name": *pair.next,
        "warnings": super::pass_warnings(std::mem::take(pair.warnings)),
    });
    let bytes = serde_json::to_vec(&request).map_err(|e| Error::new(e.to_string()))?;
    if bytes.len() > super::MAX_BYTES {
        return Err(Error::new("temporal predicate pair exceeds 64 MiB"));
    }
    let reply = env
        .with_local_frame(4, |env| -> jni::errors::Result<Vec<u8>> {
            let request = env.byte_array_from_slice(&bytes)?;
            let result = env
                .call_method(
                    analyzer,
                    jni_str!("analyze"),
                    jni_sig!("([B)[B"),
                    &[JValue::Object(&request)],
                )?
                .l()?;
            let result = JByteArray::cast_local(env, result)?;
            env.convert_byte_array(&result)
        })
        .map_err(|e| Error::new(e.to_string()))?;
    let decision: Decision =
        serde_json::from_slice(&reply).map_err(|e| Error::new(e.to_string()))?;
    if decision.next_generated_name < *pair.next {
        return Err(Error::new(
            "temporal analysis moved the name counter backwards",
        ));
    }
    *pair.next = decision.next_generated_name;
    Ok(Some(decision.merge))
}

// SAFETY: the symbol matches RustCircuitPass.mergeFilters. EnvUnowned contains
// panics at the JVM boundary and preserves exceptions raised by the analyzer.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_dbsp_sqlCompiler_compiler_backend_RustCircuitPass_mergeFilters<
    'local,
>(
    mut env: jni::EnvUnowned<'local>,
    _class: jni::objects::JClass<'local>,
    snapshot: jni::objects::JByteArray<'local>,
    next: i64,
    analyzer: jni::objects::JObject<'local>,
) -> jni::objects::JByteArray<'local> {
    use super::BridgeError;
    use jni::errors::ThrowRuntimeExAndDefault;
    let error = |message: String| BridgeError::Optimizer(format!("Live/MergeFilters: {message}"));
    env.with_env(|env| -> Result<_, BridgeError> {
        if snapshot.len(env)? > super::MAX_BYTES {
            return Err(error("input exceeds 64 MiB".into()));
        }
        let input = super::decode_live_circuit(&env.convert_byte_array(&snapshot)?)
            .map_err(|e| error(e.to_string()))?;
        let mut next = u64::try_from(next).map_err(|e| error(e.to_string()))?;
        let mut warnings = Vec::new();
        let output = feldera_sql_opt::merge_temporal_filters_with(
            input.clone(),
            &mut next,
            &mut warnings,
            |pair| analyze_live_pair(env, &analyzer, &input, pair),
        )
        .map_err(|e| error(e.to_string()))?
        .ok_or_else(|| error("missing pair decision".into()))?;
        let result = match output {
            feldera_sql_opt::TemporalMerge::Rewritten(circuit) => json!({
                "diagnostics": super::pass_warnings(warnings),
                "next_generated_name": next, "unused_sources": [],
                "circuit": circuit.changed_live_graph(&input),
            }),
            feldera_sql_opt::TemporalMerge::TypeError {
                kind,
                ty,
                next_name,
                warnings,
            } => json!({
                "type_error": {"kind": kind, "type": super::inner_number(ty.into())},
                "next_generated_name": next_name,
                "warnings": super::pass_warnings(warnings),
            }),
            feldera_sql_opt::TemporalMerge::MissingBound {
                next_name,
                warnings,
            } => json!({
                "missing_clock_bound": true,
                "next_generated_name": next_name,
                "warnings": super::pass_warnings(warnings),
            }),
            feldera_sql_opt::TemporalMerge::MissingKey {
                key,
                next_name,
                warnings,
            } => json!({
                "missing_clock_key": super::inner_number(key.expression().into()),
                "key_is_node_id": matches!(key, feldera_sql_opt::MissingClockKey::Fact(_)),
                "next_generated_name": next_name,
                "warnings": super::pass_warnings(warnings),
            }),
        };
        let bytes = serde_json::to_vec(&result).map_err(|e| error(e.to_string()))?;
        if bytes.len() > super::MAX_BYTES {
            return Err(error("output exceeds 64 MiB".into()));
        }
        Ok(env.byte_array_from_slice(&bytes)?)
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

/// Java warns about negative settings but retains the signed value. Its
/// nonempty sharing groups then all meet the threshold. Zero disables sharing.
pub(super) fn sharing_threshold(state: &[u8]) -> Result<usize, serde_json::Error> {
    let threshold = serde_json::from_slice::<i32>(state)?;
    Ok(usize::try_from(threshold).unwrap_or(1))
}

pub(super) fn source_paths(paths: Vec<feldera_sql_opt::TemporalFilterSource>) -> Value {
    Value::Array(
        paths
            .into_iter()
            .map(|path| {
                json!({
                    "filter": super::operator_number(path.filter),
                    "source": super::operator_number(path.source.operator),
                    "port": path.source.number,
                    "maps": super::operator_numbers(path.maps),
                })
            })
            .collect(),
    )
}

pub(super) fn scan(
    circuit: Circuit,
    threshold: usize,
) -> Result<Value, Box<dyn std::error::Error>> {
    use feldera_sql_opt::TemporalScan;
    Ok(
        match feldera_sql_opt::scan_temporal_windows(circuit, threshold)? {
            TemporalScan::Groups {
                groups,
                warnings,
                generated_names,
            } => {
                let groups: Vec<Vec<_>> = groups
                    .into_iter()
                    .map(|group| {
                        group
                            .into_iter()
                            .map(|(filter, conjunct)| (super::operator_number(filter), conjunct))
                            .collect()
                    })
                    .collect();
                json!({ "groups": groups, "warnings": super::pass_warnings(warnings), "generated_names": generated_names })
            }
            TemporalScan::Paths(paths) => json!({ "paths": source_paths(paths) }),
        },
    )
}

impl<const N: usize> Fragment<[feldera_sql_ir::OutputPort; N]> {
    fn clock_reply(
        &self,
        edit: &CircuitEditor,
        clock: feldera_sql_opt::ClockStreams,
        next: u64,
    ) -> Result<Value, Box<dyn std::error::Error>> {
        let mut reply = self.reply(edit, next)?;
        reply["collection"] = super::operator_number(clock.collection).into();
        reply["indexed"] = super::operator_number(clock.indexed).into();
        Ok(reply)
    }
}

pub(super) fn builtin_clock(
    input: &[u8],
    mut next: u64,
) -> Result<Value, Box<dyn std::error::Error>> {
    let (fragment, roots) = Fragment::at_empty_site([], input)?;
    let [timestamp, empty_key] = roots.as_slice() else {
        return Err("system clock requires a timestamp and an empty key".into());
    };
    let mut edit = CircuitEditor::new(fragment.base.clone());
    let timestamp = edit.type_id(*timestamp)?;
    let key = edit.type_id(*empty_key)?;
    let clock = feldera_sql_opt::builtin_clock(
        &mut edit,
        fragment.base.root_id(),
        timestamp,
        key,
        &mut next,
    )?;
    fragment.clock_reply(&edit, clock, next)
}

pub(super) fn table_clock(
    input: &[u8],
    mut next: u64,
) -> Result<Value, Box<dyn std::error::Error>> {
    let (fragment, roots) = Fragment::at_empty_site([OperatorKind::SourceMultiset], input)?;
    let [timestamp, weight] = roots.as_slice() else {
        return Err("clock table requires a timestamp and a weight variable".into());
    };
    let mut edit = CircuitEditor::new(fragment.base.clone());
    let timestamp = edit.type_id(*timestamp)?;
    let weight = edit.expression(*weight)?;
    let clock = feldera_sql_opt::table_clock(
        &mut edit,
        fragment.base.root_id(),
        fragment.inputs[0],
        timestamp,
        weight,
        &mut next,
    )?;
    fragment.clock_reply(&edit, clock, next)
}

fn window_inputs(
    edit: &CircuitEditor,
    [rows, clock]: [OutputPort; 2],
    [row_type, clock_type, empty_key, timestamp]: [NodeId; 4],
) -> Result<(TemporalFilterLowering, Stream), Error> {
    let collection = |port, ty| Stream {
        port,
        ty,
        kind: StreamKind::Collection,
    };
    Ok((
        TemporalFilterLowering {
            site: edit.circuit().root_id(),
            clock: collection(clock, edit.type_id(clock_type)?),
            empty_key: edit.type_id(empty_key)?,
            timestamp: edit.type_id(timestamp)?,
        },
        collection(rows, edit.type_id(row_type)?),
    ))
}

pub(super) fn filter(
    input: &[u8],
    state: &[u8],
    mut next: u64,
) -> Result<Value, Box<dyn std::error::Error>> {
    use feldera_sql_opt::{ClockFilter, LoweredClockFilter};
    let (fragment, roots) = Fragment::bind(
        serde_json::from_slice(state)?,
        [
            OperatorKind::SourceMultiset,
            OperatorKind::Now,
            OperatorKind::MapIndex,
        ],
        input,
    )?;
    let [rows, clock, raw_key, timestamp, join_key, output, closure] = roots.as_slice() else {
        return Err("missing temporal filter inputs".into());
    };
    let mut edit = CircuitEditor::new(fragment.base.clone());
    let (lowering, rows) = window_inputs(
        &edit,
        [fragment.inputs[0], fragment.inputs[1]],
        [*rows, *clock, *raw_key, *timestamp],
    )?;
    let filter = ClockFilter {
        function: edit.expression(*closure)?,
        output: edit.type_id(*output)?,
        indexed_clock: fragment.inputs[2],
        join_key: edit.type_id(*join_key)?,
    };
    let Some(result) = lowering.lower_filter(&mut edit, rows, filter, &mut next)? else {
        return Ok(Value::Null);
    };
    if let LoweredClockFilter::UnsupportedLambda { predicate, lambda } = result {
        let sites = edit
            .circuit()
            .live_inner_roots(&fragment.base, &[predicate.into(), lambda.into()])?;
        return Ok(json!({"rejected_lambda": sites, "next_generated_name": next}));
    }
    let mut reply = fragment.reply(&edit, next)?;
    if let LoweredClockFilter::Joined(predicate) = result {
        // This expression is read only for source positions. It carries its own node definitions.
        reply["warning_predicate"] = edit.circuit().live_expression(&fragment.base, predicate)?;
    }
    Ok(reply)
}

pub(super) fn window(
    input: &[u8],
    state: &[u8],
    mut next: u64,
) -> Result<Value, Box<dyn std::error::Error>> {
    use feldera_sql_opt::{InitialBounds, WindowComparison, WindowKey};
    #[derive(serde::Deserialize)]
    struct Request {
        #[serde(flatten)]
        site: super::ExpressionSite,
        comparisons: Vec<WindowComparison>,
    }
    let request: Request = serde_json::from_slice(state)?;
    let (fragment, roots) = Fragment::bind(
        request.site,
        [OperatorKind::SourceMultiset, OperatorKind::Now],
        input,
    )?;
    let [
        collection,
        clock,
        raw_key,
        timestamp,
        parameter,
        common,
        key,
        lower,
        upper,
        operands @ ..,
    ] = roots.as_slice()
    else {
        return Err("missing temporal window input".into());
    };
    if request.comparisons.len() != operands.len() {
        return Err("expected one expression per temporal comparison".into());
    }
    let mut edit = CircuitEditor::new(fragment.base.clone());
    let (lowering, rows) = window_inputs(
        &edit,
        fragment.inputs,
        [*collection, *clock, *raw_key, *timestamp],
    )?;
    let key = WindowKey {
        parameter: *parameter,
        row: edit.type_id(edit.node(*collection)?.edge("elementType")?)?,
        expression: edit.expression(*common)?,
        nonnull_type: edit.type_id(*key)?,
    };
    let initial = InitialBounds {
        minimum_clock: edit.minimum_value(lowering.timestamp)?,
        lower: edit.expression(*lower)?,
        upper: edit.expression(*upper)?,
    };
    let values = operands
        .iter()
        .map(|&id| edit.expression(id))
        .collect::<Result<Vec<_>, _>>()?;
    lowering.append_window(
        &mut edit,
        rows,
        key,
        initial,
        request.comparisons.into_iter().zip(values),
        &mut next,
    )?;
    fragment.reply(&edit, next)
}

pub(super) fn clock_join(
    input: &[u8],
    mut next: u64,
    consumer: feldera_sql_opt::ClockConsumer,
) -> Result<Value, Box<dyn std::error::Error>> {
    let (fragment, roots) = Fragment::at_empty_site(
        [OperatorKind::SourceMultiset, OperatorKind::MapIndex],
        input,
    )?;
    let [row, key, timestamp, function, output] = roots.as_slice() else {
        return Err(
            "clock join requires a row, an empty key, a timestamp, a closure and an output type"
                .into(),
        );
    };
    let mut edit = CircuitEditor::new(fragment.base.clone());
    let body = fragment.base.root_id();
    let [rows, indexed_clock] = fragment.inputs;
    let inputs = feldera_sql_opt::ClockJoinInputs {
        rows,
        indexed_clock,
        row_type: edit.type_id(*row)?,
    };
    let key = edit.type_id(*key)?;
    let timestamp = edit.type_id(*timestamp)?;
    let join = feldera_sql_opt::append_clock_join(
        &mut edit, body, body, inputs, key, timestamp, &mut next,
    )?;
    let projection = feldera_sql_opt::ClockProjection {
        consumer,
        function: edit.expression(*function)?,
        output: edit.type_id(*output)?,
    };
    match feldera_sql_opt::apply_joined_clock(
        &mut edit, body, join, projection, timestamp, &mut next,
    )? {
        feldera_sql_opt::ClockRewrite::Rewritten(_) => fragment.reply(&edit, next),
        feldera_sql_opt::ClockRewrite::UnsupportedLambda(lambda) => {
            let function = edit.circuit().live_expression(&fragment.base, lambda)?;
            Ok(json!({"unsupported_lambda": function, "next_generated_name": next}))
        }
    }
}

/// Encode all predicate roots together so consecutive residuals can share types.
pub(super) fn classify(
    input: &Circuit,
    edit: &mut CircuitEditor,
    roots: &[NodeId],
    state: &[u8],
) -> Result<Value, Box<dyn std::error::Error>> {
    let mut generated_names = 0;
    let parts = match *roots {
        [parameter, body] => {
            let body = edit.expression(body)?;
            let Some(parts) = feldera_sql_opt::infer_temporal_predicates(
                edit,
                parameter,
                body,
                &mut generated_names,
            )?
            else {
                return Ok(Value::Null);
            };
            parts
        }
        [body] => {
            let ids: Vec<u64> = serde_json::from_slice(state)?;
            let monotone = ids
                .into_iter()
                .map(|id| edit.expression(NodeId::Inner(id)))
                .collect::<Result<HashSet<_>, _>>()?;
            let body = edit.expression(body)?;
            feldera_sql_opt::classify_temporal_predicates(edit, body, &monotone)?
        }
        _ => {
            return Err(
                "expected a temporal predicate with its row parameter or monotonicity facts".into(),
            );
        }
    };
    let mut roots = Vec::new();
    let mut headers = Vec::with_capacity(parts.len());
    for part in parts {
        let (kind, expression, clock) = match part {
            TemporalPredicate::NoNow(expr) => (json!("NoNow"), expr, None),
            TemporalPredicate::Residual(expr) => (json!("Residual"), expr, None),
            TemporalPredicate::Bound {
                key,
                clock,
                comparison,
            } => (json!(comparison), key, Some(clock)),
        };
        roots.push(NodeId::from(expression));
        roots.extend(clock.map(NodeId::from));
        headers.push((kind, clock.is_some()));
    }

    let mut expressions = edit.circuit().live_inner_roots(input, &roots)?.into_iter();
    let mut next = || expressions.next().expect("one encoded expression per root");
    let parts = headers
        .into_iter()
        .map(|(kind, has_clock)| {
            let mut part = json!({"kind":kind, "expression":next()});
            if has_clock {
                part["clock"] = next();
            }
            part
        })
        .collect::<Vec<_>>();
    Ok(json!({"comparisons": parts, "generated_names": generated_names}))
}

/// A bound supplies its declaring parameter and key. A residual supplies just
/// its Boolean expression. Consecutive roots share the request's node table.
pub(super) fn groups(
    input: &Circuit,
    edit: &mut CircuitEditor,
    roots: &[NodeId],
    state: &[u8],
    merge: bool,
) -> Result<Value, Box<dyn std::error::Error>> {
    use feldera_sql_opt::{TemporalBoundKey, WindowTerm};
    let kinds: Vec<String> = serde_json::from_slice(state)?;
    let mut roots = roots.iter().copied();
    let mut terms = Vec::with_capacity(kinds.len());
    for kind in kinds {
        let root = roots.next().ok_or("missing temporal predicate")?;
        terms.push(match kind.as_str() {
            "NoNow" => WindowTerm::NoNow(edit.expression(root)?),
            "Residual" => WindowTerm::Residual(edit.expression(root)?),
            _ => WindowTerm::Bound(TemporalBoundKey {
                comparison: serde_json::from_value(Value::String(kind))?,
                parameter: root,
                key: edit.expression(roots.next().ok_or("missing temporal comparison key")?)?,
            }),
        });
    }
    if roots.next().is_some() {
        return Err("extra temporal predicate roots".into());
    }
    let result = if merge {
        let (target, upstream) = terms
            .split_last()
            .ok_or("missing downstream temporal filter")?;
        feldera_sql_opt::check_temporal_bound(edit, upstream, *target)
            .map(|accepted| json!(accepted))
    } else {
        group_predicates(input, edit, &terms)
    };
    match result {
        Ok(value) => Ok(value),
        Err(feldera_sql_opt::KeyComparisonError::Diagnostic(diagnostic)) => {
            Ok(json!({"comparison_failure": diagnostic}))
        }
        Err(feldera_sql_opt::KeyComparisonError::InvalidIr(error)) => Err(error.into()),
    }
}

fn group_predicates(
    input: &Circuit,
    edit: &mut CircuitEditor,
    terms: &[feldera_sql_opt::WindowTerm],
) -> Result<Value, feldera_sql_opt::KeyComparisonError> {
    let Some(groups) = feldera_sql_opt::check_temporal_groups(edit, terms)? else {
        return Ok(Value::Null);
    };
    let expressions = groups
        .iter()
        .filter_map(|g| g.predicate.map(NodeId::from))
        .collect::<Vec<_>>();
    let mut encoded = edit
        .circuit()
        .live_inner_roots(input, &expressions)?
        .into_iter();
    Ok(Value::Array(
        groups
            .into_iter()
            .map(|g| {
                let expression = g
                    .predicate
                    .map(|_| encoded.next().expect("one expression per residual group"));
                json!({"first":g.first,"count":g.count,"expression":expression})
            })
            .collect(),
    ))
}
