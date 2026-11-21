//! Run typed circuit passes inside the compiler JVM. Java retains the original
//! objects when an invocation changes only the generated-name counter.
use feldera_sql_ir::{BodyOrder, Circuit, Field, NodeId, OutputPort};
use jni::{
    errors::ThrowRuntimeExAndDefault,
    objects::{JByteArray, JClass, JString},
    EnvUnowned,
};
use serde_json::{json, Value};
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
};

#[derive(Debug, thiserror::Error)]
enum BridgeError {
    #[error(transparent)]
    Jni(#[from] jni::errors::Error),
    #[error("{0}")]
    Optimizer(String),
}

// A compiler may optimize another circuit synchronously on the same JVM thread.
thread_local! {
    static LIVE_DECODERS: RefCell<Vec<feldera_sql_ir::LiveCircuitDecoder>> = const { RefCell::new(Vec::new()) };
}

const MAX_BYTES: usize = 64 * 1024 * 1024;

fn annotations(circuit: &Circuit) -> Result<BTreeMap<u64, Value>, feldera_sql_ir::Error> {
    let mut values = BTreeMap::new();
    for id in circuit.operator_order(BodyOrder::Preorder)? {
        let NodeId::Outer(number) = id else {
            unreachable!("operator has an outer identity")
        };
        let node = circuit.node(id).expect("circuit body member");
        let Some(Field::Data(value @ Value::Array(_))) = node.field("annotations") else {
            return Err(feldera_sql_ir::Error::new("operator requires annotations"));
        };
        values.insert(number, value.clone());
    }
    Ok(values)
}

fn operator_number(id: NodeId) -> u64 {
    match id {
        NodeId::Outer(n) => n,
        NodeId::Inner(_) => unreachable!("operator has outer identity"),
    }
}

fn operator_numbers(ids: impl IntoIterator<Item = NodeId>) -> Vec<u64> {
    ids.into_iter().map(operator_number).collect()
}

fn ports(pairs: Vec<(u64, usize)>) -> BTreeSet<OutputPort> {
    pairs
        .into_iter()
        .map(|(id, number)| OutputPort {
            operator: NodeId::Outer(id),
            number,
        })
        .collect()
}

fn wire_ports(ports: BTreeSet<OutputPort>) -> Value {
    let pairs: Vec<_> = ports
        .into_iter()
        .map(|p| (operator_number(p.operator), p.number))
        .collect();
    json!(pairs)
}

fn multiplicities(facts: feldera_sql_opt::Multiplicities) -> Value {
    let mut operators = BTreeMap::<u64, Vec<Option<bool>>>::new();
    for (port, multiset) in facts {
        let outputs = operators.entry(operator_number(port.operator)).or_default();
        outputs.resize(port.number + 1, None);
        outputs[port.number] = Some(multiset);
    }
    json!(operators)
}

fn expression_arena(roots: impl IntoIterator<Item = Value>) -> Result<(Circuit, Vec<NodeId>), feldera_sql_ir::Error> {
    Circuit::from_json(json!({
        "class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": []
    }))?.with_inner_roots(roots)
}

/// The arena root carries the table or view location for expressions created
/// by the live builder. The supplied row and intervals keep their own origins.
fn declared_waterline(
    input: &[u8],
    state: &[u8],
    mut next: u64,
) -> Result<Value, Box<dyn std::error::Error>> {
    #[derive(serde::Deserialize)]
    struct Site {
        columns: Vec<usize>,
        empty: bool,
        position: Option<Value>,
    }
    let site: Site = serde_json::from_slice(state)?;
    let mut header = json!({
        "class":"DBSPCircuit", "id":0, "declarations":[], "allOperators":[], "sourceEmpty":site.empty
    });
    if let Some(range) = site.position {
        header["position"] = range;
    }
    let Value::Array(inputs) = feldera_sql_ir::parse_json(input)? else {
        return Err(
            "expected a row parameter followed by its declared lateness expressions".into(),
        );
    };
    let (arena, roots) = Circuit::from_json(header)?.with_inner_roots(inputs)?;
    let Some((&row, intervals)) = roots.split_first() else {
        return Err("missing waterline row".into());
    };
    if site.columns.len() != intervals.len() {
        return Err("expected one interval per LATENESS column".into());
    }
    let columns: Vec<_> = site
        .columns
        .into_iter()
        .zip(intervals.iter().copied())
        .collect();
    let mut edit = feldera_sql_ir::CircuitEditor::new(arena.clone());
    let row = edit.expression(row)?;
    let result = feldera_sql_opt::limiters::declared_waterline(
        &mut edit,
        arena.root_id(),
        row,
        &columns,
        &mut next,
    )?;
    let expressions = edit
        .circuit()
        .additional_expressions(&arena, &[result.timestamp, result.initial, result.maximum])?;
    Ok(json!({"expressions": expressions, "next_generated_name": next}))
}

fn binding_pairs(roots: &[NodeId]) -> Result<&[[NodeId; 2]], &'static str> {
    let (pairs, rest) = roots.as_chunks::<2>();
    if rest.is_empty() {
        Ok(pairs)
    } else {
        Err("expected a variable and value for each lazy binding")
    }
}

fn lower_function(pass: &str, input: &[u8], state: &[u8], mut next: u64) -> Result<Value, Box<dyn std::error::Error>> {
    let Value::Array(roots) = feldera_sql_ir::parse_json(input)? else {
        return Err("expected an array of lowering inputs".into());
    };
    let (arena, roots) = expression_arena(roots)?;
    if let ("ParameterFieldDependencies", [closure]) = (pass, roots.as_slice()) {
        return Ok(serde_json::to_value(feldera_sql_expr::parameter_field_paths(&arena, *closure)?)?);
    }
    if let ("Monotonicity/Cast", [from, to, bound]) = (pass, roots.as_slice()) {
        let edit = feldera_sql_ir::CircuitEditor::new(arena);
        let choice = feldera_sql_opt::cast_waterline(
            &edit, edit.type_id(*from)?, edit.type_id(*to)?, edit.type_id(*bound)?,
            serde_json::from_slice(state)?,
        )?;
        return Ok(serde_json::to_value(choice)?);
    }
    if let ("IsIdentityProjection", [closure]) = (pass, roots.as_slice()) {
        let edit = feldera_sql_ir::CircuitEditor::new(arena);
        let identity = feldera_sql_opt::identity_closure(&edit, *closure, &mut next)?;
        return Ok(json!({"identity": identity, "next_generated_name": next}));
    }
    if matches!(pass, "IsStrictProjection" | "IsNestedProjection") {
        let [closure] = roots.as_slice() else {
            return Err("expected one map closure".into());
        };
        let edit = feldera_sql_ir::CircuitEditor::new(arena);
        return Ok(Value::Bool(feldera_sql_opt::is_map_projection(
            &edit, *closure, pass == "IsNestedProjection",
        )?));
    }
    let mut edit = feldera_sql_ir::CircuitEditor::new(arena.clone());
    let mut warnings = Vec::new();
    let function = match (pass, roots.as_slice()) {
        ("Asof/LeftIndex" | "Asof/RightIndex", [pair, comparator]) => {
            let pair = edit.expression(*pair)?;
            let comparator = edit.expression(*comparator)?;
            feldera_sql_opt::asof_index(&mut edit, pair, comparator, pass == "Asof/LeftIndex")?
        }
        ("MergeFilters/Predicate", [source, filter]) => {
            let source = edit.expression(*source)?;
            let filter = edit.expression(*filter)?;
            feldera_sql_opt::merge_temporal_predicates(&mut edit, source, filter, &mut next, &mut warnings)?
        }
        ("Asof/Timestamps", [left, right]) => {
            let inputs = [edit.type_id(*left)?, edit.type_id(*right)?];
            let columns = serde_json::from_slice(state)?;
            let pair = feldera_sql_opt::asof_timestamps(&mut edit, inputs, columns, &mut next)?;
            let functions = edit.circuit().additional_expressions(&arena, &pair)?;
            return Ok(json!({"functions": functions, "next_generated_name": next}));
        }
        ("Asof/Projection", [function, key, left, right]) => {
            let (key, left, right) = (edit.type_id(*key)?, edit.type_id(*left)?, edit.type_id(*right)?);
            feldera_sql_opt::asof_projection(&mut edit, *function, key, left, right, &mut next, &mut warnings)?
        }
        ("Limiters/KeyFilter", [key, waterline]) => {
            let (name, keys, declared): (String, Vec<usize>, Vec<usize>) = serde_json::from_slice(state)?;
            let key = edit.type_id(*key)?;
            let waterline = edit.type_id(*waterline)?;
            let projection = feldera_sql_opt::limiters::KeyProjection::from_columns(&keys, &declared);
            let closures = projection.filter(&mut edit, &name, waterline, key, &mut next)?;
            let functions = edit.circuit().additional_expressions(&arena, &closures)?;
            return Ok(json!({"functions": functions, "next_generated_name": next}));
        }
        ("Limiters/RowFilter", [row, waterline]) => {
            let (name, columns): (String, Vec<usize>) = serde_json::from_slice(state)?;
            let row = edit.type_id(*row)?;
            let waterline = edit.type_id(*waterline)?;
            let filter = feldera_sql_opt::limiters::row_filter(
                &mut edit, &name, row, &columns, waterline, &mut next,
            )?;
            let functions = edit.circuit().additional_expressions(&arena, &[filter.function, filter.error])?;
            return Ok(json!({"functions": functions, "next_generated_name": next}));
        }
        ("Limiters/Join", inputs) => {
            let shapes: [Option<feldera_sql_opt::limiters::Projection>; 2] = serde_json::from_slice(state)?;
            let mut types = inputs.iter();
            let mut side = |shape| -> Result<_, feldera_sql_ir::Error> {
                let id = types.next().ok_or_else(|| feldera_sql_ir::Error::new("missing join waterline type"))?;
                Ok((edit.type_id(*id)?, shape))
            };
            let [left, right] = shapes.each_ref().map(Option::as_ref);
            let sides = [left.map(&mut side).transpose()?, right.map(side).transpose()?];
            if types.next().is_some() {
                return Err("unexpected extra join waterline type".into());
            }
            feldera_sql_opt::limiters::join_waterline(&mut edit, sides, &mut next, &mut warnings)?
        }
        ("Limiters/MergeView", [left, right]) => {
            let [inherited, declared]: [feldera_sql_opt::limiters::Projection; 2] = serde_json::from_slice(state)?;
            let left = edit.type_id(*left)?;
            let right = edit.type_id(*right)?;
            inherited.merge_view_waterline(&mut edit, left, &declared, right, &mut next)?
        }
        ("Limiters/Project", [input, declared]) => {
            let [source, destination]: [feldera_sql_opt::limiters::Projection; 2] = serde_json::from_slice(state)?;
            let ty = edit.type_id(*input)?;
            let declared = edit.type_id(*declared)?;
            source.project_waterline(&mut edit, ty, &destination, Some(declared), &mut next)?
        }
        ("Limiters/Minimum", inputs) => {
            let types = inputs.iter().map(|&id| edit.type_id(id)).collect::<Result<Vec<_>, _>>()?;
            feldera_sql_opt::limiters::minimum_waterline(&mut edit, &types, &mut next, &mut warnings)?
        }
        ("Limiters/Apply", [function, inputs @ ..]) if !inputs.is_empty() => {
            let types = inputs.iter().map(|&id| edit.type_id(id)).collect::<Result<Vec<_>, _>>()?;
            feldera_sql_opt::limiters::apply_waterlines(&mut edit, *function, &types, &mut next, &mut warnings)?
        }
        ("Statics/RenumberErrors", [initializer]) => {
            let first = serde_json::from_slice(state)?;
            let initializer = edit.expression(*initializer)?;
            let (result, next) = feldera_sql_opt::renumber_static_errors(&mut edit, initializer, first)?;
            let mut roots = edit.circuit().additional_expressions(&arena, &[result])?;
            let value = roots.pop().expect("one static initializer");
            return Ok(json!({"initializer": value, "next": next}));
        }
        ("Statics/Inline" | "Statics/Define" | "Statics/Declare", _) => {
            let value = match (pass, roots.as_slice()) {
                ("Statics/Declare", [existing]) => edit.expression(*existing)?,
                ("Statics/Inline" | "Statics/Define", [origin, initializer]) => {
                    let initializer = edit.expression(*initializer)?;
                    feldera_sql_opt::static_initializer(&mut edit, *origin, initializer, state)?
                }
                _ => return Err("invalid static initializer inputs".into()),
            };
            let nodes = if pass == "Statics/Inline" {
                vec![value.into()]
            } else {
                let (item, reference) = feldera_sql_opt::static_item(&mut edit, value)?;
                vec![item, reference.into()]
            };
            return Ok(serde_json::to_value(edit.circuit().additional_inner_nodes(&arena, nodes)?)?);
        }
        ("Monotonicity/RowIdentity" | "Monotonicity/IndexedIdentity", [row]) => {
            let ty = edit.type_id(*row)?;
            feldera_sql_opt::monotone_identity(&mut edit, ty, pass == "Monotonicity/IndexedIdentity", &mut next)?
        }
        ("Monotonicity/FilterBounds", [closure]) => {
            let bounds = feldera_sql_opt::filter_lower_bounds(&mut edit, *closure)?;
            let values = edit.circuit().additional_inner_nodes(&arena, bounds.iter().map(|bound| bound.value.into()))?;
            let ranges: Vec<_> = bounds.into_iter().zip(values).map(|(bound, value)| {
                json!({"column": bound.column, "comparison": bound.comparison.wire_name(), "value": value})
            }).collect();
            return Ok(Value::Array(ranges));
        }
        ("InnerCSE/Let", [body, pairs @ ..]) => {
            let body = edit.expression(*body)?;
            let pairs = binding_pairs(pairs)?;
            feldera_sql_opt::bind_lazy_expression(&mut edit, pairs, body)?
        }
        ("InnerCSE/Statements", pairs) => {
            let pairs = binding_pairs(pairs)?;
            let statements = feldera_sql_opt::lazy_statements(&mut edit, pairs)?;
            return Ok(serde_json::to_value(edit.circuit().additional_inner_nodes(&arena, statements)?)?);
        }
        ("SplitRowMapInput" | "SplitIndexedMapInput", [reader]) => {
            let output = if pass == "SplitIndexedMapInput" {
                feldera_sql_opt::MapOutput::Indexed
            } else {
                feldera_sql_opt::MapOutput::Row
            };
            let paths = serde_json::from_slice(state)?;
            let pair = feldera_sql_opt::split_map_input(&mut edit, *reader, &paths, output, &mut next)?;
            let functions = pair.map(|roots| edit.circuit().additional_expressions(&arena, &roots)).transpose()?;
            return Ok(json!({"functions": functions, "next_generated_name": next}));
        }
        ("ProjectUnnest", [flatmap, reader]) => {
            let Some(function) = feldera_sql_opt::project_unnest(&mut edit, *flatmap, *reader)? else {
                return Ok(Value::Null);
            };
            function
        }
        ("ProjectAggregateZero", [zero, reader]) => {
            let zero = edit.expression(*zero)?;
            let borrowed = edit.borrow_value(zero)?;
            feldera_sql_expr::apply_closure(&mut edit, *reader, &[borrowed], &mut warnings)?
        }
        ("ComposeProjection" | "TryComposeProjection", [before, after]) => {
            let Some(closure) = feldera_sql_opt::compose_projection(
                &mut edit, *before, *after, pass == "TryComposeProjection", &mut warnings,
            )? else {
                return Ok(Value::Null);
            };
            closure
        }
        ("ComposeIndexedJoinOutput", [before, after]) => {
            let before = edit.expression(*before)?;
            feldera_sql_expr::compose_closures(&mut edit, before, true, *after, Some(&mut warnings))?
        }
        ("ComposeIndexedProjection" | "TryIndexedProjection", [before, after]) => {
            let Some(closure) = feldera_sql_opt::compose_indexed_projection(
                &mut edit, *before, *after, pass == "TryIndexedProjection", &mut next, &mut warnings,
            )? else {
                return Ok(Value::Null);
            };
            closure
        }
        ("TrimTopKProjection", [producer, reader]) => {
            let Some(pair) = feldera_sql_opt::trim_topk_projection(&mut edit, *producer, *reader)? else {
                return Ok(Value::Null);
            };
            return Ok(serde_json::to_value(edit.circuit().additional_expressions(&arena, &pair)?)?);
        }
        ("TrimAggregateProjection", [reader, empty_row, key]) => {
            let Some(projection) = feldera_sql_opt::trim_aggregate_projection(
                &mut edit, *reader, *empty_row, *key, &mut next,
            )? else {
                return Ok(Value::Null);
            };
            let expressions = edit.circuit().additional_expressions(
                &arena, &[projection.empty_row, projection.filler],
            )?;
            return Ok(json!({"retained": projection.retained, "expressions": expressions, "next_generated_name": next}));
        }
        ("SplitAntijoinProjection", [closure]) => {
            let pair = feldera_sql_opt::split_antijoin_projection(&mut edit, *closure)?;
            return Ok(serde_json::to_value(edit.circuit().additional_expressions(&arena, &pair)?)?);
        }
        ("AntijoinProjection", [stream, projection @ ..]) if projection.len() <= 1 => {
            feldera_sql_opt::antijoin_projection(&mut edit, *stream, projection.first().copied(),
                &mut next, &mut warnings)?
        }
        ("LowerAggregateFold", [list]) => feldera_sql_opt::lower_aggregate_fold(
            &mut edit, *list, None, &mut next, &mut warnings,
        )?,
        ("LowerAggregateFold", [list, empty]) => {
            let ty = edit.type_id(*empty)?;
            feldera_sql_opt::lower_aggregate_fold(&mut edit, *list, Some(ty), &mut next, &mut warnings)?
        }
        ("LowerFlatmap", [flatmap, element]) => {
            let ty = edit.type_id(*element)?;
            feldera_sql_opt::lower_flatmap(&mut edit, *flatmap, ty, &mut next, &mut warnings)?
        }
        _ => return Err(format!("unexpected inputs to {pass}").into()),
    };
    let mut functions = edit.circuit().additional_expressions(&arena, &[function])?;
    Ok(json!({
        "function": functions.pop().expect("one requested expression"),
        "next_generated_name": next,
        "warnings": warnings.into_iter().map(|w| json!({
            "range": w.position, "error_type": w.kind, "message": w.message,
        })).collect::<Vec<_>>()
    }))
}

fn transform(
    pass: &str,
    error_view: &str,
    input: &[u8],
    state: &[u8],
    mut next: u64,
) -> Result<Value, Box<dyn std::error::Error>> {
    let live = pass.strip_prefix("Live/");
    let pass = live.unwrap_or(pass);
    if pass == "ContainsNow" {
        let calls: Vec<(String, usize)> = serde_json::from_slice(input)?;
        return Ok(json!(calls.iter().any(|(name, arity)| {
            feldera_sql_opt::is_clock_call(name, *arity)
        })));
    }
    if pass == "NoConnectorMetadata" {
        let calls: Vec<(String, feldera_sql_ir::provenance::SourceRange)> =
            serde_json::from_slice(input)?;
        let call = calls
            .into_iter()
            .find(|(name, _)| feldera_sql_opt::is_connector_metadata(name))
            .map(|(name, position)| json!({"name":name, "position":position}));
        return Ok(json!({"connector_call":call, "next_generated_name":next}));
    }
    if let Some(pass) = pass.strip_prefix("Expressions/") {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Inventory<'a> {
            #[serde(borrow)]
            casts: Vec<&'a str>,
            #[serde(borrow)]
            binary: Vec<&'a str>,
            unwrap: bool,
            #[serde(default)]
            conditional: bool,
        }
        let sites: Inventory = serde_json::from_slice(input)?;
        let required = match pass {
            "SimplifyConditionals" => sites.conditional,
            "ExpandSafeCasts" => sites.casts.contains(&"SqlSafe"),
            "ExpandUnsafeCasts" => sites.casts.contains(&"SqlUnsafe"),
            "AdjustSqlIndex" => sites
                .binary
                .iter()
                .any(|op| matches!(*op, "SQL_INDEX" | "SAFE_RUST_INDEX")),
            "CreateRuntimeErrorWrappers" => {
                sites.unwrap
                    || sites
                        .casts
                        .iter()
                        .any(|mode| matches!(*mode, "UnwrapResultSafe" | "UnwrapResultUnsafe"))
            }
            other => return Err(format!("unknown expression pass {other}").into()),
        };
        return Ok(json!({"needs_full_snapshot":required}));
    }
    match pass {
        "Monotonicity/Function" => {
            let (name, arguments): (String, Vec<feldera_sql_opt::WaterlineArgument>) = serde_json::from_slice(input)?;
            return Ok(serde_json::to_value(feldera_sql_opt::function_waterline(&name, &arguments))?);
        }
        "Monotonicity/Binary" => {
            #[derive(serde::Deserialize)]
            struct Binary<'a> {
                domain: feldera_sql_opt::BinaryDomain,
                opcode: &'a str,
                left: feldera_sql_opt::WaterlineOperand,
                right: feldera_sql_opt::WaterlineOperand,
            }
            let facts: Binary = serde_json::from_slice(input)?;
            return Ok(serde_json::to_value(feldera_sql_opt::binary_waterline(
                facts.domain, facts.opcode, facts.left, facts.right,
            ))?);
        }
        "InnerCSE/Bindings" => {
            let scope: feldera_sql_opt::BindingScope = serde_json::from_slice(state)?;
            let bindings: Vec<feldera_sql_opt::PendingBinding> = serde_json::from_slice(input)?;
            return Ok(serde_json::to_value(scope.placements(&bindings))?);
        }
        "InnerCSE/Uses" => {
            let graph: Vec<feldera_sql_opt::NumberedExpression> = serde_json::from_slice(input)?;
            return Ok(serde_json::to_value(feldera_sql_opt::shared_expression_uses(&graph))?);
        }
        "Limiters/DeclaredWaterline" => return declared_waterline(input, state, next),
        "MergeFilters/Predicate" | "Asof/LeftIndex" | "Asof/RightIndex" | "Asof/Timestamps" | "Asof/Projection" | "Monotonicity/Cast" | "Limiters/Apply" | "Limiters/Minimum" | "Limiters/Join" | "Limiters/RowFilter" | "Limiters/KeyFilter" | "Limiters/MergeView" | "Limiters/Project" | "Statics/RenumberErrors" | "Statics/Inline" | "Statics/Define" | "Statics/Declare"
        | "Monotonicity/RowIdentity" | "Monotonicity/IndexedIdentity" | "Monotonicity/FilterBounds"
        | "InnerCSE/Let" | "InnerCSE/Statements" | "ParameterFieldDependencies" | "SplitRowMapInput" | "SplitIndexedMapInput" | "LowerAggregateFold" | "LowerFlatmap" | "ComposeProjection" | "TryComposeProjection"
        | "ComposeIndexedProjection" | "TryIndexedProjection" | "ComposeIndexedJoinOutput" | "AntijoinProjection"
        | "SplitAntijoinProjection" | "IsNestedProjection" | "IsStrictProjection" | "TrimTopKProjection"
        | "TrimAggregateProjection" | "ProjectUnnest" | "ProjectAggregateZero" | "IsIdentityProjection" => {
            return lower_function(pass, input, state, next);
        }
        "FuseExpensiveMaps/Fragments" | "ImplementStatics/Candidates" => {
            let (arena, roots) = expression_arena([feldera_sql_ir::parse_json(input)?])?;
            return if pass == "ImplementStatics/Candidates" {
                Ok(serde_json::to_value(feldera_sql_opt::static_candidates(&arena, roots[0])?)?)
            } else {
                Ok(serde_json::to_value(feldera_sql_opt::map_fusion_fragments(&arena, roots[0])?)?)
            };
        }
        "FuseExpensiveMaps/Groups" => {
            let fingerprints: Vec<Vec<usize>> = serde_json::from_slice(input)?;
            return Ok(serde_json::to_value(feldera_sql_opt::map_fusion_groups(&fingerprints))?);
        }
        "Graph/Inputs" => {
            let bodies = serde_json::from_slice(input)?;
            return Ok(serde_json::to_value(feldera_sql_opt::projected_consumer_graphs(bodies)?)?);
        }
        "AggregateGroups" => {
            let kinds: Vec<feldera_sql_opt::AggregateKind> = serde_json::from_slice(input)?;
            let append_only: bool = serde_json::from_slice(state)?;
            return Ok(json!(feldera_sql_opt::aggregate_groups(
                &kinds,
                append_only
            )));
        }
        "WindowGroups/Keys" => {
            let inputs: Vec<String> = serde_json::from_slice(input)?;
            let threshold: usize = serde_json::from_slice(state)?;
            return Ok(json!(feldera_sql_opt::qualifying_window_groups(
                &inputs, threshold
            )));
        }
        "WindowGroups/Fields" => {
            let fields: Vec<Vec<String>> = serde_json::from_slice(input)?;
            return Ok(serde_json::to_value(
                feldera_sql_opt::shared_window_fields(&fields),
            )?);
        }
        "WindowGroups/TemporalInputs" => {
            let filters: Vec<Vec<feldera_sql_opt::TemporalCandidate>> =
                serde_json::from_slice(input)?;
            let threshold = serde_json::from_slice(state)?;
            return Ok(json!(feldera_sql_opt::shared_temporal_inputs(
                &filters, threshold
            )));
        }
        "LeftJoinChains/Key" => {
            let facts = serde_json::from_slice(input)?;
            return Ok(serde_json::to_value(feldera_sql_opt::choose_chain_key(&facts))?);
        }
        "TemporalComparisons" => {
            let nodes: Vec<feldera_sql_opt::TemporalPredicate> = serde_json::from_slice(input)?;
            return Ok(serde_json::to_value(feldera_sql_opt::classify_temporal_predicates(&nodes)?)?);
        }
        "WindowGroups/TemporalOrder" => {
            let bounds: Vec<Option<feldera_sql_opt::TemporalBound>> =
                serde_json::from_slice(input)?;
            let shared: Option<String> = serde_json::from_slice(state)?;
            return Ok(json!(feldera_sql_opt::temporal_filter_order(
                &bounds, shared.as_deref()
            )));
        }
        _ => {}
    }
    let circuit = if live.is_some() {
        LIVE_DECODERS.with_borrow_mut(|stack| match stack.last_mut() {
            Some(decoder) => decoder.decode(input),
            None => Circuit::from_live_json_bytes(input),
        })?
    } else {
        Circuit::from_json_bytes(input)?
    };
    if pass == "LowerJoinFilterMap" {
        let [id] = circuit.root().edges("allOperators")?.try_into()
            .map_err(|_| "join lowering requires one operator")?;
        let operator = circuit.node(id).expect("validated circuit member");
        let mut edit = feldera_sql_ir::CircuitEditor::new(circuit.clone());
        let mut warnings = Vec::new();
        let closure = feldera_sql_opt::lower_join_filter_map(
            &mut edit, operator.edge("function")?, operator.edge("filter")?,
            operator.field("map").map(|_| operator.edge("map")).transpose()?, &mut warnings,
        )?;
        let mut functions = edit.circuit().additional_expressions(&circuit, &[closure])?;
        let warnings: Vec<_> = warnings.into_iter().map(|w| {
            json!({"range":w.position, "error_type":w.kind, "message":w.message})
        }).collect();
        return Ok(json!({"function": functions.pop().expect("one requested closure"), "warnings": warnings}));
    }
    if pass == "UDFInliner/Declarations" {
        let calls = serde_json::from_slice::<Vec<(String, usize)>>(state)?;
        return Ok(json!({"needs_full_snapshot": feldera_sql_opt::may_inline_udfs(circuit, &calls)?}));
    }
    if pass == "DecomposeExpensiveFilters/Predicates" {
        return Ok(json!({"needs_full_snapshot": feldera_sql_opt::filters_need_decomposition(circuit)?}));
    }
    if pass == "MultisetAnalysis" {
        let facts = feldera_sql_opt::stream_multiplicities(&circuit)?;
        return Ok(multiplicities(facts));
    }
    if pass == "Topology/FindCommonProjections" {
        return Ok(json!(feldera_sql_opt::common_projection_payloads(
            &circuit
        )?));
    }
    if pass == "Topology/FindJoinTrees" {
        return Ok(match feldera_sql_opt::names_after_isolated_joins(&circuit, next)? {
            Some(next) => json!({"join_trees": [], "next_generated_name": next}),
            None => Value::Null,
        });
    }
    if pass == "FindJoinTrees" {
        let trees = feldera_sql_opt::find_join_trees(&circuit, &mut next)?;
        return Ok(json!({"join_trees":trees, "next_generated_name":next}));
    }
    if pass == "FindCommonProjections" {
        let mut edit = feldera_sql_ir::CircuitEditor::new(circuit.clone());
        let closures = feldera_sql_opt::shared_projection_closures(&mut edit, &mut next)?;
        let roots = closures.iter().map(|c| c.function).collect::<Vec<_>>();
        let expressions = edit.circuit().additional_expressions(&circuit, &roots)?;
        let records = closures.into_iter().zip(expressions).map(|(closure, function)| {
            json!({"operator": closure.operator, "original_output_size": closure.original_output_size, "function": function})
        }).collect::<Vec<_>>();
        return Ok(json!({"closures": records, "next_generated_name": next}));
    }
    if pass == "Topology/FindUnusedInputFields" {
        return Ok(json!(operator_numbers(feldera_sql_opt::input_projections(
            &circuit
        )?)));
    }
    if pass == "MapFieldDependencies" {
        return Ok(serde_json::to_value(feldera_sql_opt::map_field_dependencies(&circuit)?)?);
    }
    if pass == "FindUnusedInputFields" {
        return Ok(json!(feldera_sql_opt::input_field_uses(
            &circuit
        )?));
    }
    if pass == "Topology/MultisetAnalysis" {
        use feldera_sql_opt::MultiplicityPlan;
        let plan = feldera_sql_opt::plan_multiplicities(&circuit)?;
        return Ok(match plan {
            MultiplicityPlan::Complete(facts) => json!({"facts": multiplicities(facts)}),
            MultiplicityPlan::NeedsPayloads(ids) => json!({"payloads": operator_numbers(ids)}),
        });
    }
    let analysis = pass.strip_prefix("Topology/").unwrap_or(pass);
    if matches!(analysis, "FindEquivalentNoops" | "FindMultipleRetains") {
        let group = if analysis == "FindEquivalentNoops" {
            feldera_sql_opt::RetentionGroup::SiblingNoops
        } else {
            feldera_sql_opt::RetentionGroup::DataInput
        };
        if analysis != pass && group.needs_expressions(&circuit)? {
            return Ok(json!({"needs_full_snapshot":true}));
        }
        let facts = if analysis == "FindEquivalentNoops" {
            json!(feldera_sql_opt::share_trace_inputs(&circuit)?)
        } else {
            json!(feldera_sql_opt::retention_groups(
                &circuit,
                &serde_json::from_slice(state)?
            )?)
        };
        return Ok(json!({"facts": facts}));
    }
    if pass == "Graph" {
        return Ok(json!(feldera_sql_opt::consumer_graphs(
            &circuit
        )?));
    }
    if pass == "AsofRetainers" {
        return Ok(serde_json::to_value(feldera_sql_opt::asof_retainers(&circuit)?)?);
    }
    if pass == "DetectStreamingOperations" {
        return Ok(json!(feldera_sql_opt::detects_streaming(
            &circuit
        )?));
    }
    if pass == "FindBounded" {
        #[derive(serde::Deserialize)]
        struct BoundsContext {
            known: Vec<(u64, usize)>,
            keys: Vec<(u64, usize)>,
            now_table: String,
        }
        let context: BoundsContext = serde_json::from_slice(state)?;
        let bounded = feldera_sql_opt::propagate_bounds(
            &circuit,
            ports(context.known),
            &ports(context.keys),
            &context.now_table,
        )?;
        return Ok(wire_ports(bounded));
    }
    if pass == "CollectUnbounded" {
        let bounded = ports(serde_json::from_slice(state)?);
        return Ok(json!(feldera_sql_opt::unbounded_operators(
            &circuit,
            &bounded
        )?));
    }
    if pass == "FindGCedStreams" {
        return Ok(wire_ports(feldera_sql_opt::pruned_streams(
            &circuit,
        )?));
    }
    if pass == "AppendOnly" {
        let streams = feldera_sql_opt::append_only_streams(&circuit)?;
        return Ok(json!(operator_numbers(streams)));
    }
    if pass == "FindWindowGroups" {
        let candidates = feldera_sql_opt::window_candidates(&circuit)?;
        return Ok(json!(operator_numbers(candidates)));
    }
    if let Some(name) = pass.strip_prefix("Topology/") {
        if name == "CSE" {
            return Ok(
                json!({"payloads": operator_numbers(feldera_sql_opt::cse_payloads(&circuit)?)}),
            );
        }
        if name == "SeparateIntegrators" {
            if let Some(id) = feldera_sql_opt::snapshot_aggregate(&circuit)? {
                return Ok(json!({"stream_aggregate":operator_number(id)}));
            }
        }
        if name == "StrayGC" {
            if let Some(error) = feldera_sql_opt::retention_violation(&circuit)? {
                return Ok(json!({"retention_violation": error}));
            }
        }
        if matches!(name, "DeadCode" | "DeadCodeKeepSources") {
            let pruning = feldera_sql_opt::pruning(&circuit, name == "DeadCodeKeepSources")?;
            return Ok(json!({"pruned_operators":operator_numbers(pruning.removed),
                "unused_sources":operator_numbers(pruning.unused_sources)}));
        }
        let needs_full_snapshot = feldera_sql_opt::needs_full_snapshot(name, &circuit)?;
        return Ok(json!({ "needs_full_snapshot": needs_full_snapshot }));
    }
    if matches!(pass, "NoIntegralVisitor" | "ValidateRecursiveOperators") {
        let violation = if pass == "NoIntegralVisitor" {
            feldera_sql_opt::first_integral(&circuit)?.map(|id| {
                let NodeId::Outer(operator) = id else { unreachable!("integrator is an operator") };
                json!({"operator":operator, "message":"Circuit contains an integration operator", "internal":true})
            })
        } else {
            feldera_sql_opt::recursive_violation(&circuit)?.map(|error| json!({
                "operator":error.operator, "internal":error.internal,
                "message":format!("Unsupported operation '{}' in recursive code", error.operation),
            }))
        };
        return Ok(json!({"violation":violation, "next_generated_name":next}));
    }
    if pass == "FindMapChains" {
        let chains: Vec<_> = feldera_sql_opt::find_map_chains(&circuit)?
            .into_iter()
            .map(operator_numbers)
            .collect();
        return Ok(json!({"map_chains": chains}));
    }
    if pass == "OrderGcDependencies" {
        let edges: Vec<(u64, u64)> = serde_json::from_slice(state)?;
        let edges = edges.into_iter().map(|(a, b)| (NodeId::Outer(a), NodeId::Outer(b)));
        let order = feldera_sql_opt::dependency_order(&circuit, edges)?;
        return Ok(json!(operator_numbers(order)));
    }
    if pass == "OrderErrorConsumers" {
        let table: &str = serde_json::from_slice(state)?;
        let order = feldera_sql_opt::error_consumers(&circuit, table)?;
        return Ok(
            json!({"order": operator_numbers(order.operators), "first_error": order.first_error}),
        );
    }
    if pass == "CheckRetain" {
        let duplicate = feldera_sql_opt::duplicate_retention(&circuit)?;
        return Ok(json!({"duplicate_retention":duplicate,"next_generated_name":next}));
    }
    if pass == "FixupViewReferences" {
        return Ok(
            match feldera_sql_opt::resolve_view_references(&circuit)? {
                feldera_sql_opt::ViewResolution::UndeclaredRecursion { operator, message } => {
                    json!({"violation":{"operator":operator_number(operator), "internal":false, "message":message}})
                }
                feldera_sql_opt::ViewResolution::Valid(plan) => {
                    let redirects: BTreeMap<_, _> = plan
                        .redirects
                        .into_iter()
                        .map(|(id, slots)| {
                            let inputs: BTreeMap<_, _> = slots
                                .into_iter()
                                .map(|(slot, source)| (slot, operator_number(source)))
                                .collect();
                            (operator_number(id), inputs)
                        })
                        .collect();
                    json!({"order":operator_numbers(plan.order), "redirects":redirects})
                }
            },
        );
    }
    if pass == "RemoveTable" {
        let name: String = serde_json::from_slice(state)?;
        let ids = feldera_sql_opt::unreferenced_table_sources(&circuit, &name)?;
        return Ok(json!({"remove_sources":operator_numbers(ids)}));
    }
    if pass == "CircuitStatistics" {
        let stats = feldera_sql_opt::circuit_statistics(&circuit)?;
        return Ok(json!({"statistics":stats,"next_generated_name":next}));
    }
    if pass == "ConstantViews" {
        let sinks = feldera_sql_opt::constant_views(&circuit, error_view)?;
        return Ok(json!({"constant_views":operator_numbers(sinks), "next_generated_name":next}));
    }
    if pass == "DeterministicFunctions" {
        let findings = feldera_sql_opt::nondeterministic_initializers(&circuit)?;
        return Ok(json!({"nondeterministic_initializers":findings,"next_generated_name":next}));
    }
    if pass == "CheckHints" {
        let diagnostics = feldera_sql_opt::check_hints(&circuit)?;
        return Ok(json!({"diagnostics":diagnostics, "next_generated_name":next}));
    }
    if pass == "CheckPersistentIds" {
        let findings = feldera_sql_opt::check_persistent_ids(&circuit)?;
        return Ok(json!({"persistent_id_findings": findings}));
    }
    if matches!(pass, "DeltaHashPrefixes" | "GcHashPrefixes") {
        let inputs: feldera_sql_opt::PrefixInputs = serde_json::from_slice(state)?;
        let prefixes = if pass == "DeltaHashPrefixes" {
            inputs.delta_prefixes(&circuit)?
        } else {
            inputs.gc_prefixes(&circuit)?
        };
        return Ok(json!({"prefixes":prefixes}));
    }
    if matches!(pass, "CompactNames" | "TagRegions" | "SetOperatorHashes") {
        // Java implements these passes by mutating annotations on the live
        // operators. Preserve those identities for the subsequent hash pass.
        let before = annotations(&circuit)?;
        let changed = match pass {
            "CompactNames" => feldera_sql_opt::compact_names(circuit)?,
            "SetOperatorHashes" => {
                serde_json::from_slice::<feldera_sql_opt::ComputedHashes>(state)?
                    .annotate(circuit)?
            }
            _ => feldera_sql_opt::tag_regions(circuit)?,
        };
        let mut edits = annotations(&changed)?;
        edits.retain(|id, value| before.get(id) != Some(value));
        return Ok(json!({"annotations": edits, "next_generated_name": next}));
    }
    let (original, lowering) = if pass == "LowerCircuitVisitor" {
        let types: feldera_sql_opt::LoweringTypes = serde_json::from_slice(state)?;
        let (circuit, context) = types.bind(circuit)?;
        (circuit, Some(context))
    } else {
        (circuit, None)
    };
    // Move the encoded graph into the reply. Passing it through json! would
    // traverse and allocate the entire graph again on every changed pass.
    let publish = |output: &Circuit, mut reply: Value| {
        let graph = if live.is_some() {
            output.changed_live_graph(&original)
        } else {
            output.changed_graph(&original)
        };
        reply["circuit"] = graph.unwrap_or(Value::Null);
        reply
    };
    let circuit = original.clone();
    if pass == "RemoveUselessLeftJoinsVisitor" {
        let keyed: Vec<u64> = serde_json::from_slice(state)?;
        let keyed = keyed.into_iter().map(NodeId::Outer).collect();
        let (result, removed) =
            feldera_sql_opt::remove_useless_left_joins(circuit, &keyed, &mut next)?;
        return Ok(publish(&result, json!({
            "removed_left_joins": operator_numbers(removed), "unused_sources": [],
            "next_generated_name": next
        })));
    }
    if pass == "BuildNestedOperators" {
        let result = feldera_sql_opt::build_nested_operators(circuit, &mut next)?;
        let late = result
            .diagnostics
            .late_column
            .map(|(id, column)| json!({"operator":operator_number(id), "column":column}));
        return Ok(publish(&result.circuit, json!({
            "next_generated_name":next, "unused_sources":[],
            "unused_declarations":operator_numbers(result.diagnostics.unused_declarations), "late_column":late
        })));
    }
    let mut unused_sources = Vec::new();
    let result = match pass {
        "EnsureDistinctOutputs" => {
            let sinks = serde_json::from_slice::<Vec<_>>(state)?;
            feldera_sql_opt::ensure_distinct_outputs(circuit, &sinks, &mut next)?
        }
        "ImplementWindowGroups" => {
            let groups = serde_json::from_slice::<Vec<_>>(state)?;
            feldera_sql_opt::implement_window_groups(circuit, &groups, &mut next)?
        }
        "CollapseSharedChains" => {
            let chains: Vec<Vec<u64>> = serde_json::from_slice(state)?;
            let chains: Vec<_> = chains
                .into_iter()
                .map(|c| c.into_iter().map(NodeId::Outer).collect())
                .collect();
            feldera_sql_opt::collapse_shared_chains(circuit, &chains, &mut next)?
        }
        "DuplicateSharedIndexes" => feldera_sql_opt::duplicate_shared_indexes(circuit, &mut next)?,
        "ImplementChains" => feldera_sql_opt::implement_chains(circuit, &mut next)?,
        "EnsureExpressionTrees" => feldera_sql_opt::ensure_expression_trees(circuit, &mut next)?,
        "MergeRetain" => {
            let groups: Vec<Vec<u64>> = serde_json::from_slice(state)?;
            feldera_sql_opt::merge_retention_policies(circuit, &groups, &mut next)?
        }
        "MergeNoops" => {
            let inputs: feldera_sql_opt::SharedTraceInputs = serde_json::from_slice(state)?;
            inputs.apply(circuit, &mut next)?
        }
        "OptimizeDistinctVisitor" => feldera_sql_opt::optimize_distinct(circuit, &mut next)?,
        "UDFInliner" => feldera_sql_opt::inline_udfs(circuit, &mut next)?,
        "SubstituteLeftJoins" => feldera_sql_opt::substitute_left_joins(circuit, &mut next)?,
        "DeadCode" | "DeadCodeKeepSources" => {
            let result = feldera_sql_opt::eliminate_dead_code(
                circuit,
                &mut next,
                pass == "DeadCodeKeepSources",
            )?;
            for id in result.unused_sources {
                let NodeId::Outer(number) = id else {
                    unreachable!("table has an outer identity")
                };
                unused_sources.push(number);
            }
            result.circuit
        }
        "CSE" => {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Sidecar {
                seen_constants: Vec<u64>,
                shuffles: BTreeMap<u64, u64>,
            }
            let sidecar: Sidecar = serde_json::from_slice(state)?;
            let cse = feldera_sql_opt::CseState::with_seen_constants(
                sidecar.seen_constants.into_iter().map(NodeId::Outer),
            );
            let shuffles = sidecar
                .shuffles
                .into_iter()
                .map(|(id, group)| (NodeId::Inner(id), group))
                .collect();
            let aliases: Vec<_> = cse
                .aliases(&circuit, &shuffles)?
                .into_iter()
                .map(|(id, base)| [operator_number(id), operator_number(base)])
                .collect();
            return Ok(json!({"aliases":aliases}));
        }
        "IncrementalizeVisitor" => feldera_sql_opt::incrementalize(circuit, &mut next)?,
        "InsertWeightValidation" => feldera_sql_opt::insert_weight_validation(circuit, &mut next)?,
        "ExpandAggregateZero" => {
            let order: Vec<u64> = serde_json::from_slice(state)?;
            let order: Vec<_> = order.into_iter().map(NodeId::Outer).collect();
            feldera_sql_opt::expand_aggregate_zero(circuit, &mut next, &order)?
        }
        "ImplementJoins" => feldera_sql_opt::implement_joins(circuit, &mut next)?,
        "ShareOutputIntegrators" => feldera_sql_opt::share_output_integrators(circuit, &mut next)?,
        "FilterJoinVisitor" => feldera_sql_opt::filter_joins(circuit, &mut next)?,
        "SimplifyConditionals" => feldera_sql_opt::simplify_conditionals(circuit, &mut next)?,
        "SimplifyWaterline" => feldera_sql_opt::simplify_waterlines(circuit, &mut next)?,
        "DesugarNowEquality" => feldera_sql_opt::desugar_now_equality(circuit, &mut next)?,
        "LowerCircuitVisitor"
        | "ExpandStreamAggregates"
        | "Fuse"
        | "RemoveStarJoins"
        | "OptimizeProjectionVisitor"
        | "PullFilterVisitor"
        | "PropagateConstants"
        | "Simplify"
        | "DecomposeExpensiveFilters"
        | "BreakFilters" => {
            let mut weight = None;
            let (output, warnings) = match pass {
                "LowerCircuitVisitor" => feldera_sql_opt::lower_circuit(
                    circuit, lowering.as_ref().expect("bound lowering types"), &mut next,
                )?,
                "ExpandStreamAggregates" => {
                    let context = serde_json::from_slice(state)?;
                    let result =
                        feldera_sql_opt::expand_stream_aggregates(circuit, &context, &mut next)?;
                    weight = result.new_weight;
                    (result.circuit, result.warnings)
                }
                "RemoveStarJoins" => feldera_sql_opt::remove_star_joins(circuit, &mut next)?,
                "Fuse" => feldera_sql_opt::fuse_expensive_maps(
                    circuit, serde_json::from_slice(state)?, &mut next,
                )?,
                "OptimizeProjectionVisitor" => {
                    feldera_sql_opt::project_outputs(circuit, &mut next)?
                }
                "PullFilterVisitor" => feldera_sql_opt::pull_filters(circuit, &mut next)?,
                "PropagateConstants" => feldera_sql_opt::propagate_constants(circuit, &mut next)?,
                "Simplify" => feldera_sql_opt::simplify_expressions(circuit, &mut next)?,
                "BreakFilters" => feldera_sql_opt::break_temporal_filters(circuit, &mut next)?,
                _ => feldera_sql_opt::decompose_expensive_filters(circuit, &mut next)?,
            };
            let warnings: Vec<_> = warnings
                .into_iter()
                .map(|w| {
                    json!({
                        "range":w.position, "error_type":w.kind, "message":w.message,
                        "warning":true, "continuation":false
                    })
                })
                .collect();
            return Ok(publish(&output, json!({
                "diagnostics":warnings, "weight_node":weight, "next_generated_name":next,
                "unused_sources":[]
            })));
        }
        "RemoveIdentityOperators" => {
            feldera_sql_opt::remove_identity_operators(circuit, &mut next)?
        }
        "MoveGC" => feldera_sql_opt::move_asof_retention(circuit, serde_json::from_slice(state)?, &mut next)?,
        "CanonicalForm" => feldera_sql_opt::canonicalize_names(circuit, &mut next)?,
        "ReplaceStarJoins" => feldera_sql_opt::replace_star_joins(
            circuit, serde_json::from_slice(state)?, &mut next,
        )?,
        "ReplaceCommonProjections" => feldera_sql_opt::replace_common_projections(
            circuit, serde_json::from_slice(state)?, &mut next)?,
        "DeferCarriedColumns" => feldera_sql_opt::defer_carried_columns(circuit, serde_json::from_slice(state)?, &mut next)?,
        "TrimFilters" => feldera_sql_opt::trim_filters(circuit, serde_json::from_slice(state)?, &mut next)?,
        "TrimWindows" => feldera_sql_opt::trim_windows(circuit, &mut next)?,
        "TrimInputs" => {
            feldera_sql_opt::trim_inputs(circuit, &serde_json::from_slice(state)?, &mut next)?
        }
        "IndexedInputs" => feldera_sql_opt::expand_indexed_inputs(
            circuit,
            serde_json::from_slice(state)?,
            &mut next,
        )?,
        "ExpandHop" => feldera_sql_opt::expand_hop(circuit, &mut next)?,
        "LinearPostprocessRetainKeys" => {
            feldera_sql_opt::linear_postprocess_retain_keys(circuit, &mut next)?
        }
        "PushDifferentialsUp" => feldera_sql_opt::optimize_differentials(circuit, &mut next)?,
        "PropagateEmptySources" => feldera_sql_opt::propagate_empty_sources(circuit, &mut next)?,
        "RemoveConstantFilters" => feldera_sql_opt::remove_constant_filters(circuit, &mut next)?,
        "OptimizeIncrementalVisitor" => feldera_sql_opt::push_integrals(circuit, &mut next)?,
        "BalancedJoins" => feldera_sql_opt::optimize_balanced_joins(circuit, &mut next)?,
        "SeparateIntegrators" => feldera_sql_opt::separate_integrators(circuit, &mut next)?,
        "StrayGC" => {
            let (result, violation) =
                feldera_sql_opt::check_and_prune_retention(circuit, &mut next)?;
            if let Some(error) = violation {
                return Ok(json!({"retention_violation":error,"next_generated_name":next}));
            }
            result
        }
        "ChainVisitor" => feldera_sql_opt::optimize_chains(circuit, &mut next)?,
        "CloneOperatorsWithFanout" => feldera_sql_opt::optimize_fanout(circuit, &mut next)?,
        "MergeSums" => feldera_sql_opt::merge_sums(circuit, &mut next)?,
        "RemoveIAfterD" => feldera_sql_opt::remove_redundant_integrates(circuit, &mut next)?,
        "RemoveAllViews" | "RemoveViewOperators" => {
            let mode = if pass == "RemoveAllViews" {
                feldera_sql_opt::ViewRemoval::All
            } else {
                feldera_sql_opt::ViewRemoval::Ordinary { error_view }
            };
            feldera_sql_opt::remove_views(circuit, mode, &mut next)?
        }
        "RemoveNoops" => feldera_sql_opt::optimize_noops(circuit, &mut next)?,
        "FuseIntoArrayAgg" => feldera_sql_opt::fuse_into_array_agg(circuit, &mut next)?,
        "ComparatorDeclarations" => feldera_sql_opt::declare_comparators(circuit)?,
        "ExpandSafeCasts" | "ExpandUnsafeCasts" => {
            let safety = if pass == "ExpandSafeCasts" {
                feldera_sql_opt::CastSafety::Safe
            } else {
                feldera_sql_opt::CastSafety::Unsafe
            };
            feldera_sql_opt::expand_circuit_casts(circuit, &mut next, safety)?
        }
        "ExpandMetadataCasts" => feldera_sql_opt::expand_metadata_casts(circuit, &mut next)?,
        "CreateRuntimeErrorWrappers" => {
            feldera_sql_opt::create_runtime_error_wrappers(circuit, &mut next)?
        }
        "AdjustSqlIndex" => feldera_sql_opt::adjust_sql_index(circuit)?,
        "Gen2Passes" => feldera_sql_opt::gen2_passes(circuit)?,
        "RemoveDeindexOperators" => feldera_sql_opt::remove_deindex_operators(circuit, &mut next)?,
        other => return Err(format!("unknown optimizer pass {other}").into()),
    };
    Ok(publish(&result, json!({
        "next_generated_name": next, "unused_sources": unused_sources
    })))
}

// SAFETY: these export names and signatures match the static methods of RustCircuitPass.
// Each scope belongs to the calling thread and is closed by the Java finally block.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_dbsp_sqlCompiler_compiler_backend_RustCircuitPass_enterSnapshotScope(
    mut env: EnvUnowned<'_>,
    _class: JClass<'_>,
) {
    env.with_env(|_| -> Result<(), BridgeError> {
        LIVE_DECODERS.with_borrow_mut(|stack| stack.push(Default::default()));
        Ok(())
    })
    .resolve::<ThrowRuntimeExAndDefault>();
}

// SAFETY: the export name and signature match RustCircuitPass.leaveSnapshotScope.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_dbsp_sqlCompiler_compiler_backend_RustCircuitPass_leaveSnapshotScope(
    mut env: EnvUnowned<'_>,
    _class: JClass<'_>,
) {
    env.with_env(|_| -> Result<(), BridgeError> {
        LIVE_DECODERS.with_borrow_mut(|stack| stack.pop())
            .ok_or_else(|| BridgeError::Optimizer("no active optimizer snapshot scope".into()))?;
        Ok(())
    })
    .resolve::<ThrowRuntimeExAndDefault>();
}

// SAFETY: the export name is unique to the matching static native method in
// RustCircuitPass. EnvUnowned contains panics before returning to Java.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_dbsp_sqlCompiler_compiler_backend_RustCircuitPass_transformWithContext<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    pass: JString<'local>,
    circuit: JByteArray<'local>,
    next: i64,
    error_view: JString<'local>,
    state: JByteArray<'local>,
) -> JByteArray<'local> {
    env.with_env(|env| -> Result<_, BridgeError> {
        if circuit.len(env)?.saturating_add(state.len(env)?) > MAX_BYTES {
            return Err(BridgeError::Optimizer(
                "optimizer input exceeds 64 MiB".into(),
            ));
        }
        let bytes = env.convert_byte_array(&circuit)?;
        let next = u64::try_from(next).map_err(|e| BridgeError::Optimizer(e.to_string()))?;
        let state = env.convert_byte_array(&state)?;
        let pass = pass.to_string();
        let result = transform(&pass, &error_view.to_string(), &bytes, &state, next)
            .map_err(|err| BridgeError::Optimizer(format!("{pass}: {err}")))?;
        let bytes =
            serde_json::to_vec(&result).map_err(|e| BridgeError::Optimizer(e.to_string()))?;
        if bytes.len() > MAX_BYTES {
            return Err(BridgeError::Optimizer(
                "optimizer output exceeds 64 MiB".into(),
            ));
        }
        Ok(env.byte_array_from_slice(&bytes)?)
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
