//! Run typed circuit passes inside the compiler JVM. Java retains the original
//! objects when an invocation changes only the generated-name counter.
mod fragment;
mod repeat;
mod delta_expansion;
mod shared_indexes;
mod star_join;
mod temporal;
use feldera_sql_ir::{BodyOrder, Circuit, CircuitEditor, Field, NodeId, OutputPort};
use jni::{
    errors::ThrowRuntimeExAndDefault,
    objects::{JByteArray, JClass, JString},
    EnvUnowned,
};
use serde_json::{json, Value};
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap},
};

#[derive(Debug, thiserror::Error)]
enum BridgeError {
    #[error(transparent)]
    Jni(#[from] jni::errors::Error),
    #[error("{0}")]
    Optimizer(String),
}

#[derive(Default)]
struct LiveCompilation {
    decoder: feldera_sql_ir::LiveCircuitDecoder,
    // Repeated Simplify passes often see the same closure again. Only retain
    // a result if it returned the original root without emitting a warning.
    simplified: HashMap<Box<[u8]>, u64>,
    simplified_bytes: usize,
}

impl LiveCompilation {
    fn remember_simplified(&mut self, input: &[u8], root: u64) {
        const BYTE_BUDGET: usize = 4 * 1024 * 1024;
        const ENTRY_BUDGET: usize = 4096;
        if input.len() <= BYTE_BUDGET - self.simplified_bytes
            && self.simplified.len() < ENTRY_BUDGET
        {
            self.simplified.insert(input.into(), root);
            self.simplified_bytes += input.len();
        }
    }
}

// A compiler may optimize another circuit synchronously on the same JVM thread.
thread_local! {
    static LIVE_COMPILATIONS: RefCell<Vec<LiveCompilation>> = const { RefCell::new(Vec::new()) };
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

/// A waterline projection can nest as deeply as the SQL row it describes.
/// Keep the ordinary typed decoder on the common path and use the circuit
/// decoder's checked depth limit for deeply nested compiler input.
fn waterline_input<T: serde::de::DeserializeOwned>(state: &[u8]) -> serde_json::Result<T> {
    match serde_json::from_slice(state) {
        Err(error) if error.is_syntax() => {
            serde_json::from_value(feldera_sql_ir::parse_circuit_json(state)?)
        }
        result => result,
    }
}

fn map_chains(state: &[u8]) -> Result<Vec<Vec<NodeId>>, serde_json::Error> {
    let chains: Vec<Vec<u64>> = serde_json::from_slice(state)?;
    Ok(chains.into_iter().map(|chain| chain.into_iter().map(NodeId::Outer).collect()).collect())
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

#[derive(serde::Deserialize)]
struct ExpressionSite {
    empty: bool,
    position: Option<Value>,
}

impl ExpressionSite {
    fn arena(self, input: &[u8]) -> Result<(Circuit, Vec<NodeId>), Box<dyn std::error::Error>> {
        self.arena_with_operators(input, &[])
    }

    fn arena_with_operators(self, input: &[u8], operators: &[Value]) -> Result<(Circuit, Vec<NodeId>), Box<dyn std::error::Error>> {
        let mut header = json!({
            "class":"DBSPCircuit", "id":0, "declarations":[], "allOperators":operators, "sourceEmpty":self.empty
        });
        if let Some(position) = self.position {
            header["position"] = position;
        }
        lowering_arguments(Circuit::from_json(header)?, input)
    }
}

fn tuple_bound(input: &[u8], state: &[u8], next: u64) -> Result<Value, Box<dyn std::error::Error>> {
    #[derive(serde::Deserialize)]
    struct Request {
        #[serde(flatten)]
        site: ExpressionSite,
        raw: bool,
    }
    let request: Request = serde_json::from_slice(state)?;
    let (arena, roots) = request.site.arena(input)?;
    let mut edit = CircuitEditor::new(arena.clone());
    let fields = roots.into_iter().map(|id| edit.expression(id)).collect::<Result<Vec<_>, _>>()?;
    let bound = feldera_sql_opt::tuple_waterline(&mut edit, arena.root_id(), request.raw, &fields)?;
    lowered_function_reply(&arena, &edit, Some(bound), next, vec![])
}

fn binary_bound(input: &[u8], state: &[u8]) -> Result<Value, Box<dyn std::error::Error>> {
    #[derive(serde::Deserialize)]
    struct Request<'a> {
        #[serde(flatten)]
        site: ExpressionSite,
        #[serde(borrow)]
        operation: feldera_sql_opt::BinaryOperation<'a>,
    }
    let request: Request = serde_json::from_slice(state)?;
    let (arena, roots) = request.site.arena(input)?;
    let (result, operands) = roots.split_first().ok_or("missing binary result type")?;
    let mut edit = feldera_sql_ir::CircuitEditor::new(arena.clone());
    let mut operands = operands.iter();
    let mut bound = |present| -> Result<_, Box<dyn std::error::Error>> {
        Ok(if present {
            Some(edit.expression(*operands.next().ok_or("missing binary operand bound")?)?)
        } else {
            None
        })
    };
    let bounds = [bound(request.operation.left.monotone)?, bound(request.operation.right.monotone)?];
    if operands.next().is_some() {
        return Err("unexpected binary operand bound".into());
    }
    let result = edit.type_id(*result)?;
    let (rule, value) = request.operation.lower(&mut edit, arena.root_id(), result, bounds)?;
    let mut expressions = edit.circuit().additional_expressions(&arena, value.as_slice())?;
    Ok(json!({"rule": rule, "bound": expressions.pop()}))
}

fn finality_functions(input: &[u8], state: &[u8], mut next: u64) -> Result<Value, Box<dyn std::error::Error>> {
    #[derive(serde::Deserialize)]
    struct Request {
        #[serde(flatten)]
        site: ExpressionSite,
        #[serde(flatten)]
        column: feldera_sql_opt::limiters::FinalityColumn,
    }
    let request: Request = serde_json::from_slice(state)?;
    let (arena, roots) = request.site.arena(input)?;
    let [row, bound] = roots.as_slice() else {
        return Err("expected row and waterline types for EMIT FINAL".into());
    };
    let mut edit = feldera_sql_ir::CircuitEditor::new(arena.clone());
    let row = edit.type_id(*row)?;
    let bound = edit.type_id(*bound)?;
    let f = request.column.functions(&mut edit, arena.root_id(), row, bound, &mut next)?;
    let slots = [Some(f.bounds), f.filter, Some(f.index), f.project_bound];
    let roots: Vec<_> = slots.into_iter().flatten().collect();
    let mut encoded = edit.circuit().additional_expressions(&arena, &roots)?.into_iter();
    let functions = slots.map(|slot| slot.and_then(|_| encoded.next()));
    Ok(json!({"functions": functions, "next_generated_name": next}))
}

fn declared_waterline(input: &[u8], state: &[u8], mut next: u64) -> Result<Value, Box<dyn std::error::Error>> {
    #[derive(serde::Deserialize)]
    struct Request {
        columns: Vec<usize>,
        #[serde(flatten)]
        site: ExpressionSite,
    }
    let request: Request = serde_json::from_slice(state)?;
    let (arena, roots) = request.site.arena(input)?;
    let Some((&row, intervals)) = roots.split_first() else {
        return Err("missing waterline row".into());
    };
    if request.columns.len() != intervals.len() {
        return Err("expected one interval per LATENESS column".into());
    }
    let columns: Vec<_> = request
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

fn project_join_input(input: &[u8], state: &[u8], mut next: u64) -> Result<Value, Box<dyn std::error::Error>> {
    #[derive(serde::Deserialize)]
    struct Request {
        columns: Vec<usize>,
        #[serde(flatten)]
        site: ExpressionSite,
    }
    let request: Request = serde_json::from_slice(state)?;
    let (arena, roots) = request.site.arena(input)?;
    let [pair] = roots.as_slice() else {
        return Err("join input projection requires one indexed parameter type".into());
    };
    let mut edit = CircuitEditor::new(arena.clone());
    let pair = edit.type_id(*pair)?;
    let f = feldera_sql_opt::project_join_input(&mut edit, arena.root_id(), pair, &request.columns, &mut next)?;
    lowered_function_reply(&arena, &edit, Some(f), next, vec![])
}

fn narrow_join_inputs(input: &[u8], state: &[u8], mut next: u64) -> Result<Value, Box<dyn std::error::Error>> {
    use feldera_sql_opt::JoinInputRewrite;
    #[derive(serde::Deserialize)]
    struct JoinRequest {
        #[serde(flatten)]
        site: ExpressionSite,
        timestamps: Option<[usize; 2]>,
    }
    let request: JoinRequest = serde_json::from_slice(state)?;
    let (arena, roots) = request.site.arena(input)?;
    let (function, pairs) = roots.split_first().ok_or("missing join closure")?;
    let mut edit = CircuitEditor::new(arena.clone());
    match feldera_sql_opt::narrow_join_inputs(&mut edit, arena.root_id(), *function, pairs, request.timestamps, &mut next)? {
        JoinInputRewrite::Unsupported => Ok(Value::Null),
        JoinInputRewrite::Unchanged => Ok(json!({"narrowed": false})),
        JoinInputRewrite::Narrowed { inputs, reader, timestamps } => {
            let changed: Vec<_> = inputs.iter().map(Option::is_some).collect();
            let functions: Vec<_> = inputs.into_iter().flatten().chain([reader]).collect();
            Ok(json!({"narrowed": true, "changed": changed, "timestamps": timestamps, "next_generated_name": next,
                "functions": edit.circuit().additional_expressions(&arena, &functions)?}))
        }
    }
}

fn asof_waterline(input: &[u8], state: &[u8], mut next: u64) -> Result<Value, Box<dyn std::error::Error>> {
    #[derive(serde::Deserialize)]
    struct Request {
        timestamps: [usize; 2],
        #[serde(flatten)]
        site: ExpressionSite,
    }
    let request: Request = serde_json::from_slice(state)?;
    let (arena, roots) = request.site.arena(input)?;
    let &[key, join, right] = roots.as_slice() else {
        return Err("expected ASOF key type, join closure and right row type".into());
    };
    let mut edit = feldera_sql_ir::CircuitEditor::new(arena.clone());
    let function = edit.expression(join)?;
    let key = edit.type_id(key)?;
    let right = edit.type_id(right)?;
    let closure = feldera_sql_opt::asof_waterline(
        &mut edit, arena.root_id(), function, key, right, request.timestamps, &mut next,
    )?;
    lowered_function_reply(&arena, &edit, Some(closure), next, Vec::new())
}

fn hop_waterline(input: &[u8], state: &[u8], mut next: u64) -> Result<Value, Box<dyn std::error::Error>> {
    #[derive(serde::Deserialize)]
    struct Request {
        timestamp_index: usize,
        #[serde(flatten)]
        site: ExpressionSite,
    }
    let request: Request = serde_json::from_slice(state)?;
    let (arena, roots) = request.site.arena(input)?;
    let &[row, output, interval, size, offset] = roots.as_slice() else {
        return Err("expected HOP input and output types, interval, size and offset".into());
    };
    let mut edit = feldera_sql_ir::CircuitEditor::new(arena.clone());
    let row = edit.type_id(row)?;
    let output = edit.type_id(output)?;
    let intervals = [edit.expression(interval)?, edit.expression(size)?, edit.expression(offset)?];
    let closure = feldera_sql_opt::hop_waterline(
        &mut edit, arena.root_id(), row, output, request.timestamp_index, intervals, &mut next,
    )?;
    lowered_function_reply(&arena, &edit, Some(closure), next, Vec::new())
}

fn expand_stream_aggregate(input: &[u8], state: &[u8], mut next: u64) -> Result<Value, Box<dyn std::error::Error>> {
    #[derive(serde::Deserialize)]
    struct Request {
        append_only: bool,
        input_port: usize,
        #[serde(flatten)]
        site: ExpressionSite,
    }
    let request: Request = serde_json::from_slice(state)?;
    let (fragment, roots) = fragment::Fragment::bind(request.site, [feldera_sql_ir::OperatorKind::MapIndex], input)?;
    let [input_type, output_type, list, weight] = roots.as_slice() else {
        return Err("expected indexed input and output types, aggregate list and row weight".into());
    };
    let mut edit = CircuitEditor::new(fragment.base.clone());
    let aggregate = feldera_sql_opt::StreamAggregate {
        input: OutputPort { number: request.input_port, ..fragment.inputs[0] },
        input_type: edit.type_id(*input_type)?,
        output_type: edit.type_id(*output_type)?,
        list: *list,
        append_only: request.append_only,
        weight: edit.expression(*weight)?,
    };
    let body = fragment.base.root_id();
    let mut warnings = Vec::new();
    let output = aggregate.expand(&mut edit, body, body, &mut next, &mut warnings)?;
    let mut reply = fragment.reply(&edit, next)?;
    reply["output"] = operator_number(output).into();
    reply["warnings"] = pass_warnings(warnings).into();
    Ok(reply)
}

fn expand_hop(input: &[u8], state: &[u8], mut next: u64) -> Result<Value, Box<dyn std::error::Error>> {
    #[derive(serde::Deserialize)]
    struct Request {
        timestamp_index: usize,
        input_port: usize,
        #[serde(flatten)]
        site: ExpressionSite,
    }
    let request: Request = serde_json::from_slice(state)?;
    let (fragment, roots) = fragment::Fragment::bind(
        request.site, [feldera_sql_ir::OperatorKind::SourceMultiset], input,
    )?;
    let &[row, output, interval, size, offset] = roots.as_slice() else {
        return Err("expected HOP input and output rows, interval, size and offset".into());
    };
    let mut edit = CircuitEditor::new(fragment.base.clone());
    let site = fragment.base.root_id();
    let hop = feldera_sql_opt::Hop::new(
        &edit, site, edit.type_id(row)?, edit.type_id(output)?, request.timestamp_index,
        [edit.expression(interval)?, edit.expression(size)?, edit.expression(offset)?],
    )?;
    let source = OutputPort { number: request.input_port, ..fragment.inputs[0] };
    hop.append(&mut edit, site, source, &mut next)?;
    fragment.reply(&edit, next)
}

fn binding_pairs(roots: &[NodeId]) -> Result<&[[NodeId; 2]], &'static str> {
    let (pairs, rest) = roots.as_chunks::<2>();
    if rest.is_empty() {
        Ok(pairs)
    } else {
        Err("expected a variable and value for each lazy binding")
    }
}

/// The Java lowering writer can put deep subtrees after the arguments, where
/// the arena resolves them by ID without nesting the entire body in one object.
fn lowering_arguments(base: Circuit, input: &[u8]) -> Result<(Circuit, Vec<NodeId>), Box<dyn std::error::Error>> {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Batch<'a> {
        arguments: usize,
        #[serde(borrow)]
        nodes: &'a serde_json::value::RawValue,
    }

    if input.first() == Some(&b'{') {
        let batch: Batch = serde_json::from_slice(input)?;
        let (arena, mut roots) = base.with_inner_roots_bytes(batch.nodes.get().as_bytes())?;
        if batch.arguments > roots.len() {
            return Err("missing lowering arguments in expression batch".into());
        }
        roots.truncate(batch.arguments);
        Ok((arena, roots))
    } else {
        Ok(base.with_inner_roots_bytes(input)?)
    }
}

fn lower_function(pass: &str, input: &[u8], state: &[u8], mut next: u64) -> Result<Value, Box<dyn std::error::Error>> {
    if pass == "Simplify/Root" {
        let cached = LIVE_COMPILATIONS.with_borrow(|stack| stack.last()?.simplified.get(input).copied());
        if let Some(root) = cached {
            return Ok(json!({"root": {"node": root}, "next_generated_name": next, "warnings": []}));
        }
    }
    let (arena, roots) = if matches!(pass, "Monotonicity/Filter" | "Monotonicity/Minimum" | "Monotonicity/ChainMinimum") {
        serde_json::from_slice::<ExpressionSite>(state)?.arena(input)?
    } else {
        let header = json!({"class":"DBSPCircuit", "id":0, "declarations":[], "allOperators":[]});
        lowering_arguments(Circuit::from_json(header)?, input)?
    };
    if let ("KeyAnalysis/ColumnCopies" | "KeyAnalysis/OutputColumns" | "KeyAnalysis/JoinColumns" | "KeyAnalysis/FixedColumns" | "KeyAnalysis/FilterKeys", [closure]) = (pass, roots.as_slice()) {
        #[derive(serde::Deserialize)]
        struct Request {
            #[serde(flatten)]
            shape: feldera_sql_opt::OutputShape,
            lossless: bool,
            keys: Option<Vec<feldera_sql_opt::KeyColumns>>,
            projection: Option<feldera_sql_opt::KeyProjection>,
            join: Option<feldera_sql_opt::JoinInputs>,
        }
        let request: Request = serde_json::from_slice(state)?;
        let edit = CircuitEditor::new(arena);
        if matches!(pass, "KeyAnalysis/FixedColumns" | "KeyAnalysis/FilterKeys") {
            let fixed = feldera_sql_opt::fixed_columns(&edit, *closure, &request.shape, request.lossless)?;
            if pass == "KeyAnalysis/FilterKeys" {
                let keys = request.keys.ok_or("filter key analysis requires input keys")?;
                return Ok(serde_json::to_value(fixed.map(|fixed| feldera_sql_opt::filter_keys(keys, &fixed)))?);
            }
            return Ok(serde_json::to_value(fixed)?);
        }
        let copies = feldera_sql_opt::column_copies(&edit, *closure, &request.shape, request.lossless)?;
        if matches!(pass, "KeyAnalysis/OutputColumns" | "KeyAnalysis/JoinColumns") {
            let groups = feldera_sql_opt::equal_computations(&edit, *closure, &request.shape)?;
            let keys = if pass == "KeyAnalysis/JoinColumns" {
                let inputs = request.join.ok_or("join column analysis requires input keys")?;
                copies.as_ref().map(|copies| inputs.derive(copies))
            } else {
                match (request.projection, request.keys) {
                    (Some(projection), Some(keys)) => copies.as_ref().map(|copies| projection.keys(&keys, copies)),
                    (None, None) => None,
                    _ => return Err("column projection requires a row layout and input keys".into()),
                }
            };
            return Ok(json!({"copies": copies, "groups": groups, "keys": keys}));
        }
        return Ok(serde_json::to_value(copies)?);
    }
    if let ("ParameterFieldDependencies", [closure]) = (pass, roots.as_slice()) {
        return Ok(serde_json::to_value(feldera_sql_expr::live_parameter_field_paths(&arena, *closure)?)?);
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
    if let Some(name) = pass.strip_suffix("/Root") {
        use feldera_sql_expr::CastSafety;
        let &[root] = roots.as_slice() else {
            return Err(format!("{pass} requires one inner root").into());
        };
        let result = match name {
            "Simplify" => feldera_sql_expr::simplify_closure_with_warnings(&mut edit, root, Some(&mut warnings))?,
            "SimplifyWaterline" => feldera_sql_opt::simplify_waterline_root(&mut edit, root, &mut next, Some(&mut warnings))?,
            "SimplifyConditionals" => feldera_sql_opt::simplify_conditional_root(&mut edit, root)?,
            "AdjustSqlIndex" => feldera_sql_expr::adjust_index_root(&mut edit, root)?,
            "ExpandSafeCasts" => feldera_sql_expr::expand_sql_casts(&mut edit, root, CastSafety::Safe, &mut next)?,
            "ExpandUnsafeCasts" => feldera_sql_expr::expand_sql_casts(&mut edit, root, CastSafety::Unsafe, &mut next)?,
            _ => return Err(format!("unknown expression rewrite {name}").into()),
        };
        if name == "Simplify" && result == root && warnings.is_empty() {
            let NodeId::Inner(id) = root else { return Err("expected an inner simplification root".into()) };
            LIVE_COMPILATIONS.with_borrow_mut(|stack| {
                if let Some(compile) = stack.last_mut() {
                    compile.remember_simplified(input, id);
                }
            });
        }
        let encoded = edit.circuit().live_inner_node(&arena, result)?;
        return Ok(json!({"root": encoded,
            "next_generated_name": next, "warnings": pass_warnings(warnings)}));
    }
    let function = match (pass, roots.as_slice()) {
        ("FuseExpensiveMaps/Fragments", [closure]) => {
            return Ok(serde_json::to_value(feldera_sql_opt::map_fusion_fragments(&arena, *closure)?)?);
        }
        ("ImplementStatics/Candidates", [closure]) => {
            return Ok(serde_json::to_value(feldera_sql_opt::static_candidates(&arena, *closure)?)?);
        }
        ("ComparatorDeclarations/Functions", functions) => {
            let names = serde_json::from_slice(state)?;
            let replacements = feldera_sql_opt::rewrite_index_functions(&mut edit, functions, names)?;
            let items = edit.circuit().root().edges("declarations")?.into_iter()
                .map(|id| edit.node(id)?.edge("item"))
                .collect::<Result<Vec<_>, feldera_sql_ir::Error>>()?;
            // Encode both lists in the same identity space. A declaration can refer
            // to an original comparator that also appears in an untouched closure.
            let mut encoded = edit.circuit().additional_inner_nodes(&arena, replacements.into_iter().chain(items))?;
            let items = encoded.split_off(functions.len());
            return Ok(json!({"functions": encoded, "items": items}));
        }
        ("ExpandMetadataCasts/Initializer", [root]) => {
            let root = edit.expression(*root)?;
            feldera_sql_expr::prepare_column_default(&mut edit, root, &mut next)?
        }
        ("Monotonicity/WaterlineBindings", types) => {
            let names: Vec<&str> = serde_json::from_slice(state)?;
            if names.len() != types.len() {
                return Err("expected one name per waterline input type".into());
            }
            let inputs = names.into_iter().zip(types)
                .map(|(name, &ty)| Ok((name, edit.type_id(ty)?)))
                .collect::<Result<Vec<_>, feldera_sql_ir::Error>>()?;
            let bindings = feldera_sql_opt::waterline_bindings(&mut edit, &inputs)?;
            let roots = std::iter::once(bindings.parameter).chain(bindings.statements);
            return Ok(Value::Array(edit.circuit().additional_inner_nodes(&arena, roots)?));
        }
        ("Monotonicity/IsMinimumChain", [init, update]) => {
            return Ok(json!(feldera_sql_opt::is_minimum_chain(&mut edit, *init, *update)?));
        }
        ("SharedIndexes/Reader", [reader, bindings @ ..]) => {
            let columns: Vec<Vec<usize>> = serde_json::from_slice(state)?;
            let (pairs, rest) = bindings.as_chunks::<2>();
            if !rest.is_empty() || columns.len() != pairs.len() {
                return Err("expected a declaration, wide variable and layout for each shared input".into());
            }
            let parameters: Vec<_> = pairs.iter().zip(columns).map(|(&[declaration, variable], columns)| {
                feldera_sql_opt::IndexParameter { declaration, variable, columns }
            }).collect();
            let reader = feldera_sql_opt::rewrite_index_reader(&mut edit, *reader, &parameters)?;
            let nodes = edit.circuit().additional_inner_nodes(&arena, [reader])?;
            return Ok(nodes.into_iter().next().expect("one shared-index reader"));
        }
        ("SharedIndexes/Split", roots) => {
            let fixed: Vec<bool> = serde_json::from_slice(state)?;
            let groups = feldera_sql_opt::split_shared_indexes(&arena, roots, &fixed, &mut next, &mut warnings)?;
            return Ok(match groups {
                None => Value::Null,
                Some(groups) => json!({"groups": groups, "next_generated_name":next, "warnings": warnings.into_iter()
                    .map(|w| json!({"range":w.position, "error_type":w.kind, "message":w.message})).collect::<Vec<_>>()})
            });
        }
        ("SharedIndexes/WideFields", roots) => {
            let fields = feldera_sql_opt::wide_index_fields(&mut edit, roots, &mut next, &mut warnings)?;
            let widths: Vec<_> = fields.rows.iter().map(Vec::len).collect();
            let (columns, layout) = match fields.layout(&edit)? {
                Some(layout) => (layout.columns, Some(layout.positions)),
                None => (fields.rows.into_iter().flatten().collect(), None),
            };
            let roots: Vec<_> = [fields.variable, fields.key].into_iter().chain(columns).collect();
            let expressions = edit.circuit().additional_expressions(&arena, &roots)?;
            return Ok(json!({"expressions": expressions, "widths": widths, "layout": layout, "nullable": fields.nullable,
                "next_generated_name": next, "warnings": warnings.into_iter().map(|w| json!({
                    "range":w.position, "error_type":w.kind, "message":w.message,
                })).collect::<Vec<_>>()}));
        }
        ("TemporalGroups" | "MergeTemporalBounds", roots) => {
            return temporal::groups(&arena, &mut edit, roots, state, pass == "MergeTemporalBounds");
        }
        ("WindowGroups/ComposeTimestamp", [timestamp, maps @ ..]) => {
            let timestamp = edit.expression(*timestamp)?;
            feldera_sql_opt::compose_timestamp(&mut edit, timestamp, maps, &mut warnings)?
        }
        ("TemporalComparisons", roots) => return temporal::classify(&arena, &mut edit, roots, state),
        ("WindowGroups/Conjuncts", [parameter, body]) => {
            let body = edit.expression(*body)?;
            let Some(windows) = feldera_sql_opt::conjunct_windows(&mut edit, *parameter, body)? else {
                return Ok(Value::Null);
            };
            let windows = windows.into_iter().map(|window| window.map(|window| {
                let NodeId::Inner(key) = window.key.into() else { unreachable!("expression has inner identity") };
                json!({"key": key, "lower": window.lower})
            })).collect::<Vec<_>>();
            return Ok(serde_json::to_value(windows)?);
        }
        ("MergeFilters/Predicate", [source, filter]) => {
            let source = edit.expression(*source)?;
            let filter = edit.expression(*filter)?;
            feldera_sql_opt::merge_temporal_predicates(&mut edit, source, filter, &mut next, &mut warnings)?
        }
        ("Limiters/Window", [bounds, input @ ..]) if input.len() <= 1 => {
            let bounds = edit.expression(*bounds)?;
            let input = input.first().map(|id| edit.type_id(*id)).transpose()?;
            let window = feldera_sql_opt::limiters::window_waterline(
                &mut edit, bounds, input, waterline_input(state)?, &mut next,
            )?;
            let roots = [Some(window.endpoint), window.combine].into_iter().flatten().collect::<Vec<_>>();
            let functions = edit.circuit().additional_expressions(&arena, &roots)?;
            return Ok(json!({"functions": functions, "next_generated_name": next}));
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
            let shapes: [Option<feldera_sql_opt::limiters::Projection>; 2] = waterline_input(state)?;
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
            let [inherited, declared]: [feldera_sql_opt::limiters::Projection; 2] = waterline_input(state)?;
            let left = edit.type_id(*left)?;
            let right = edit.type_id(*right)?;
            inherited.merge_view_waterline(&mut edit, left, &declared, right, &mut next)?
        }
        ("Limiters/Project", [input, declared]) => {
            let [source, destination]: [feldera_sql_opt::limiters::Projection; 2] = waterline_input(state)?;
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
        ("EmptyRightJoin", [function, indexed]) => {
            let ty = edit.type_id(*indexed)?;
            feldera_sql_opt::empty_right_join_closure(&mut edit, *function, ty, &mut next)?
        }
        ("DeltaExpansion/UnmatchedRows", [function, indexed]) => {
            let ty = edit.type_id(*indexed)?;
            let projection = feldera_sql_opt::unmatched_row_projection(
                &mut edit, *function, ty, &mut next, Some(&mut warnings),
            )?;
            let tree = feldera_sql_expr::ensure_expression_tree(&mut edit, projection.into())?;
            edit.expression(tree)?
        }
        ("Monotonicity/Lag", [function, comparator, input]) => {
            let function = edit.expression(*function)?;
            let comparator = edit.expression(*comparator)?;
            let input = edit.type_id(*input)?;
            feldera_sql_opt::lag_transfer(&mut edit, function, comparator, input,
                serde_json::from_slice(state)?, &mut next, &mut warnings)?
        }
        ("Monotonicity/Group" | "Monotonicity/Rolling", [input, output]) => {
            use feldera_sql_opt::AggregateTransfer;
            let kind = if pass == "Monotonicity/Group" { AggregateTransfer::Group } else { AggregateTransfer::Rolling };
            let input = edit.type_id(*input)?;
            let output = edit.type_id(*output)?;
            feldera_sql_opt::aggregate_transfer(&mut edit, kind, input, output, &mut next)?
        }
        ("Monotonicity/Minimum", [input, postprocess @ ..]) if postprocess.len() <= 1 => {
            let pair = edit.type_id(*input)?;
            feldera_sql_opt::minimum_transfer(&mut edit, arena.root_id(), pair,
                postprocess.first().copied(), &mut next, &mut warnings)?
        }
        ("Monotonicity/ChainMinimum", [input, accumulators, update]) => {
            let input = edit.type_id(*input)?;
            let accumulators = edit.type_id(*accumulators)?;
            let function = feldera_sql_opt::chain_minimum_transfer(&mut edit, arena.root_id(),
                input, accumulators, *update, &mut next, &mut warnings)?;
            return lowered_function_reply(&arena, &edit, function, next, warnings);
        }
        ("Monotonicity/Literal", [id]) => {
            use feldera_sql_opt::LiteralWaterline;
            let literal = edit.expression(*id)?;
            let reply = match feldera_sql_opt::literal_waterline(&mut edit, literal)? {
                LiteralWaterline::Unbounded => json!({"kind": "unbounded"}),
                LiteralWaterline::TargetMaximum => json!({"kind": "target_maximum"}),
                LiteralWaterline::Bounded { bound, constant } => json!({
                    "kind": "bounded",
                    "bound": edit.circuit().live_expression(&arena, bound)?,
                    "constant": constant,
                    "positive": constant && feldera_sql_expr::literal_is_positive(&edit, literal)?,
                }),
            };
            return Ok(reply);
        }
        ("Monotonicity/Clock", [call]) => {
            let call = edit.expression(*call)?;
            feldera_sql_opt::clock_waterline(&mut edit, call)?
        }
        ("Monotonicity/FunctionBound", [call, inputs @ ..]) => {
            let (target, present): (&str, Vec<bool>) = serde_json::from_slice(state)?;
            let mut inputs = inputs.iter();
            let mut bounds = Vec::with_capacity(present.len());
            for bounded in present {
                bounds.push(if bounded {
                    Some(edit.expression(*inputs.next().ok_or("missing function argument bound")?)?)
                } else {
                    None
                });
            }
            if inputs.next().is_some() {
                return Err("unexpected function argument bound".into());
            }
            let call = edit.expression(*call)?;
            feldera_sql_opt::build_function_waterline(&mut edit, call, target, &bounds)?
        }
        ("Monotonicity/Unary", [operator, bound]) => {
            let operator = edit.expression(*operator)?;
            let bound = edit.expression(*bound)?;
            let Some(reduced) = feldera_sql_opt::unary_waterline(&mut edit, operator, bound)? else {
                return Ok(Value::Null);
            };
            reduced
        }
        ("TrimFilters/Closures", [reader, filter]) => {
            let functions = feldera_sql_opt::trim_filter_closures(&mut edit, *reader, *filter, &mut next)?;
            let encoded = functions.map(|fs| edit.circuit().live_inner_roots(&arena, &fs.map(NodeId::from))).transpose()?;
            return Ok(json!({"functions": encoded, "next_generated_name": next}));
        }
        ("Monotonicity/RowIdentityBounds" | "Monotonicity/IndexedIdentityBounds", [row, projected]) => {
            let row = edit.type_id(*row)?;
            let projected = edit.type_id(*projected)?;
            let indexed = pass == "Monotonicity/IndexedIdentityBounds";
            let pair = feldera_sql_opt::identity_bounds(&mut edit, row, projected, indexed, &mut next)?;
            let functions = edit.circuit().additional_expressions(&arena, &pair)?;
            return Ok(json!({"functions": functions, "next_generated_name": next}));
        }
        ("ProjectOutputs/Function", [before, reader, output]) => {
            let (kind, indexed, single_reader): (&str, bool, bool) = serde_json::from_slice(state)?;
            let projection = feldera_sql_opt::OutputProjection {
                producer: feldera_sql_ir::OperatorKind::from_wire(kind)
                    .ok_or_else(|| format!("unknown projection producer {kind}"))?,
                indexed, single_reader, output_type: edit.type_id(*output)?,
            };
            let before = edit.expression(*before)?;
            let Some((_, function)) = projection.apply(&mut edit, before, *reader, &mut warnings)? else {
                return Ok(Value::Null);
            };
            return lowered_function_reply(&arena, &edit, Some(function), next, warnings);
        }
        ("Monotonicity/ProjectClosure", [body, inputs @ ..]) => {
            #[derive(serde::Deserialize)]
            struct InputShapes {
                indexed: bool,
                shapes: Vec<feldera_sql_opt::limiters::Projection>,
            }
            let request: InputShapes = waterline_input(state)?;
            let body = edit.expression(*body)?;
            let (pairs, remainder) = inputs.as_chunks::<2>();
            if !remainder.is_empty() {
                return Err("missing projected type for waterline parameter".into());
            }
            let parameters = pairs.iter()
                .map(|&[parameter, ty]| edit.type_id(ty).map(|ty| (parameter, ty)))
                .collect::<Result<Vec<_>, _>>()?;
            let Some(result) = feldera_sql_opt::project_closure(&mut edit, body, &parameters, &request.shapes, request.indexed)? else {
                return Ok(Value::Null);
            };
            let closure = result.closure.map(|id| edit.circuit().live_expression(&arena, id)).transpose()?;
            return Ok(json!({"row_type": result.row_type, "closure": closure}));
        }
        ("Monotonicity/ProjectRow" | "Monotonicity/ProjectIndexedRow" | "Monotonicity/ProjectJoinRow", [body, bounds @ ..]) => {
            let indexed = pass == "Monotonicity/ProjectIndexedRow";
            let shapes = if pass == "Monotonicity/ProjectRow" {
                vec![waterline_input(state)?]
            } else {
                waterline_input(state)?
            };
            let body = edit.expression(*body)?;
            let bounds = bounds.iter().map(|&id| edit.expression(id)).collect::<Result<Vec<_>, _>>()?;
            let Some(value) = feldera_sql_opt::project_rows(&mut edit, body, &bounds, &shapes, indexed)? else {
                return Ok(Value::Null);
            };
            let reduced = value.reduced.map(|id| edit.circuit().live_expression(&arena, id)).transpose()?;
            return Ok(json!({"row_type": value.row_type, "bound": reduced}));
        }
        ("Monotonicity/Filter", [closure, row]) => {
            let closure = edit.expression(*closure)?;
            let row = edit.type_id(*row)?;
            feldera_sql_opt::filter_transfer(&mut edit, arena.root_id(), closure, row)?
        }
        ("InnerCSE/BorrowedVariables", places) => {
            return Ok(json!(feldera_sql_opt::borrowed_variables(&arena, places)?));
        }
        ("InnerCSE/ReadLazy", [source]) => {
            let borrowed = serde_json::from_slice(state)?;
            let roots = feldera_sql_opt::read_lazy_value(&mut edit, *source, borrowed, &mut next)?;
            let roots = edit.circuit().live_inner_roots(&arena, &roots.map(NodeId::from))?;
            return Ok(json!({"roots": roots, "next_generated_name": next}));
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
        ("PullFilters/Map" | "PullFilters/MapIndex", [producer, filter]) => {
            use feldera_sql_ir::OperatorKind;
            let kind = if pass == "PullFilters/Map" {
                OperatorKind::Map
            } else {
                OperatorKind::MapIndex
            };
            let function = feldera_sql_opt::pull_filter_through_map(
                &mut edit, kind, *producer, *filter, &mut warnings,
            )?;
            return lowered_function_reply(&arena, &edit, function, next, warnings);
        }
        ("PullFilters/Aggregate", [input, output, filter]) => {
            let input = edit.type_id(*input)?;
            let output = edit.type_id(*output)?;
            let function = feldera_sql_opt::pull_filter_through_aggregate(
                &mut edit, input, output, *filter, &mut next, &mut warnings,
            )?;
            return lowered_function_reply(&arena, &edit, function, next, warnings);
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
        ("RemoveUnusedFields/FlatmapInput", [flatmap]) => {
            let used = serde_json::from_slice(state)?;
            let Some(functions) = feldera_sql_opt::trim_flatmap_input(&mut edit, *flatmap, used, &mut next)? else {
                return Ok(Value::Null);
            };
            return Ok(json!({"functions": edit.circuit().additional_expressions(&arena, &functions)?,
                "next_generated_name": next}));
        }
        ("RemoveUnusedFields/AggregateReader", [aggregate, rows @ ..]) => {
            let used: Vec<bool> = serde_json::from_slice(state)?;
            let aggregate = feldera_sql_opt::rebind_projected_aggregate(&mut edit, *aggregate, rows, &used)?;
            return Ok(Value::Array(edit.circuit().additional_expressions(&arena, &[aggregate])?));
        }
        ("RemoveUnusedFields/Reader", [closure, parameters @ ..]) => {
            let used: Vec<Vec<bool>> = serde_json::from_slice(state)?;
            feldera_sql_opt::rebind_projected_reader(&mut edit, *closure, parameters, &used)?
        }
        ("RemoveUnusedFields/Row", [row]) => {
            let used = serde_json::from_slice(state)?;
            let row = edit.type_id(*row)?;
            feldera_sql_opt::project_used_fields(&mut edit, row, used, &mut next)?
        }
        ("RemoveUnusedFields/TopKFields", [rank, ties]) => {
            let fields = feldera_sql_opt::topk_compared_fields(&edit, [*rank, *ties])?;
            return Ok(serde_json::to_value(fields)?);
        }
        ("RemoveUnusedFields/TopKComparators", [rank, ties, row]) => {
            #[derive(serde::Deserialize)]
            struct Projection {
                row: String,
                used: Vec<bool>,
            }
            let projection: Projection = serde_json::from_slice(state)?;
            let row = edit.type_id(*row)?;
            let orders = feldera_sql_opt::project_topk_comparators(
                &mut edit, [*rank, *ties], row, &projection.row, &projection.used,
            )?;
            return Ok(serde_json::to_value(edit.circuit().additional_expressions(&arena, &orders)?)?);
        }
        ("RemoveUnusedFields/ParameterTypes", types) => {
            #[derive(serde::Deserialize)]
            struct Request { depth: usize, usage: Vec<Vec<bool>> }
            let request: Request = serde_json::from_slice(state)?;
            if types.len() != request.usage.len() {
                return Err("parameter count differs from the number of field usage maps".into());
            }
            let mut outputs = Vec::with_capacity(types.len());
            for (&ty, used) in types.iter().zip(&request.usage) {
                let ty = edit.type_id(ty)?;
                outputs.push(NodeId::from(feldera_sql_opt::compressed_field_type(&mut edit, ty, request.depth, used)?));
            }
            return Ok(serde_json::to_value(edit.circuit().additional_inner_nodes(&arena, outputs)?)?);
        }
        ("RemoveUnusedFields/IndexedValue", [row, input]) => {
            let used = serde_json::from_slice(state)?;
            let row = edit.type_id(*row)?;
            let input = edit.type_id(*input)?;
            let projection = feldera_sql_opt::project_used_fields(&mut edit, row, used, &mut next)?;
            feldera_sql_opt::project_indexed_value(&mut edit, input, projection, &mut next, &mut warnings)?
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
        ("Monotonicity/Flatmap", [flatmap]) => {
            feldera_sql_opt::flatmap_waterline(&mut edit, *flatmap, &mut next)?
        }
        ("LowerFlatmap", [flatmap, element]) => {
            let ty = edit.type_id(*element)?;
            feldera_sql_opt::lower_flatmap(&mut edit, *flatmap, ty, &mut next, &mut warnings)?
        }
        _ => return Err(format!("unexpected inputs to {pass}").into()),
    };
    lowered_function_reply(&arena, &edit, Some(function), next, warnings)
}

fn lowered_function_reply(arena: &Circuit, edit: &feldera_sql_ir::CircuitEditor,
    function: Option<feldera_sql_ir::ExpressionId>, next: u64, warnings: Vec<feldera_sql_expr::SimplifyWarning>,
) -> Result<Value, Box<dyn std::error::Error>> {
    let function = function.map(|root| edit.circuit().live_expression(arena, root)).transpose()?;
    Ok(json!({
        "function": function,
        "next_generated_name": next,
        "warnings": warnings.into_iter().map(|w| json!({
            "range": w.position, "error_type": w.kind, "message": w.message,
        })).collect::<Vec<_>>()
    }))
}

fn pass_warnings(warnings: Vec<feldera_sql_expr::SimplifyWarning>) -> Vec<Value> {
    warnings.into_iter().map(|w| json!({
        "range":w.position, "error_type":w.kind, "message":w.message,
        "warning":true, "continuation":false
    })).collect()
}

#[derive(serde::Serialize)]
#[serde(untagged)]
enum BoundReply {
    Nested(feldera_sql_opt::BoundSelection),
    Postorder(Vec<Value>),
}

impl BoundReply {
    fn encode(selection: feldera_sql_opt::BoundSelection, format: &[u8]) -> Self {
        if format == b"postorder" {
            Self::Postorder(selection.into_postorder())
        } else {
            Self::Nested(selection)
        }
    }
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
    match pass {
        "OptimizeProjections/Producer" => {
            let classes = serde_json::from_slice::<Vec<&str>>(input)?;
            let rules = classes.into_iter().find_map(feldera_sql_ir::OperatorKind::from_wire)
                .map(feldera_sql_opt::ProjectionRules::for_producer).unwrap_or_default();
            return Ok(serde_json::to_value(rules)?);
        }
        "RemoveUnusedFields/NarrowJoin" | "RemoveUnusedFields/NarrowAsof" => return narrow_join_inputs(input, state, next),
        "RemoveUnusedFields/JoinInput" => return project_join_input(input, state, next),
        "OptimizerOrder" => {
            let options = serde_json::from_slice(input)?;
            return Ok(serde_json::to_value(feldera_sql_opt::optimizer_order(options).collect::<Vec<_>>())?);
        }
        "ExpandStreamAggregates/List" => return expand_stream_aggregate(input, state, next),
        "DeltaExpansion/BinaryJoin" => return delta_expansion::binary(input, state, next),
        "DeltaExpansion/StarJoin" => return star_join::delta(input, state, next),
        "DeltaExpansion/Distinct" => return delta_expansion::unary(feldera_sql_ir::OperatorKind::Distinct, input, state, next),
        "DeltaExpansion/Positive" => return delta_expansion::unary(feldera_sql_ir::OperatorKind::Positive, input, state, next),
        "DeltaExpansion/Aggregate" => return delta_expansion::unary(feldera_sql_ir::OperatorKind::Aggregate, input, state, next),
        "RewriteNow/BuiltinClock" => return temporal::builtin_clock(input, next),
        "RewriteNow/TableClock" => return temporal::table_clock(input, next),
        "RewriteNow/Filter" => return temporal::filter(input, state, next),
        "RewriteNow/JoinedMap" => return temporal::clock_join(input, next, feldera_sql_opt::ClockConsumer::Map),
        "RewriteNow/JoinedFilter" => return temporal::clock_join(input, next, feldera_sql_opt::ClockConsumer::Filter),
        _ => {}
    }
    if pass == "RewriteNow/Window" {
        return temporal::window(input, state, next);
    }
    if pass == "RewriteNow/ClockSites" {
        let calls = serde_json::from_slice(input)?;
        return Ok(serde_json::to_value(feldera_sql_opt::temporal_clock_sites(calls))?);
    }
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
        "KeyAnalysis/Join" => {
            let request: feldera_sql_opt::JoinKeys = serde_json::from_slice(input)?;
            return Ok(serde_json::to_value(request.derive())?);
        }
        "Incrementalize/Boundaries" => {
            let operators: Vec<feldera_sql_opt::OperatorInputs> = serde_json::from_slice(input)?;
            return Ok(serde_json::to_value(feldera_sql_opt::incremental_boundaries(&operators))?);
        }
        "CSE/Candidates" => {
            let operators = serde_json::from_slice(input)?;
            return Ok(json!(operator_numbers(feldera_sql_opt::projected_cse_payloads(operators))));
        }
        "Prune/AllTables" | "Prune/UsedTables" => {
            let operators: Vec<feldera_sql_opt::OperatorInputs> = serde_json::from_slice(input)?;
            let pruning = feldera_sql_opt::projected_pruning(&operators, pass == "Prune/AllTables")?;
            return Ok(json!({"pruned_operators": operator_numbers(pruning.removed),
                             "unused_sources": operator_numbers(pruning.unused_sources)}));
        }
        "SharedProjections/Edits" => {
            #[derive(serde::Deserialize)]
            struct Site {
                id: u64,
                kind: String,
                reader: bool,
                indexed: Option<bool>,
            }
            let mut edits = BTreeMap::new();
            for site in serde_json::from_slice::<Vec<Site>>(input)? {
                let kind = feldera_sql_ir::OperatorKind::from_wire(&site.kind)
                    .ok_or_else(|| format!("unknown projection operator {}", site.kind))?;
                if let Some(edit) = feldera_sql_opt::projection_edits(kind, site.reader, site.indexed) {
                    edits.insert(site.id, edit);
                }
            }
            return Ok(serde_json::to_value(edits)?);
        }
        "Views/RemoveOrdinary" | "Views/RemoveAll" => {
            #[derive(serde::Deserialize)]
            struct View {
                id: u64,
                name: String,
                recursive: bool,
                lateness: bool,
                input: [u64; 2],
            }
            let mode = if pass == "Views/RemoveAll" {
                feldera_sql_opt::ViewRemoval::All
            } else {
                feldera_sql_opt::ViewRemoval::Ordinary { error_view }
            };
            let views: Vec<View> = serde_json::from_slice(input)?;
            let mut edits = BTreeMap::new();
            for view in views {
                if mode.removes(&view.name, view.recursive, view.lateness) {
                    edits.insert(view.id, json!({"kind": "Forward", "inputs": [view.input]}));
                }
            }
            return Ok(json!(edits));
        }
        "Monotonicity/JoinArguments" => {
            let inputs = serde_json::from_slice(input)?;
            let reply = feldera_sql_opt::join_arguments(inputs)
                .map(|args| args.map(|arg| BoundReply::encode(arg, state)));
            return Ok(serde_json::to_value(reply)?);
        }
        "Monotonicity/AntiJoinProjection" => {
            let inputs = serde_json::from_slice(input)?;
            let reply = feldera_sql_opt::antijoin_waterline(inputs)
                .map(|args| args.map(|arg| BoundReply::encode(arg, state)));
            return Ok(serde_json::to_value(reply)?);
        }
        "Monotonicity/Union" | "Monotonicity/Intersection" => {
            use feldera_sql_opt::BoundCombination::{Intersection, Union};
            let mode = if pass == "Monotonicity/Union" { Union } else { Intersection };
            let inputs = serde_json::from_slice(input)?;
            let reply = feldera_sql_opt::combine_bounds(inputs, mode)
                .map(|bound| BoundReply::encode(bound, state));
            return Ok(serde_json::to_value(reply)?);
        }
        "Monotonicity/Function" => {
            let (name, arguments): (String, Vec<feldera_sql_opt::WaterlineArgument>) = serde_json::from_slice(input)?;
            return Ok(serde_json::to_value(feldera_sql_opt::function_waterline(&name, &arguments))?);
        }
        "Monotonicity/BinaryBound" => return binary_bound(input, state),
        "Monotonicity/TupleBound" => return tuple_bound(input, state, next),
        "InnerCSE/Bindings" => {
            let scope: feldera_sql_opt::BindingScope = serde_json::from_slice(state)?;
            let bindings: Vec<feldera_sql_opt::PendingBinding> = serde_json::from_slice(input)?;
            return Ok(serde_json::to_value(scope.placements(&bindings))?);
        }
        "InnerCSE/NumberValues" => {
            let request: feldera_sql_opt::NumberingInput = serde_json::from_slice(input)?;
            return Ok(serde_json::to_value(request.number()?)?);
        }
        "InnerCSE/Uses" => {
            let graph: Vec<feldera_sql_opt::NumberedExpression> = serde_json::from_slice(input)?;
            return Ok(serde_json::to_value(feldera_sql_opt::shared_expression_uses(&graph))?);
        }
        "Monotonicity/Asof" => return asof_waterline(input, state, next),
        "Monotonicity/Hop" => return hop_waterline(input, state, next),
        "ExpandHop/Window" => return expand_hop(input, state, next),
        "Limiters/Finality" => return finality_functions(input, state, next),
        "Limiters/DeclaredWaterline" => return declared_waterline(input, state, next),
        "SharedIndexes/Reader" | "SharedIndexes/Split" | "SharedIndexes/WideFields"
        | "TemporalComparisons" | "TemporalGroups" | "MergeTemporalBounds" | "WindowGroups/ComposeTimestamp" | "WindowGroups/Conjuncts"
        | "MergeFilters/Predicate" | "Monotonicity/Cast" | "Limiters/Apply" | "Limiters/Minimum" | "Limiters/Join" | "Limiters/RowFilter" | "Limiters/Window" | "Limiters/KeyFilter" | "Limiters/MergeView" | "Limiters/Project" | "Statics/RenumberErrors" | "Statics/Inline" | "Statics/Define" | "Statics/Declare"
        | "EmptyRightJoin" | "DeltaExpansion/UnmatchedRows"
        | "Monotonicity/RowIdentityBounds" | "Monotonicity/IndexedIdentityBounds" | "Monotonicity/FunctionBound"
        | "Monotonicity/Clock"
        | "Monotonicity/ProjectClosure" | "Monotonicity/WaterlineBindings" | "Monotonicity/Flatmap" | "Monotonicity/Lag" | "Monotonicity/Group" | "Monotonicity/Rolling" | "Monotonicity/Minimum" | "Monotonicity/IsMinimumChain" | "Monotonicity/ChainMinimum" | "Monotonicity/Unary" | "Monotonicity/Literal" | "ProjectOutputs/Function" | "Monotonicity/ProjectRow" | "Monotonicity/ProjectIndexedRow" | "Monotonicity/ProjectJoinRow" | "Monotonicity/Filter"
        | "ComparatorDeclarations/Functions" | "Simplify/Root" | "SimplifyWaterline/Root" | "SimplifyConditionals/Root" | "ExpandSafeCasts/Root" | "ExpandUnsafeCasts/Root" | "AdjustSqlIndex/Root"
        | "KeyAnalysis/ColumnCopies" | "KeyAnalysis/OutputColumns" | "KeyAnalysis/JoinColumns" | "KeyAnalysis/FixedColumns" | "KeyAnalysis/FilterKeys" | "ExpandMetadataCasts/Initializer" | "TrimFilters/Closures" | "InnerCSE/Let" | "InnerCSE/Statements" | "ParameterFieldDependencies" | "SplitRowMapInput" | "SplitIndexedMapInput" | "LowerAggregateFold" | "LowerFlatmap" | "ComposeProjection" | "TryComposeProjection"
        | "PullFilters/Map" | "PullFilters/MapIndex" | "PullFilters/Aggregate"
        | "ComposeIndexedProjection" | "TryIndexedProjection" | "ComposeIndexedJoinOutput" | "AntijoinProjection"
        | "SplitAntijoinProjection" | "IsNestedProjection" | "IsStrictProjection" | "TrimTopKProjection"
        | "RemoveUnusedFields/Row" | "RemoveUnusedFields/IndexedValue" | "RemoveUnusedFields/ParameterTypes"
        | "RemoveUnusedFields/Reader" | "RemoveUnusedFields/AggregateReader" | "RemoveUnusedFields/FlatmapInput" | "RemoveUnusedFields/TopKFields" | "RemoveUnusedFields/TopKComparators"
        | "TrimAggregateProjection" | "ProjectUnnest" | "ProjectAggregateZero" | "IsIdentityProjection"
        | "FuseExpensiveMaps/Fragments" | "ImplementStatics/Candidates" | "InnerCSE/ReadLazy" | "InnerCSE/BorrowedVariables" => {
            return lower_function(pass, input, state, next);
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
            let threshold = temporal::sharing_threshold(state)?;
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
            let threshold = temporal::sharing_threshold(state)?;
            return Ok(json!(feldera_sql_opt::shared_temporal_inputs(
                &filters, threshold
            )));
        }
        "LeftJoinChains/Steps" => {
            let graph = serde_json::from_slice::<Vec<_>>(input)?;
            return Ok(serde_json::to_value(feldera_sql_opt::chain_steps(&graph)?)?);
        }
        "LeftJoinChains/Columns" => {
            let facts = serde_json::from_slice(input)?;
            return Ok(serde_json::to_value(feldera_sql_opt::carried_columns(facts)?)?);
        }
        "LeftJoinChains/Key" => {
            let facts = serde_json::from_slice(input)?;
            return Ok(serde_json::to_value(feldera_sql_opt::choose_chain_key(&facts))?);
        }
        _ => {}
    }
    let circuit = if live.is_some() {
        LIVE_COMPILATIONS.with_borrow_mut(|stack| match stack.last_mut() {
            Some(compile) => compile.decoder.decode(input),
            None => Circuit::from_live_json_bytes(input),
        })?
    } else {
        Circuit::from_json_bytes(input)?
    };
    if pass == "KeyAnalysis/Circuit" {
        let edit = feldera_sql_ir::CircuitEditor::new(circuit);
        let keys = feldera_sql_opt::circuit_keys(&edit, serde_json::from_slice(state)?)?;
        return Ok(serde_json::to_value(keys)?);
    }
    if pass == "Lineage" {
        let edit = feldera_sql_ir::CircuitEditor::new(circuit);
        return Ok(serde_json::to_value(feldera_sql_opt::correlated_columns(&edit)?)?);
    }
    if live == Some("OptimizeIncrementalVisitor/Signatures") {
        let output = feldera_sql_opt::push_integrals(circuit.clone(), &mut next)?;
        return Ok(match output.retained_live_graph(&circuit) {
            Ok(graph) => {
                let mut reply = json!({"unused_sources": [], "next_generated_name": next});
                reply["circuit"] = graph.unwrap_or(Value::Null);
                reply
            }
            Err(_) => json!({"needs_full_snapshot": true}),
        });
    }
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
    if pass == "Topology/FindSharedIndexes" {
        return Ok(json!(feldera_sql_opt::shared_index_payloads(&circuit)?));
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
    if pass == "CollapseSharedChains/Closures" {
        let mut edit = feldera_sql_ir::CircuitEditor::new(circuit.clone());
        let indexes = feldera_sql_opt::collapsed_indexes(&mut edit, &map_chains(state)?)?;
        let roots: Vec<_> = indexes.iter().map(|index| index.function).collect();
        let closures = edit.circuit().additional_expressions(&circuit, &roots)?;
        let records = indexes.into_iter().zip(closures).map(|(index, function)| {
            json!({"tail": operator_number(index.tail), "source": operator_number(index.input.operator),
                   "port": index.input.number, "function": function})
        }).collect::<Vec<_>>();
        return Ok(json!(records));
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
    if pass == "Topology/ValidateStreamKinds" {
        let violation = feldera_sql_ir::OutputClocks::new(&circuit)?.validate()?;
        return Ok(serde_json::to_value(violation)?);
    }
    if pass == "Topology/Monotonicity" {
        #[derive(serde::Serialize)]
        struct Step {
            operator: u64,
            #[serde(flatten)]
            transfer: feldera_sql_opt::MonotonicityTransfer,
        }
        let steps: Vec<_> = feldera_sql_opt::monotonicity_transfers(&circuit)?
            .into_iter()
            .map(|(id, transfer)| Step { operator: operator_number(id), transfer })
            .collect();
        return Ok(serde_json::to_value(steps)?);
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
    if pass == "WindowGroups/Scan" {
        return temporal::scan(circuit, temporal::sharing_threshold(state)?);
    }
    if pass == "WindowGroups/FilterSources" {
        return Ok(temporal::source_paths(feldera_sql_opt::temporal_filter_sources(circuit)?));
    }
    if pass == "WindowGroups/Indexes" {
        use feldera_sql_opt::IndexedWindows;
        let threshold = temporal::sharing_threshold(state)?;
        return Ok(match feldera_sql_opt::indexed_window_groups(circuit, threshold)? {
            IndexedWindows::Groups(plans) => json!({"plans": plans}),
            IndexedWindows::Candidates(windows) => json!({"candidates": operator_numbers(windows)}),
        });
    }
    if pass == "FindWindowGroups" {
        let candidates = feldera_sql_opt::window_candidates(&circuit)?;
        return Ok(json!(operator_numbers(candidates)));
    }
    if let Some(name) = pass.strip_prefix("Topology/") {
        if name == "OrderedIndexes" {
            return Ok(json!(operator_numbers(feldera_sql_opt::ordered_indexes(&circuit)?)));
        }
        if name == "OptimizeIncrementalVisitor" {
            use feldera_sql_opt::IntegralEdit;
            if let Some(edits) = feldera_sql_opt::flat_integral_edits(&circuit)? {
                let actions: BTreeMap<_, _> = edits.into_iter().map(|(id, edit)| {
                    let action = match edit {
                        IntegralEdit::Bypass => "Bypass",
                        IntegralEdit::Push => "Push",
                        IntegralEdit::ReplaceAndPush(kind) => kind.wire_name(),
                    };
                    (operator_number(id), action)
                }).collect();
                return Ok(json!({"integral_edits": actions}));
            }
        }
        if name == "LinearPostprocessRetainKeys" {
            let Some(plan) = feldera_sql_opt::linear_retention_plan(&circuit)? else {
                return Ok(Value::Null);
            };
            let replacements: BTreeMap<_, _> = plan.replacements.into_iter()
                .map(|(aggregate, retainer)| (operator_number(aggregate), operator_number(retainer)))
                .collect();
            return Ok(json!({"order": operator_numbers(plan.order), "retainers": replacements}));
        }
        if name == "ChainReaders" {
            return Ok(json!(operator_numbers(feldera_sql_opt::chain_readers(&circuit)?)));
        }
        if name == "TrimFilters" {
            let readers = feldera_sql_opt::trim_filter_readers(&circuit, serde_json::from_slice(state)?)?;
            return Ok(json!(operator_numbers(readers)));
        }
        if name == "BypassNoops" {
            return Ok(json!(operator_numbers(feldera_sql_opt::bypassed_noops(&circuit)?)));
        }
        if name == "MergeNoops" {
            let inputs: feldera_sql_opt::SharedTraceInputs = serde_json::from_slice(state)?;
            inputs.coalesce(circuit)?;
            let mut edits = BTreeMap::new();
            for (from, to) in inputs.aliases {
                edits.insert(from, json!({"kind": "Forward", "inputs": [[to, 0]]}));
            }
            for discarded in inputs.removed {
                edits.insert(discarded, json!({"kind": "Discard"}));
            }
            return Ok(serde_json::to_value(edits)?);
        }
        if name == "PullFilterVisitor" {
            if let Some(sites) = feldera_sql_opt::pull_filter_sites(&circuit)? {
                return Ok(json!({"pull_filters": sites}));
            }
        }
        if name == "DeindexMaps" {
            let edits: BTreeMap<_, _> = feldera_sql_opt::deindex_operators(&circuit)?
                .into_iter()
                .map(|id| (operator_number(id), json!({"kind": "MapWithFunction"})))
                .collect();
            return Ok(serde_json::to_value(edits)?);
        }
        if name == "FanoutProducers" {
            return Ok(json!(operator_numbers(feldera_sql_opt::fanout_producers(&circuit)?)));
        }
        if name == "CSE" {
            return Ok(
                json!({"payloads": operator_numbers(feldera_sql_opt::cse_payloads(&circuit)?)}),
            );
        }
        if name == "PropagateEmptySources" {
            use feldera_sql_ir::StreamKind;
            use feldera_sql_opt::EmptyStreamEdit::*;
            let edits = feldera_sql_opt::empty_stream_edits(&circuit)?;
            let port = |p: OutputPort| json!([operator_number(p.operator), p.number]);
            let mut actions = BTreeMap::new();
            for (id, edit) in edits {
                let action = match edit {
                    Constant(StreamKind::Delta) => json!({"kind":"Delta"}),
                    Constant(StreamKind::Collection) => json!({"kind":"Collection"}),
                    Constant(StreamKind::Waterline) => return Err("empty multiset has a waterline clock".into()),
                    Forward(p) => json!({"kind":"Forward","inputs":[port(p)]}),
                    LeftJoin(p) => json!({"kind":"LeftJoin","inputs":[port(p)]}),
                    Sum(ports) => json!({"kind":"Sum","inputs":ports.into_iter().map(port).collect::<Vec<_>>()}),
                };
                actions.insert(operator_number(id), action);
            }
            return Ok(json!({"empty_stream_edits": actions}));
        }
        if name == "SeparateIntegrators" {
            if let Some(id) = feldera_sql_opt::snapshot_aggregate(&circuit)? {
                return Ok(json!({"stream_aggregate":operator_number(id)}));
            }
            let slots: BTreeMap<_, _> = feldera_sql_opt::separate_trace_inputs(&circuit)?
                .into_iter()
                .map(|(reader, slots)| (operator_number(reader), slots))
                .collect();
            return Ok(json!({"trace_inputs": slots}));
        }
        if name == "StrayGC" {
            if let Some(error) = feldera_sql_opt::retention_violation(&circuit)? {
                return Ok(json!({"retention_violation": error}));
            }
            return Ok(json!({"needs_full_snapshot":false}));
        }
        if matches!(name, "DeadCode" | "DeadCodeKeepSources") {
            let pruning = feldera_sql_opt::pruning(&circuit, name == "DeadCodeKeepSources")?;
            return Ok(json!({"pruned_operators":operator_numbers(pruning.removed),
                "unused_sources":operator_numbers(pruning.unused_sources)}));
        }
        let needs_full_snapshot = feldera_sql_opt::needs_full_snapshot(name, &circuit)?;
        return Ok(json!({ "needs_full_snapshot": needs_full_snapshot }));
    }
    if pass == "FanoutReaders" {
        return Ok(json!(operator_numbers(feldera_sql_opt::fanout_readers(&circuit)?)));
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
    if pass == "FindFusable" {
        return Ok(serde_json::to_value(feldera_sql_opt::ArrayFusions::find(&CircuitEditor::new(circuit))?)?);
    }
    if pass == "DetectConflicts" {
        return Ok(json!(feldera_sql_opt::conflicting_regions(&circuit)?));
    }
    if pass == "IdentityDecisions" {
        let mut editor = CircuitEditor::new(circuit);
        let actions: BTreeMap<_, _> = feldera_sql_opt::identity_decisions(&mut editor, &mut next)?
            .into_iter().map(|(id, remove)| (operator_number(id), remove)).collect();
        return Ok(json!({"identity_actions":actions, "next_generated_name":next}));
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
        "CollapseSharedChains" => feldera_sql_opt::collapse_shared_chains(circuit, &map_chains(state)?, &mut next)?,
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
        "WindowGroups/Reorder" => {
            let chosen = serde_json::from_slice(state)?;
            let Some(output) = feldera_sql_opt::reorder_shared_windows(circuit, &chosen, &mut next)? else {
                return Ok(Value::Null);
            };
            output
        }
        "Reorder" => feldera_sql_opt::reorder_temporal_filters(circuit, serde_json::from_slice(state)?, &mut next)?,
        "DesugarNowEquality" => feldera_sql_opt::desugar_now_equality(circuit, &mut next)?,
        "LowerCircuitVisitor"
        | "LowerAsofInner"
        | "InsertLimiters"
        | "ExpandStreamAggregates"
        | "Fuse"
        | "RemoveStarJoinsPass"
        | "RemoveStarJoins"
        | "OptimizeProjectionVisitor"
        | "PullFilterVisitor"
        | "PropagateConstants"
        | "Simplify"
        | "DecomposeExpensiveFilters"
        | "ReplaceSharedIndexes"
        | "MergeFilters" | "BreakFilters" => {
            let mut weight = None;
            let mut retention = None;
            let (output, warnings) = match pass {
                "InsertLimiters" => {
                    let mut warnings = Vec::new();
                    use feldera_sql_opt::limiters::LimiterError;
                    let (output, counter) = match feldera_sql_opt::limiters::insert_limiters_with_warnings(
                        circuit, waterline_input(state)?, &mut warnings,
                    ) {
                        Ok(result) => result,
                        Err(error) => {
                            let message = error.to_string();
                            let (finding, counter) = match error {
                                LimiterError::Gen2KeyLateness { operator, next, .. } =>
                                    (json!({"kind":"gen2_key_lateness", "operator":operator}), next),
                                LimiterError::MissingWaterline { operator, row, next, .. } =>
                                    (json!({"kind":"missing_waterline", "operator":operator, "column":row}), next),
                                LimiterError::InvalidCircuit(error) => return Err(error.into()),
                            };
                            return Ok(json!({"limiter_refusal":finding, "message":message,
                                "next_generated_name":counter, "diagnostics":pass_warnings(warnings)}));
                        }
                    };
                    next = counter;
                    (output, warnings)
                }
                "ReplaceSharedIndexes" => {
                    let groups = serde_json::from_slice::<Vec<Vec<feldera_sql_opt::IndexReader>>>(state)?;
                    let mut warnings = Vec::new();
                    let Some(output) = feldera_sql_opt::replace_shared_indexes(circuit, &groups, &mut next, &mut warnings)? else {
                        return Ok(Value::Null);
                    };
                    (output, warnings)
                }
                "LowerAsofInner" => {
                    let lowered = feldera_sql_opt::lower_asof(circuit, serde_json::from_slice(state)?, &mut next)?;
                    retention = Some(lowered.retention.into_iter().map(|binding| json!({
                        "retainer":operator_number(binding.retainer), "join":operator_number(binding.join),
                        "rebuilt":binding.rebuilt,
                    })).collect::<Vec<_>>());
                    (lowered.circuit, lowered.warnings)
                }
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
                "RemoveStarJoinsPass" => {
                    let mut warnings = Vec::new();
                    let result = feldera_sql_opt::expand_star_join_layer(circuit, &mut next, &mut warnings)?;
                    (result, warnings)
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
                "MergeFilters" => {
                    let mut warnings = Vec::new();
                    let Some(output) = feldera_sql_opt::merge_temporal_filters(circuit, &mut next, &mut warnings)? else {
                        return Ok(Value::Null);
                    };
                    (output, warnings)
                }
                "BreakFilters" => feldera_sql_opt::break_temporal_filters(circuit, &mut next)?,
                _ => feldera_sql_opt::decompose_expensive_filters(circuit, &mut next)?,
            };
            let mut reply = json!({
                "diagnostics":pass_warnings(warnings), "weight_node":weight, "next_generated_name":next, "unused_sources":[]
            });
            if let Some(retention) = retention {
                reply["asof_retention"] = Value::Array(retention);
            }
            return Ok(publish(&output, reply));
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
        "PushDifferentialsUp" => feldera_sql_opt::push_differentials_up(circuit, &mut next)?,
        "PropagateEmptySources" => feldera_sql_opt::propagate_empty_sources(circuit, &mut next)?,
        "RemoveConstantFilters" => feldera_sql_opt::remove_constant_filters(circuit, &mut next)?,
        "OptimizeIncrementalVisitor" => feldera_sql_opt::push_integrals(circuit, &mut next)?,
        "BalancedJoins" => feldera_sql_opt::balance_joins(circuit, &mut next)?,
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
        "Unbox" => feldera_sql_opt::remove_typed_boxes(circuit)?,
        "RemoveClones" => feldera_sql_opt::remove_clones(circuit)?,
        "FuseArrayReaders" => {
            let matches: feldera_sql_opt::ArrayFusions = serde_json::from_slice(state)?;
            matches.apply(CircuitEditor::new(circuit), &mut next)?
        }
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
        LIVE_COMPILATIONS.with_borrow_mut(|stack| stack.push(Default::default()));
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
        LIVE_COMPILATIONS.with_borrow_mut(|stack| stack.pop())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_simplification_preserves_names_and_reports_each_warning() {
        let int = json!({"class":"DBSPTypeInteger","id":2,"code":"INT32","width":32,"signed":true,"mayBeNull":false});
        let literal = serde_json::to_vec(&json!([{
            "class":"DBSPI32Literal","id":1,"type":int,"value":"3"
        }])).unwrap();
        let invalid_cast = serde_json::to_vec(&json!([{
            "class":"DBSPCastExpression","id":1,"safe":"SqlUnsafe","type":int,
            "source":{"class":"DBSPStringLiteral","id":3,"value":"oops","charset":"UTF-8",
                "type":{"class":"DBSPTypeString","id":4,"code":"STRING","precision":-1,"fixed":false,"mayBeNull":false}}
        }])).unwrap();
        LIVE_COMPILATIONS.with_borrow_mut(|stack| stack.push(Default::default()));
        for next in [0, 27] {
            let same = lower_function("Simplify/Root", &literal, &[], next).unwrap();
            assert_eq!(same, json!({"root":{"node":1},"warnings":[],"next_generated_name":next}));
            // Both roots have ID 1. The serialized graph distinguishes them.
            let warning = lower_function("Simplify/Root", &invalid_cast, &[], next).unwrap();
            assert_eq!(warning["warnings"][0]["error_type"], "Not a number");
            assert_eq!(warning["next_generated_name"], next);
        }
        let scope = LIVE_COMPILATIONS.with_borrow_mut(|stack| stack.pop().unwrap());
        assert_eq!(scope.simplified.len(), 1);
        assert_eq!(scope.simplified.get(literal.as_slice()), Some(&1));
        assert_eq!(scope.simplified_bytes, literal.len());
    }
}
