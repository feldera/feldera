//! Apply the source, view and bound-combination portion of InsertLimiters to
//! the original circuit, using the analysis from its matching expansion.

use super::source::{Columns, LatenessSite, source_bounds};
use super::{Bound, Projection, Stream, StreamKind, connect_error_view, minimum_bound};
use crate::rebuild::ProjectionNames;
use feldera_sql_ir::{Circuit, CircuitEditor, Error, Field, Node, NodeId, OperatorKind as Op};
use feldera_sql_ir::{OutputClocks, OutputPort, StreamKind as Clock, provenance::SourceRange};
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Deserialize)]
struct OperatorFacts {
    position: SourceRange,
    reachable_from_error: bool,
    input_kinds: Option<Vec<Clock>>,
    projection: Option<Projection>,
    input_projection: Option<Projection>,
    aggregate: Option<super::aggregate::AggregateFacts>,
    join: Option<super::join::JoinFacts>,
    #[serde(alias = "snapshot_join")]
    replacement_join: Option<super::join::JoinTerm>,
    star_join: Option<super::join::StarJoinExpansion>,
    distinct: Option<DistinctFacts>,
}

#[derive(Deserialize)]
struct DistinctFacts {
    expanded: bool,
    input_projection: Option<Projection>,
}

#[derive(Default, Deserialize)]
struct TransferRoots {
    checkpoint: BTreeMap<u64, serde_json::Value>,
    // Deep live roots refer to additional definitions in checkpoint ID space.
    #[serde(default, rename = "expressionDefinitions")]
    expression_definitions: Vec<serde_json::Value>,
    // A live compiler sends roots in its checkpoint's identity space.
    #[serde(default)]
    observation: BTreeMap<u64, serde_json::Value>,
}

impl TransferRoots {
    fn attach(self, circuit: Circuit) -> Result<(Circuit, BTreeMap<u64, NodeId>), Error> {
        let checkpoint = circuit
            .operators()
            .any(|(_, node)| node.field("provenance").is_some());
        let functions = if checkpoint {
            self.checkpoint
        } else {
            self.observation
        };
        let (operators, mut definitions): (Vec<_>, Vec<_>) = functions.into_iter().unzip();
        if checkpoint {
            definitions.extend(self.expression_definitions);
        }
        let (circuit, roots) = circuit.with_inner_roots(definitions)?;
        Ok((circuit, operators.into_iter().zip(roots).collect()))
    }
}

#[derive(Deserialize)]
pub struct LimiterInputs {
    pub next_generated_name: u64,
    gen2: bool,
    singletons: Vec<u64>,
    #[serde(default, alias = "transfer_functions")]
    transfer_roots: TransferRoots,
    operators: BTreeMap<u64, OperatorFacts>,
}

/// A malformed checkpoint is an internal failure. SQL refusals identify the
/// live table or output column so the front end can report its source range.
#[derive(Debug, thiserror::Error)]
pub enum LimiterError {
    #[error(transparent)]
    InvalidCircuit(#[from] Error),
    #[error(
        "LATENESS on the primary-key table '{table}' is not yet supported by the Gen-2 engine; remove the LATENESS annotation or select a different runtime version"
    )]
    Gen2KeyLateness {
        operator: u64,
        table: String,
        next: u64,
    },
    #[error(
        "Could not infer a waterline for column '{view}'.'{column}' which has an 'emit_final' annotation"
    )]
    MissingWaterline {
        operator: u64,
        row: usize,
        view: String,
        column: String,
        next: u64,
    },
}

fn identifier<'a>(op: &'a Node, slot: &str) -> Option<&'a str> {
    let Field::Object(name) = op.field(slot)? else {
        return None;
    };
    let Field::Data(value) = name.get("name")? else {
        return None;
    };
    value.as_str()
}

/// The original operator and the circuit body where its waterlines must be inserted.
#[derive(Clone, Copy)]
pub(super) struct InsertionSite {
    pub body: NodeId,
    pub operator: NodeId,
}

/// The clone visitor finishes a nested body before rebuilding the enclosing operator.
/// Keep the following sibling so generated operators cannot cross a body boundary.
fn insertion_order(circuit: &Circuit) -> Result<Vec<(InsertionSite, Option<NodeId>)>, Error> {
    fn visit(
        circuit: &Circuit,
        body: NodeId,
        order: &mut Vec<(InsertionSite, Option<NodeId>)>,
    ) -> Result<(), Error> {
        let members = circuit
            .node(body)
            .expect("validated circuit body")
            .edges("allOperators")?;
        for (index, &operator) in members.iter().enumerate() {
            if circuit.node(operator).and_then(Node::operator_kind) == Some(Op::Nested) {
                visit(circuit, operator, order)?;
            }
            order.push((
                InsertionSite { body, operator },
                members.get(index + 1).copied(),
            ));
        }
        Ok(())
    }
    let mut order = Vec::new();
    visit(circuit, circuit.root_id(), &mut order)?;
    Ok(order)
}

pub(super) fn projected_bound(
    edit: &mut CircuitEditor,
    body: NodeId,
    input: Bound,
    src: &Projection,
    dst: &Projection,
    next: &mut u64,
) -> Result<Bound, Error> {
    let function = src.project_waterline(edit, input.ty, dst, None, next)?;
    apply_bound(edit, body, input, function)
}

/// Project inputs in port order before combining their waterlines. The whole
/// group belongs immediately before the original operator.
pub(super) fn projected_minimum<'a>(
    edit: &mut CircuitEditor,
    site: InsertionSite,
    inputs: impl IntoIterator<Item = (Bound, &'a Projection)>,
    dst: &Projection,
    next: &mut u64,
) -> Result<Bound, Error> {
    let InsertionSite {
        body,
        operator: owner,
    } = site;
    let projected = inputs
        .into_iter()
        .map(|(bound, src)| projected_bound(edit, body, bound, src, dst, next))
        .collect::<Result<Vec<_>, _>>()?;
    let first = projected
        .first()
        .ok_or_else(|| Error::new("a waterline minimum needs an input"))?;
    let bound = minimum_bound(edit, body, &projected, next)?;
    edit.place_before(body, first.port.operator, owner)?;
    Ok(bound)
}

pub(super) fn apply_bound_pair(
    edit: &mut CircuitEditor,
    body: NodeId,
    inputs: [Bound; 2],
    function: feldera_sql_ir::ExpressionId,
) -> Result<Bound, Error> {
    let result = edit.node(function.into())?.edge("body")?;
    let ty = edit.expression_type(edit.expression(result)?)?;
    let operator = edit.append_operator(
        body,
        inputs[0].port.operator,
        Op::Apply2,
        &inputs.map(|bound| bound.port),
        [
            ("outputType", Field::Reference(ty.into())),
            ("function", Field::Reference(function.into())),
        ],
    )?;
    edit.annotate(operator, serde_json::json!({"class":"Waterline"}))?;
    Ok(Bound {
        port: OutputPort {
            operator,
            number: 0,
        },
        ty,
    })
}

pub(super) fn apply_bound(
    edit: &mut CircuitEditor,
    body: NodeId,
    input: Bound,
    function: feldera_sql_ir::ExpressionId,
) -> Result<Bound, Error> {
    let result = edit.node(function.into())?.edge("body")?;
    let ty = edit.expression_type(edit.expression(result)?)?;
    let id = edit.append_operator(
        body,
        input.port.operator,
        Op::Apply,
        &[input.port],
        [
            ("outputType", Field::Reference(ty.into())),
            ("function", Field::Reference(function.into())),
        ],
    )?;
    edit.annotate(id, serde_json::json!({"class":"Waterline"}))?;
    Ok(Bound {
        port: OutputPort {
            operator: id,
            number: 0,
        },
        ty,
    })
}

/// The pass needs data clocks at tables, views and their output sinks. Infer them on the
/// original graph before editing the sources. Earlier sidecars also recorded
/// every input clock, which lets replay detect mismatched circuit/state pairs.
fn relation_clocks(
    circuit: &Circuit,
    facts: &BTreeMap<u64, OperatorFacts>,
    order: &[(InsertionSite, Option<NodeId>)],
) -> Result<BTreeMap<NodeId, StreamKind>, Error> {
    let mut clocks = circuit
        .root()
        .field("incremental")
        .map(|_| OutputClocks::new(circuit))
        .transpose()?;
    let mut recorded = BTreeMap::new();
    for &(site, _) in order {
        let id = site.operator;
        let node = circuit.node(id).expect("validated operator");
        let NodeId::Outer(number) = id else {
            unreachable!("validated circuit member")
        };
        let facts = facts
            .get(&number)
            .ok_or_else(|| Error::new(format!("missing limiter facts for {id:?}")))?;
        let Some(kinds) = &facts.input_kinds else {
            if clocks.is_none() {
                return Err(Error::new(
                    "limiter replay needs the circuit incremental flag or recorded input clocks",
                ));
            }
            continue;
        };
        if kinds.len() != node.inputs().count() {
            return Err(Error::new("limiter input stream kinds disagree with graph"));
        }
        for (port, &kind) in node.inputs().zip(kinds) {
            if let Some(clocks) = clocks.as_mut() {
                if clocks.get(port)? != kind {
                    return Err(Error::new(format!(
                        "recorded limiter clock differs at {port:?}"
                    )));
                }
            } else if recorded.insert(port, kind).is_some_and(|old| old != kind) {
                return Err(Error::new("conflicting stream kinds for the same output"));
            }
        }
    }
    let mut relations = BTreeMap::new();
    for &(site, _) in order {
        let id = site.operator;
        let node = circuit.node(id).expect("validated operator");
        if !matches!(
            node.operator_kind(),
            Some(Op::SourceMultiset | Op::View | Op::Sink)
        ) {
            continue;
        }
        let port = OutputPort {
            operator: id,
            number: 0,
        };
        let kind = if let Some(clocks) = clocks.as_mut() {
            clocks.get(port)?
        } else {
            // Legacy snapshots without a circuit mode carried clocks at the readers.
            let source = node.inputs().next().unwrap_or(port);
            recorded
                .get(&source)
                .or_else(|| recorded.get(&port))
                .copied()
                .unwrap_or(Clock::Collection)
        };
        relations.insert(id, StreamKind::try_from(kind)?);
    }
    Ok(relations)
}

pub fn insert_limiters(circuit: Circuit, state: LimiterInputs) -> Result<(Circuit, u64), Error> {
    insert_limiters_with_warnings(circuit, state, &mut Vec::new())
        .map_err(|error| Error::new(error.to_string()))
}

pub fn insert_limiters_with_warnings(
    circuit: Circuit,
    state: LimiterInputs,
    warnings: &mut Vec<feldera_sql_expr::SimplifyWarning>,
) -> Result<(Circuit, u64), LimiterError> {
    let original = insertion_order(&circuit)?;
    if original.len() != state.operators.len() {
        return Err(Error::new("limiter facts and circuit have different populations").into());
    }
    let clocks = relation_clocks(&circuit, &state.operators, &original)?;
    let (circuit, functions) = state.transfer_roots.attach(circuit)?;
    let mut edit = CircuitEditor::new(circuit);
    for number in state.singletons {
        let id = NodeId::Inner(number);
        if edit.circuit().node(id).is_some() {
            edit.register_singleton(id)?;
        }
    }
    let mut next = state.next_generated_name;
    let mut names = ProjectionNames::default();
    let mut errors = Vec::new();
    let mut data = BTreeMap::<OutputPort, Stream>::new();
    let mut bounds = BTreeMap::<OutputPort, (Bound, &Projection)>::new();
    // Stream joins and rolling aggregates publish bounds only for their expanded
    // output. Their original port cannot authorize trace retention downstream.
    let mut expanded_only = BTreeMap::new();
    for (site, following) in original {
        let InsertionSite { body, operator: id } = site;
        let expanded_bound = |p: &OutputPort| expanded_only.get(p).or_else(|| bounds.get(p));
        let NodeId::Outer(number) = id else {
            return Err(Error::new("expected outer operator").into());
        };
        let facts = state
            .operators
            .get(&number)
            .ok_or_else(|| Error::new(format!("missing limiter facts for {id:?}")))?;
        edit.remember_position(id, facts.position)?;
        let op = edit.node(id)?;
        let inputs = op.inputs().collect::<Vec<_>>();
        let output = OutputPort {
            operator: id,
            number: 0,
        };
        let projection = facts.projection.as_ref().filter(|p| p.monotone());
        let kind = op.operator_kind();
        match kind {
            Some(Op::SourceMultiset | Op::View)
                if identifier(op, "viewName") != Some("error_view") =>
            {
                let columns = Columns::read(op)?;
                if let Some(projection) = projection.filter(|_| !columns.declared.is_empty()) {
                    let name = identifier(op, "tableName")
                        .or_else(|| identifier(op, "viewName"))
                        .ok_or_else(|| Error::new("missing table name"))?
                        .to_owned();
                    if state.gen2 && !columns.key.is_empty() {
                        return Err(LimiterError::Gen2KeyLateness {
                            operator: number,
                            table: name,
                            next,
                        });
                    }
                    let kind = clocks[&id];
                    let inherited = inputs.first().and_then(expanded_bound).copied();
                    if let Some(input) = inputs.first().and_then(|p| data.get(p)) {
                        edit.replace_stream_inputs(id, &[input.port], input.ty)?;
                    }
                    let result = source_bounds(
                        &mut edit,
                        body,
                        LatenessSite {
                            operator: id,
                            name: &name,
                            columns: &columns,
                            kind,
                            inherited,
                        },
                        &mut next,
                    )?;
                    if !facts.reachable_from_error {
                        errors.push(result.error);
                    }
                    data.insert(output, result.data);
                    names.replaced(id);
                    bounds.insert(output, (result.bound, projection));
                    if let Some(before) = following {
                        edit.place_before(body, result.first_inserted, before)?;
                    }
                    continue;
                }
                if let Some(dst) = projection {
                    if let Some(&(input, _)) = inputs.first().and_then(expanded_bound) {
                        let param = edit.ref_parameter(input.ty, &mut next)?;
                        let read = edit.deref_parameter(&param)?;
                        let function = edit.closure(read, &[param])?;
                        let bound = apply_bound(&mut edit, body, input, function)?;
                        bounds.insert(output, (bound, dst));
                        edit.place_before(body, bound.port.operator, id)?;
                    }
                }
            }
            Some(Op::View) if identifier(op, "viewName") == Some("error_view") => {
                if inputs.len() != 1 {
                    return Err(Error::new("ERROR_VIEW needs one input").into());
                }
                connect_error_view(&mut edit, body, id, &errors, clocks[&id])?;
                names.replaced(id);
                continue;
            }
            Some(Op::Sum | Op::AtomicSum | Op::Subtract | Op::AntiJoin) => {
                if let Some(dst) = projection {
                    let upstream = inputs
                        .iter()
                        .map(|p| expanded_bound(p).copied())
                        .collect::<Option<Vec<_>>>();
                    if let Some(upstream) = upstream {
                        let bound = projected_minimum(&mut edit, site, upstream, dst, &mut next)?;
                        bounds.insert(output, (bound, dst));
                    } else if kind == Some(Op::AntiJoin) {
                        return Err(
                            Error::new("antijoin key projection needs both input bounds").into(),
                        );
                    }
                }
            }
            Some(Op::ChainAggregate)
                if op
                    .data("annotations")
                    .and_then(|x| x.as_array())
                    .is_some_and(|annotations| {
                        annotations.iter().any(|a| a["class"] == "AlwaysMonotone")
                    }) =>
            {
                if let Some(projection) = projection {
                    let (first, bound) =
                        super::chain::chain_waterline(&mut edit, body, id, &mut next)?;
                    bounds.insert(output, (bound, projection));
                    if let Some(before) = following {
                        edit.place_before(body, first, before)?;
                    }
                }
            }
            Some(
                Op::Map
                | Op::MapIndex
                | Op::Filter
                | Op::Hop
                | Op::FlatMap
                | Op::Noop
                | Op::Negate
                | Op::Deindex
                | Op::AggregateLinearPostprocess
                | Op::ChainAggregate
                | Op::Lag
                | Op::StreamAntiJoin,
            ) => {
                // These expansion operators transfer the bound of their first input,
                // including a stream antijoin whose right input only affects membership.
                let source = match (kind, inputs.as_slice()) {
                    (Some(Op::StreamAntiJoin), [left, _]) => left,
                    (Some(Op::StreamAntiJoin), _) => {
                        return Err(Error::new("stream antijoin requires two inputs").into());
                    }
                    (_, [input]) => input,
                    _ => {
                        return Err(Error::new(format!(
                            "{} requires one input stream",
                            op.class()
                        ))
                        .into());
                    }
                };
                let retains_keys = matches!(
                    kind,
                    Some(Op::AggregateLinearPostprocess | Op::ChainAggregate | Op::Lag)
                );
                let transfer = projection
                    .filter(|_| kind != Some(Op::Lag) || facts.input_projection.is_some());
                let bound = if retains_keys {
                    bounds.get(source)
                } else {
                    expanded_bound(source)
                };
                if let (Some(projection), Some(&(upstream, _))) = (transfer, bound) {
                    let function = functions.get(&number).copied().ok_or_else(|| {
                        Error::new(format!(
                            "missing reduced waterline closure for {}",
                            op.class()
                        ))
                    })?;
                    if let (Some(Op::Lag), Some(input_projection)) = (kind, &facts.input_projection)
                    {
                        let input = data.get(source).map_or(*source, |s| s.port);
                        super::retention::retain_keys(
                            &mut edit,
                            body,
                            id,
                            input,
                            input_projection,
                            upstream,
                            &mut next,
                        )?
                        .place_before_owner(&mut edit, body, id)?;
                    }
                    let function = super::apply_waterlines(
                        &mut edit,
                        function,
                        &[upstream.ty],
                        &mut next,
                        warnings,
                    )?;
                    let bound = apply_bound(&mut edit, body, upstream, function)?;
                    bounds.insert(output, (bound, projection));
                    edit.place_before(body, bound.port.operator, id)?;
                    if retains_keys {
                        let retention = super::retention::retain_keys(
                            &mut edit, body, id, output, projection, bound, &mut next,
                        )?;
                        if let Some(before) = following {
                            edit.place_before(body, retention.first, before)?;
                        }
                    }
                } else if projection.is_some() && !retains_keys {
                    return Err(
                        Error::new(format!("missing input waterline for {}", op.class())).into(),
                    );
                }
            }
            Some(
                Op::AsofJoin
                | Op::StreamJoin
                | Op::StreamJoinIndex
                | Op::Join
                | Op::JoinIndex
                | Op::JoinFilterMap
                | Op::LeftJoin
                | Op::LeftJoinIndex
                | Op::LeftJoinFilterMap,
            ) => {
                let ports: [OutputPort; 2] = inputs
                    .as_slice()
                    .try_into()
                    .map_err(|_| Error::new("join requires two inputs"))?;
                let join = super::join::JoinInputs {
                    site,
                    data: ports.map(|p| data.get(&p).map_or(p, |s| s.port)),
                    original_bounds: ports.map(|p| bounds.get(&p).map(|(b, _)| *b)),
                    expanded_bounds: ports.map(|p| expanded_bound(&p).map(|(b, _)| *b)),
                    functions: &functions,
                };
                if kind == Some(Op::AsofJoin) {
                    if let Some(term) = &facts.replacement_join {
                        if let Some(result) =
                            term.insert_asof(&mut edit, &join, &mut next, warnings)?
                        {
                            expanded_only.insert(output, result);
                        }
                    }
                } else if matches!(kind, Some(Op::StreamJoin | Op::StreamJoinIndex)) {
                    if let Some(term) = &facts.replacement_join {
                        if let Some(result) = term.insert(
                            &mut edit,
                            site,
                            &functions,
                            join.expanded_bounds,
                            &mut next,
                            warnings,
                        )? {
                            expanded_only.insert(output, result);
                        }
                    } else if projection.is_some() {
                        return Err(Error::new("missing bounded snapshot join expansion").into());
                    }
                } else {
                    let facts = facts
                        .join
                        .as_ref()
                        .ok_or_else(|| Error::new("missing incremental join expansion facts"))?;
                    if let Some(expansion) = &facts.expansion {
                        if let Some(result) =
                            expansion.insert(&mut edit, &join, projection, &mut next, warnings)?
                        {
                            bounds.insert(output, result);
                        }
                    }
                }
            }
            Some(Op::StarJoin | Op::StarJoinIndex | Op::StarJoinFilterMap) => {
                if let Some(star) = &facts.star_join {
                    if inputs.iter().any(|p| bounds.contains_key(p)) {
                        let incoming = inputs.iter().map(|p| expanded_bound(p).copied());
                        if let Some(result) =
                            star.insert(&mut edit, site, &functions, incoming, &mut next, warnings)?
                        {
                            if star.has_original_bound() {
                                bounds.insert(output, result);
                            } else {
                                expanded_only.insert(output, result);
                            }
                        }
                    }
                }
            }
            Some(Op::PartitionedRollingAggregate) => {
                let [source] = inputs.as_slice() else {
                    return Err(Error::new("rolling aggregate requires one input").into());
                };
                if let (Some(shape @ Projection::Tuple { fields, .. }), Some(&(bound, _))) =
                    (&facts.input_projection, expanded_bound(source))
                {
                    if fields.first().is_some_and(Projection::monotone) {
                        let input = super::rolling::RollingInput {
                            data: data.get(source).map_or(*source, |s| s.port),
                            bound,
                            projection: shape,
                            analyzed_type: edit.type_id(*functions.get(&number).ok_or_else(
                                || Error::new("missing analyzed rolling aggregate input type"),
                            )?)?,
                        };
                        if let Some(dst) = super::rolling::insert(
                            &mut edit,
                            site,
                            input,
                            projection.is_some(),
                            &mut next,
                            warnings,
                        )?
                        .zip(projection)
                        {
                            expanded_only.insert(output, dst);
                        }
                        names.replaced(id);
                        continue;
                    }
                }
            }
            Some(Op::Aggregate) => {
                let [source] = inputs.as_slice() else {
                    return Err(Error::new("aggregate requires one input").into());
                };
                let expansion = facts
                    .aggregate
                    .as_ref()
                    .ok_or_else(|| Error::new("missing aggregate expansion facts"))?;
                if let Some(&(waterline, _)) = bounds.get(source) {
                    let input = super::aggregate::AggregateInput {
                        site: id,
                        data: data.get(source).map_or(*source, |s| s.port),
                        waterline,
                        following,
                        body,
                        functions: &functions,
                    };
                    if let Some(bound) = input.insert(&mut edit, expansion, &mut next, warnings)? {
                        let projection = projection
                            .ok_or_else(|| Error::new("missing aggregate upsert projection"))?;
                        bounds.insert(output, (bound, projection));
                    }
                }
            }
            Some(Op::Distinct | Op::Positive) => {
                let [source] = inputs.as_slice() else {
                    return Err(Error::new("distinct retention requires one input").into());
                };
                let distinct = facts
                    .distinct
                    .as_ref()
                    .ok_or_else(|| Error::new("missing distinct expansion facts"))?;
                if let Some(&(bound, _)) = bounds.get(source).filter(|_| distinct.expanded) {
                    let projected = distinct.input_projection.as_ref().ok_or_else(|| {
                        Error::new("missing projection for bounded distinct input")
                    })?;
                    let port = data.get(source).map_or(*source, |stream| stream.port);
                    let retention = super::retention::retain_keys(
                        &mut edit, body, id, port, projected, bound, &mut next,
                    )?;
                    retention.place_before_owner(&mut edit, body, id)?;
                    bounds.insert(output, (bound, projected));
                }
            }
            Some(Op::Window) => {
                if let Some(projection) = projection {
                    let input = inputs
                        .first()
                        .and_then(expanded_bound)
                        .map(|(bound, _)| *bound);
                    let bound = super::window::insert_window_bound(
                        &mut edit,
                        site,
                        projection,
                        input,
                        |port| data.get(&port).map_or(port, |stream| stream.port),
                        &mut next,
                        warnings,
                    )?;
                    bounds.insert(output, (bound, projection));
                }
            }
            Some(Op::Sink) => {
                if let Some(field) = super::FinalityColumn::from_sink(op, projection, number, next)?
                {
                    let input = op
                        .inputs()
                        .next()
                        .ok_or_else(|| Error::new("EMIT FINAL sink needs a stream"))?;
                    let (bound, _) = expanded_bound(&input).copied().ok_or_else(|| {
                        Error::new("missing upstream bound for monotone EMIT FINAL column")
                    })?;
                    let stream = match data.get(&input) {
                        Some(stream) => *stream,
                        None => Stream {
                            port: input,
                            ty: crate::rebuild::input_type(&edit, input)?,
                            kind: clocks[&id],
                        },
                    };
                    super::finality::emit_final(
                        &mut edit, body, id, stream, bound, field, &mut next,
                    )?;
                    names.replaced(id);
                    continue;
                }
            }
            Some(Op::Nested) => {
                let Some(Field::Array(slots)) = op.field("internalOutputs") else {
                    return Err(Error::new("nested circuit requires output slots").into());
                };
                let mapped = slots
                    .iter()
                    .map(|slot| {
                        OutputPort::from_field(slot).map(|p| data.get(&p).map_or(p, |s| s.port))
                    })
                    .collect::<Vec<_>>();
                edit.remap_nested_outputs(id, &mapped)?;
            }
            // The Java visitor clones these operators without deriving output bounds.
            Some(
                Op::Delta
                | Op::ViewDeclaration
                | Op::Now
                | Op::Constant
                | Op::IncrementalConstant
                | Op::StreamAggregate
                | Op::StreamDistinct
                | Op::Waterline
                | Op::Apply
                | Op::Weigh
                | Op::Differentiate
                | Op::Integrate
                | Op::IndexedTopK
                | Op::Rank
                | Op::RowNumber,
            ) => {}
            _ => {
                return Err(Error::new(format!(
                    "{} requires another InsertLimiters visitor",
                    op.class()
                ))
                .into());
            }
        }
        // withInputs constructs its projection after the operator's bound closure.
        names.visit(id, edit.node(id)?, &mut next)?;
        if inputs.iter().any(|port| data.contains_key(port)) {
            let ports = inputs
                .iter()
                .map(|p| data.get(p).map_or(*p, |s| s.port))
                .collect::<Vec<_>>();
            if kind.is_some_and(Op::supports_stream_input_replacement) {
                let output_type = match data.get(&inputs[0]) {
                    Some(input) => input.ty,
                    None => edit.type_id(edit.node(id)?.edge("outputType")?)?,
                };
                edit.replace_stream_inputs(id, &ports, output_type)?;
            } else {
                edit.rewire_inputs(id, &ports)?;
            }
        }
    }
    let mut rewrite = edit.finish_with_rewrites()?;
    // Newly inserted deindexes already own the closure allocated by their constructor.
    rewrite
        .rebuilt_inputs
        .retain(|id| matches!(id, NodeId::Outer(number) if state.operators.contains_key(number)));
    Ok((names.finish(rewrite)?, next))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn recursive_insertions_use_the_next_sibling_in_the_same_body() {
        let port = |id| json!({"operator": {"node": id}, "outputNumber": 0});
        let circuit = Circuit::from_json(json!({
            "class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": [
                {"class": "DBSPConstantOperator", "id": 1, "inputs": []},
                {"class": "DBSPNestedOperator", "id": 2, "inputs": [port(1)],
                 "internalOutputs": [port(4)], "allOperators": [
                    {"class": "DBSPViewDeclarationOperator", "id": 3, "inputs": []},
                    {"class": "DBSPNoopOperator", "id": 4, "inputs": [port(3)]}
                 ]},
                {"class": "DBSPNoopOperator", "id": 5, "inputs": [port(2)]}
            ]
        }))
        .unwrap();
        let actual: Vec<_> = insertion_order(&circuit)
            .unwrap()
            .into_iter()
            .map(|(site, next)| (site.body, site.operator, next))
            .collect();
        let outer = NodeId::Outer;
        assert_eq!(
            actual,
            [
                (outer(0), outer(1), Some(outer(2))),
                (outer(2), outer(3), Some(outer(4))),
                (outer(2), outer(4), None),
                (outer(0), outer(2), Some(outer(5))),
                (outer(0), outer(5), None),
            ]
        );
    }

    #[test]
    fn unused_view_keeps_the_clock_of_its_input() {
        let circuit = Circuit::from_json(json!({
            "class": "DBSPCircuit", "id": 0, "incremental": true, "declarations": [],
            "allOperators": [
                {"class": "DBSPIncrementalConstantOperator", "id": 1, "inputs": []},
                {"class": "DBSPIntegrateOperator", "id": 2,
                 "inputs": [{"operator": {"node": 1}, "outputNumber": 0}]},
                {"class": "DBSPViewOperator", "id": 3,
                 "inputs": [{"operator": {"node": 2}, "outputNumber": 0}]}
            ]
        }))
        .unwrap();
        let mut facts: BTreeMap<_, _> = (1..=3)
            .map(|id| {
                (
                    id,
                    OperatorFacts {
                        position: SourceRange::default(),
                        reachable_from_error: false,
                        input_kinds: None,
                        projection: None,
                        distinct: None,
                        aggregate: None,
                        join: None,
                        replacement_join: None,
                        star_join: None,
                        input_projection: None,
                    },
                )
            })
            .collect();
        let order = insertion_order(&circuit).unwrap();
        let clocks = relation_clocks(&circuit, &facts, &order).unwrap();
        assert_eq!(clocks[&NodeId::Outer(3)], StreamKind::Collection);

        facts.get_mut(&3).unwrap().input_kinds = Some(vec![Clock::Delta]);
        assert!(
            relation_clocks(&circuit, &facts, &order)
                .unwrap_err()
                .to_string()
                .contains("recorded limiter clock differs")
        );
    }
}
