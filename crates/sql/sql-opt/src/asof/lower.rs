//! Replace each logical ASOF join with two ordered indexes and a concrete join.
use super::{asof_index, asof_projection, asof_timestamps};
use crate::{
    asof_retention::asof_retainers,
    field_comparator::{empty_comparator, field_comparator},
    rebuild::{input_type, ProjectionNames},
    stream_rewrites::body_members,
};
use feldera_sql_expr::SimplifyWarning;
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, Field, NodeId, OperatorKind as Op,
    OutputPort, TypeId,
};
use std::collections::{BTreeMap, HashMap, HashSet};

pub struct AsofRetention {
    pub retainer: NodeId,
    pub join: NodeId,
    /// The Java clone visitor gives the retainer a new identity when its inputs change.
    pub rebuilt: bool,
}

pub struct LoweredAsof {
    pub circuit: Circuit,
    pub warnings: Vec<SimplifyWarning>,
    /// Original retainer and new concrete join, in retainer traversal order.
    pub retention: Vec<AsofRetention>,
}

struct JoinInput<'a> {
    port: OutputPort,
    row: TypeId,
    pair: ExpressionId,
    timestamp: usize,
    printed_row: &'a str,
}

impl<'a> JoinInput<'a> {
    fn new(
        edit: &mut CircuitEditor,
        port: OutputPort,
        timestamp: usize,
        printed_row: &'a str,
        next: &mut u64,
    ) -> Result<Self, Error> {
        let stream = input_type(edit, port)?;
        let source = edit.node(stream.into())?;
        let key = edit.type_id(source.edge("keyType")?)?;
        let row = edit.type_id(source.edge("elementType")?)?;
        let k = edit.reference_type(key)?;
        let v = edit.reference_type(row)?;
        let pair = edit.tuple_type(stream.into(), true, [k.into(), v.into()])?;
        let pair = edit.variable(pair, next)?;
        Ok(Self {
            port,
            row,
            pair,
            timestamp,
            printed_row,
        })
    }

    fn index(
        &self,
        edit: &mut CircuitEditor,
        body: NodeId,
        site: NodeId,
        key: TypeId,
        flatten_key: bool,
    ) -> Result<(OutputPort, TypeId), Error> {
        let empty = empty_comparator(edit, site, self.row, self.printed_row)?;
        let order = field_comparator(edit, site, empty, self.timestamp, true, true)?;
        let field_type = edit.expression_type(order)?;
        let function = asof_index(edit, self.pair, order, flatten_key)?;
        let result = edit.node(function.into())?.edge("body")?;
        let pair = edit.expression_type(edit.expression(result)?)?;
        let value = edit.node(pair.into())?.edges("tupFields")?[1];
        let stream = edit.indexed_zset_type(value, key, edit.type_id(value)?)?;
        let operator = edit.append_operator(
            body,
            site,
            Op::MapIndex,
            &[self.port],
            [
                ("outputType", Field::Reference(stream.into())),
                ("function", Field::Reference(function.into())),
            ],
        )?;
        let wrapper = edit.custom_order_type(site, self.row, field_type)?;
        Ok((
            OutputPort {
                operator,
                number: 0,
            },
            wrapper,
        ))
    }
}

fn lower_join(
    edit: &mut CircuitEditor,
    body: NodeId,
    id: NodeId,
    rows: &[String; 2],
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<(NodeId, OutputPort), Error> {
    let join = edit.node(id)?;
    let [left, right] = join
        .inputs()
        .collect::<Vec<_>>()
        .try_into()
        .map_err(|_| Error::new("ASOF join requires two inputs"))?;
    let column = |field| {
        join.data(field)
            .and_then(|v| v.as_u64())
            .and_then(|i| usize::try_from(i).ok())
            .ok_or_else(|| Error::new(format!("invalid ASOF {field}")))
    };
    let indices = [
        column("leftTimestampIndex")?,
        column("rightTimestampIndex")?,
    ];
    let output = join.edge("outputType")?;
    let comparator = join.edge("comparator")?;
    let is_left = join
        .data("isLeft")
        .and_then(|v| v.as_bool())
        .ok_or_else(|| Error::new("missing ASOF join mode"))?;
    let function = join.edge("function")?;
    let key = edit.type_id(edit.node(input_type(edit, left)?.into())?.edge("keyType")?)?;
    let left = JoinInput::new(edit, left, indices[0], &rows[0], next)?;
    let right = JoinInput::new(edit, right, indices[1], &rows[1], next)?;
    let (left, left_type) = left.index(edit, body, id, key, true)?;
    let (right, right_type) = right.index(edit, body, id, key, false)?;
    let [left_ts, right_ts] = asof_timestamps(edit, [left_type, right_type], indices, next)?;
    let projection = asof_projection(edit, function, key, left_type, right_type, next, warnings)?;
    let concrete = edit.append_operator(
        body,
        id,
        Op::ConcreteAsofJoin,
        &[left, right],
        [
            ("outputType", Field::Reference(output)),
            ("function", Field::Reference(projection.into())),
            ("leftTimestamp", Field::Reference(left_ts.into())),
            ("rightTimestamp", Field::Reference(right_ts.into())),
            ("comparator", Field::Reference(comparator)),
            ("balanced", Field::Data(false.into())),
            ("isLeft", Field::Data(is_left.into())),
        ],
    )?;
    edit.copy_annotations(id, concrete)?;
    Ok((
        left.operator,
        OutputPort {
            operator: concrete,
            number: 0,
        },
    ))
}

/// The Java type printer supplies the row text used in persistent comparator names.
pub fn lower_asof(
    circuit: Circuit,
    mut rows: BTreeMap<u64, [String; 2]>,
    next: &mut u64,
) -> Result<LoweredAsof, Error> {
    let bodies: HashMap<_, _> = body_members(&circuit)?
        .into_iter()
        .map(|(body, id)| (id, body))
        .collect();
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let mut retained_by = HashMap::new();
    for (join, retainer) in asof_retainers(&circuit)? {
        if retained_by
            .insert(NodeId::Outer(retainer), NodeId::Outer(join))
            .is_some()
        {
            return Err(Error::new("ASOF retainer belongs to multiple joins"));
        }
    }
    let mut edit = CircuitEditor::new(circuit);
    let mut placements = Vec::new();
    let mut replacements = BTreeMap::new();
    let mut projections = ProjectionNames::default();
    let mut names = *next;
    let mut warnings = Vec::new();
    for &id in &order {
        let node = edit.node(id)?;
        projections.visit(id, node, &mut names)?;
        if node.operator_kind() != Some(Op::AsofJoin) {
            continue;
        }
        let NodeId::Outer(number) = id else {
            unreachable!("circuit members have outer IDs")
        };
        let rendered = rows
            .remove(&number)
            .ok_or_else(|| Error::new("missing ASOF input type renderings"))?;
        let body = bodies[&id];
        let (first, replacement) =
            lower_join(&mut edit, body, id, &rendered, &mut names, &mut warnings)?;
        placements.push((body, first, id));
        replacements.insert(id, replacement);
        projections.replaced(id);
    }
    if !rows.is_empty() {
        return Err(Error::new("ASOF type renderings reference an absent join"));
    }
    for (body, first, original) in placements.into_iter().rev() {
        edit.place_before(body, first, original)?;
    }
    let rewrite = edit.finish_forwarding(&replacements)?;
    let rebuilt: HashSet<_> = rewrite.rebuilt_inputs.iter().copied().collect();
    let retention = order
        .into_iter()
        .filter_map(|retainer| {
            Some(AsofRetention {
                retainer,
                join: replacements[retained_by.get(&retainer)?].operator,
                rebuilt: rebuilt.contains(&retainer),
            })
        })
        .collect();
    let circuit = projections.finish(rewrite)?;
    *next = names;
    Ok(LoweredAsof {
        circuit,
        warnings,
        retention,
    })
}
