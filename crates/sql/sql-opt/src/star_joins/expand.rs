//! Expand a multiway join into a balanced tree of binary indexed joins.
use super::output;
use crate::{
    rebuild::{input_type, ProjectionNames},
    stream_rewrites::body_members,
};
use feldera_sql_expr::SimplifyWarning;
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, Field, NodeId, OperatorKind as Op, OutputPort,
    TypeId, TypeKind as Ty,
};
use std::collections::{BTreeMap, HashMap};

struct Expansion<'a> {
    edit: &'a mut CircuitEditor,
    body: NodeId,
    site: NodeId,
    next: &'a mut u64,
    warnings: &'a mut Vec<SimplifyWarning>,
    first: Option<NodeId>,
}

impl Expansion<'_> {
    fn append(
        &mut self,
        kind: Op,
        inputs: &[OutputPort],
        result: TypeId,
        function: NodeId,
    ) -> Result<OutputPort, Error> {
        let mut fields = vec![
            ("outputType", Field::Reference(result.into())),
            ("function", Field::Reference(function)),
        ];
        if kind == Op::JoinIndex {
            fields.push(("balanced", Field::Data(false.into())));
        }
        let op = self
            .edit
            .append_operator(self.body, self.site, kind, inputs, fields)?;
        self.first.get_or_insert(op);
        Ok(output(op))
    }

    fn element(&self, port: OutputPort) -> Result<TypeId, Error> {
        let collection = input_type(self.edit, port)?;
        self.edit
            .type_id(self.edit.node(collection.into())?.edge("elementType")?)
    }

    fn map(&mut self, kind: Op, function: NodeId, input: OutputPort) -> Result<OutputPort, Error> {
        let body = self.edit.node(function)?.edge("body")?;
        let row = self.edit.expression_type(self.edit.expression(body)?)?;
        let output = if kind == Op::MapIndex {
            self.edit.indexed_zset_from_pair(row)?
        } else {
            self.edit.zset_type(row)?
        };
        self.append(kind, &[input], output, function)
    }

    fn split_filter(
        &mut self,
        inputs: &[OutputPort],
        function: NodeId,
    ) -> Result<OutputPort, Error> {
        let node = self.edit.node(self.site)?;
        let predicate = node.edge("filter")?;
        let map = node.field("map").and_then(Field::node_id);
        let output_type = self.edit.type_id(node.edge("outputType")?)?;
        let body = self.edit.node(function)?.edge("body")?;
        let row = self.edit.expression_type(self.edit.expression(body)?)?;
        let (kind, collection) = if self.edit.type_kind(row)? == Ty::RawTuple {
            (Op::StarJoinIndex, self.edit.indexed_zset_from_pair(row)?)
        } else {
            (Op::StarJoin, self.edit.zset_type(row)?)
        };
        let join = self.append(kind, inputs, collection, function)?;
        let filter = self.append(Op::Filter, &[join], collection, predicate)?;
        let Some(map) = map else { return Ok(filter) };
        let kind = if self.edit.type_kind(output_type)? == Ty::IndexedZSet {
            Op::MapIndex
        } else {
            Op::Map
        };
        self.map(kind, map, filter)
    }

    fn expand(
        &mut self,
        kind: Op,
        mapped: &BTreeMap<NodeId, OutputPort>,
    ) -> Result<OutputPort, Error> {
        let node = self.edit.node(self.site)?;
        let function = node.edge("function")?;
        let inputs = node
            .inputs()
            .map(|p| mapped.get(&p.operator).copied().unwrap_or(p))
            .collect::<Vec<_>>();
        let result = if kind == Op::StarJoinFilterMap {
            self.split_filter(&inputs, function)?
        } else {
            let first = node
                .inputs()
                .next()
                .ok_or_else(|| Error::new("star join requires inputs"))?;
            // The grouping key comes from the original join, while value rows come from its mapped inputs.
            let source = input_type(self.edit, first)?;
            let key = self
                .edit
                .type_id(self.edit.node(source.into())?.edge("keyType")?)?;
            let rows = inputs
                .iter()
                .map(|&port| self.element(port))
                .collect::<Result<Vec<_>, _>>()?;
            let plan =
                super::expand_star_term(self.edit, function, key, &rows, self.next, self.warnings)?;
            let mut streams = inputs;
            for join in plan.joins {
                let pair = join.inputs.map(|index| streams[index]);
                streams.push(self.append(
                    Op::JoinIndex,
                    &pair,
                    join.output_type,
                    join.function.into(),
                )?);
            }
            let joined = *streams.last().expect("nonempty star join");
            let map = if kind == Op::StarJoinIndex {
                Op::MapIndex
            } else {
                Op::Map
            };
            self.map(map, plan.projection.into(), joined)?
        };
        self.edit.place_before(
            self.body,
            self.first.expect("expanded star join"),
            self.site,
        )?;
        Ok(result)
    }
}

/// One original visitor iteration. Filtered joins become a star join followed by
/// a filter and optional map. An ordinary star join becomes only binary joins.
pub fn expand_star_join_layer(
    circuit: Circuit,
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<Circuit, Error> {
    expand_layer(circuit, next, warnings).map(|(circuit, _)| circuit)
}

// Splitting a filtered join introduces an ordinary star join for the next iteration.
fn expand_layer(
    circuit: Circuit,
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<(Circuit, bool), Error> {
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let star_kind = |id| {
        circuit.node(id)?.operator_kind().filter(|kind| {
            matches!(
                kind,
                Op::StarJoin | Op::StarJoinIndex | Op::StarJoinFilterMap
            )
        })
    };
    let sites: HashMap<_, _> = order
        .iter()
        .filter_map(|&id| Some((id, star_kind(id)?)))
        .collect();
    if sites.is_empty() {
        return Ok((circuit, false));
    }
    let split_filter = sites.values().any(|kind| *kind == Op::StarJoinFilterMap);
    let bodies: HashMap<_, _> = body_members(&circuit)?
        .into_iter()
        .map(|(body, id)| (id, body))
        .collect();
    let mut edit = CircuitEditor::new(circuit);
    let mut replacements = BTreeMap::new();
    let mut projections = ProjectionNames::default();
    let mut names = *next;
    for id in order {
        let node = edit.node(id)?;
        projections.visit(id, node, &mut names)?;
        if let Some(&kind) = sites.get(&id) {
            let mut expansion = Expansion {
                edit: &mut edit,
                body: bodies[&id],
                site: id,
                next: &mut names,
                warnings,
                first: None,
            };
            let replacement = expansion.expand(kind, &replacements)?;
            replacements.insert(id, replacement);
            projections.replaced(id);
        }
    }
    let result = projections.finish(edit.finish_forwarding(&replacements)?)?;
    *next = names;
    Ok((result, split_filter))
}

pub fn remove_star_joins(
    circuit: Circuit,
    next: &mut u64,
) -> Result<(Circuit, Vec<SimplifyWarning>), Error> {
    let mut warnings = Vec::new();
    let mut names = *next;
    let (mut result, split_filter) = expand_layer(circuit, &mut names, &mut warnings)?;
    if split_filter {
        result = expand_star_join_layer(result, &mut names, &mut warnings)?;
    }
    *next = names;
    Ok((result, warnings))
}
