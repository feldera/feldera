//! Fuse output projections into constants, UNNEST and single-reader joins.
use crate::{
    projection::{projection, simple_tuple, Projection},
    projection_composition::{inline_projection, CompositionError},
    rebuild::ProjectionNames,
    stream_rewrites::{body_members, reader_counts},
};
use feldera_sql_expr::constant_rows::{self as rows, multiset};
use feldera_sql_expr::{
    compose_closures, project_flatmap, reduce_closure_body_with_warnings, SimplifyWarning,
};
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, NodeId,
    OperatorKind as Op, OutputPort, TypeId, TypeKind,
};
use std::collections::{BTreeMap, HashMap};

/// DetectShuffle accepts only a tuple of fields of the closure parameter,
/// allowing dereference and clone on either side of a field access.
fn shuffle(
    edit: &CircuitEditor,
    body: NodeId,
    parameter: NodeId,
) -> Result<Option<Vec<usize>>, Error> {
    let transparent = |mut id| -> Result<NodeId, Error> {
        while matches!(
            edit.expr_kind(edit.expression(id)?)?,
            Expr::Deref | Expr::Clone
        ) {
            id = edit.node(id)?.edge("expression")?;
        }
        Ok(id)
    };
    let parameter_node = edit.node(parameter)?;
    let reference = edit.node(parameter_node.edge("type")?)?;
    let Some(tuple) = reference.field("type").and_then(Field::node_id) else {
        return Ok(None);
    };
    if !matches!(
        edit.type_kind(edit.type_id(tuple)?)?,
        TypeKind::RawTuple | TypeKind::Tuple
    ) {
        return Ok(None);
    }
    let body = transparent(body)?;
    if !matches!(
        edit.expr_kind(edit.expression(body)?)?,
        Expr::Tuple | Expr::RawTuple
    ) {
        return Ok(None);
    }
    let mut result = Vec::new();
    if edit.node(body)?.field("fields").is_some() {
        for field in edit.node(body)?.edges("fields")? {
            let field = transparent(field)?;
            if !matches!(
                edit.expr_kind(edit.expression(field)?)?,
                Expr::Field | Expr::CustomOrdField
            ) {
                return Ok(None);
            }
            let access = edit.node(field)?;
            let var = edit.node(transparent(access.edge("expression")?)?)?;
            if var.data("variable") != parameter_node.data("name") {
                return Ok(None);
            }
            result.push(
                access
                    .data("fieldNo")
                    .and_then(|n| n.as_u64())
                    .and_then(|n| usize::try_from(n).ok())
                    .ok_or_else(|| Error::new("invalid projection field"))?,
            );
        }
    }
    Ok(Some(result))
}

/// Apply a pure field shuffle to UNNEST's current output layout. Repeated
/// indexes duplicate output columns and a computed column prevents fusion.
pub fn project_unnest(
    edit: &mut CircuitEditor,
    flatmap: NodeId,
    reader: NodeId,
) -> Result<Option<ExpressionId>, Error> {
    let closure = edit.node(reader)?;
    let parameters = closure.edges("parameters")?;
    let [parameter] = parameters.as_slice() else {
        return Ok(None);
    };
    let Some(fields) = shuffle(edit, closure.edge("body")?, *parameter)? else {
        return Ok(None);
    };
    let function = project_flatmap(edit, flatmap, &fields)?;
    Ok(Some(edit.expression(function)?))
}

fn project_constant(
    edit: &mut CircuitEditor,
    constant: ExpressionId,
    function: NodeId,
    indexed: bool,
    output_type: NodeId,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<Option<ExpressionId>, Error> {
    let kind = edit.expr_kind(constant)?;
    if indexed {
        if kind == Expr::IndexedZSet
            || (kind == Expr::ZSet && edit.node(constant.into())?.edges("data")?.is_empty())
        {
            return edit
                .make_expr(
                    constant.into(),
                    Expr::IndexedZSet,
                    edit.type_id(output_type)?,
                    [],
                )
                .map(Some);
        }
        return Ok(None);
    }
    if kind != Expr::ZSet {
        return Ok(None);
    }
    let closure = edit.node(function)?;
    let params = closure.edges("parameters")?;
    let body = closure.edge("body")?;
    let element = edit.expression_type(edit.expression(body)?)?;
    let mut projected = Vec::new();
    for (row, weight) in rows::entries(edit, constant)? {
        let arg = edit.borrow_value(row)?;
        let row =
            reduce_closure_body_with_warnings(edit, body, &params, &[arg.into()], Some(warnings))?;
        rows::put(edit, &mut projected, (edit.expression(row)?, weight))?;
    }
    multiset(edit, element, projected).map(Some)
}

fn projected_join(source: Op, indexed: bool) -> Option<(Op, bool)> {
    let (plain, index, was_indexed) = match source {
        Op::Join | Op::JoinIndex => (Op::Join, Op::JoinIndex, source == Op::JoinIndex),
        Op::LeftJoin | Op::LeftJoinIndex => {
            (Op::LeftJoin, Op::LeftJoinIndex, source == Op::LeftJoinIndex)
        }
        Op::StreamJoin | Op::StreamJoinIndex => (
            Op::StreamJoin,
            Op::StreamJoinIndex,
            source == Op::StreamJoinIndex,
        ),
        Op::StarJoin | Op::StarJoinIndex => {
            (Op::StarJoin, Op::StarJoinIndex, source == Op::StarJoinIndex)
        }
        Op::AsofJoin if !indexed => return Some((source, false)),
        _ => return None,
    };
    Some((if indexed { index } else { plain }, was_indexed))
}

/// The reader layout and the original fanout determine whether a projection
/// can replace its producer. Both the live visitor and detached replay run this rule.
pub struct OutputProjection {
    pub producer: Op,
    pub indexed: bool,
    pub single_reader: bool,
    pub output_type: TypeId,
}

/// The fused operator keeps the reader's output schema and the producer's inputs.
pub struct ProjectedOutput {
    kind: Op,
    function: ExpressionId,
    output: TypeId,
}

impl ProjectedOutput {
    pub fn append(
        &self,
        edit: &mut CircuitEditor,
        body: NodeId,
        site: NodeId,
        inputs: &[OutputPort],
        properties: impl IntoIterator<Item = (&'static str, Field)>,
    ) -> Result<NodeId, Error> {
        let fields = [
            ("outputType", Field::Reference(self.output.into())),
            ("function", Field::Reference(self.function.into())),
        ];
        edit.append_operator(
            body,
            site,
            self.kind,
            inputs,
            fields.into_iter().chain(properties),
        )
    }
}

impl OutputProjection {
    /// Fuse a Map or MapIndex reader into a unary operator selected by OptimizeProjections.
    pub fn compose_unary(
        &self,
        edit: &mut CircuitEditor,
        before: ExpressionId,
        reader: NodeId,
        only_projections: bool,
        next: &mut u64,
        warnings: &mut Vec<SimplifyWarning>,
    ) -> Result<Option<ProjectedOutput>, CompositionError> {
        let (kind, function) = match self.producer {
            Op::Map => (
                if self.indexed { Op::MapIndex } else { Op::Map },
                crate::compose_projection(edit, before.into(), reader, true, warnings)?,
            ),
            Op::MapIndex if self.indexed => (
                Op::MapIndex,
                crate::compose_indexed_projection(
                    edit,
                    before.into(),
                    reader,
                    only_projections,
                    next,
                    warnings,
                )?,
            ),
            Op::FlatMap if !self.indexed => {
                (Op::FlatMap, project_unnest(edit, before.into(), reader)?)
            }
            Op::Deindex if !self.indexed => (
                Op::Map,
                Some(inline_projection(edit, before.into(), reader, warnings)?),
            ),
            _ => return Err(Error::new("unsupported unary projection").into()),
        };
        let Some(function) = function else {
            return Ok(None);
        };
        let output = if self.producer == Op::Deindex {
            let body = edit.expression(edit.node(function.into())?.edge("body")?)?;
            edit.zset_type(edit.expression_type(body)?)?
        } else {
            self.output_type
        };
        Ok(Some(ProjectedOutput {
            kind,
            function,
            output,
        }))
    }

    fn accepts_producer(&self) -> bool {
        match self.producer {
            Op::Constant => !self.indexed || self.single_reader,
            Op::FlatMap => !self.indexed,
            kind => self.single_reader && projected_join(kind, self.indexed).is_some(),
        }
    }

    /// The optimizer has already accepted the reader and checked the producer's fanout.
    pub fn compose_join(
        &self,
        edit: &mut CircuitEditor,
        before: ExpressionId,
        reader: NodeId,
        warnings: &mut Vec<SimplifyWarning>,
    ) -> Result<ProjectedOutput, CompositionError> {
        let (kind, was_indexed) = projected_join(self.producer, self.indexed)
            .ok_or_else(|| Error::new("unsupported join output projection"))?;
        let function = if was_indexed {
            compose_closures(edit, before, true, reader, Some(warnings))?
        } else {
            inline_projection(edit, before.into(), reader, warnings)?
        };
        Ok(ProjectedOutput {
            kind,
            function,
            output: self.output_type,
        })
    }

    pub fn apply(
        &self,
        edit: &mut CircuitEditor,
        before: ExpressionId,
        reader: NodeId,
        warnings: &mut Vec<SimplifyWarning>,
    ) -> Result<Option<ProjectedOutput>, CompositionError> {
        if !self.accepts_producer() {
            return Ok(None);
        }
        let closure = edit.node(reader)?;
        if closure.class() != "DBSPClosureExpression" {
            return Ok(None);
        }
        let parameters = closure.edges("parameters")?;
        let [parameter] = parameters.as_slice() else {
            return Ok(None);
        };
        let body = closure.edge("body")?;
        if self.indexed && !simple_tuple(edit, edit.node(body)?.edge("type")?)? {
            return Ok(None);
        }
        let shape = projection(edit, body, *parameter)?;
        let join = projected_join(self.producer, self.indexed);
        if matches!(shape, Projection::Other)
            || ((self.indexed || join.is_some()) && !matches!(shape, Projection::Fields(_)))
        {
            return Ok(None);
        }
        let new = match self.producer {
            Op::Constant => project_constant(
                edit,
                before,
                reader,
                self.indexed,
                self.output_type.into(),
                warnings,
            )?,
            Op::FlatMap if edit.expr_kind(before)? == Expr::Flatmap => {
                project_unnest(edit, before.into(), reader)?
            }
            _ if join.is_some() => {
                return self.compose_join(edit, before, reader, warnings).map(Some);
            }
            _ => return Ok(None),
        };
        let Some(function) = new else { return Ok(None) };
        let output = if self.producer == Op::Constant {
            edit.expression_type(function)?
        } else {
            self.output_type
        };
        Ok(Some(ProjectedOutput {
            kind: self.producer,
            function,
            output,
        }))
    }
}

pub fn project_outputs(
    circuit: Circuit,
    next: &mut u64,
) -> Result<(Circuit, Vec<SimplifyWarning>), CompositionError> {
    let members = body_members(&circuit)?;
    let readers = reader_counts(&circuit, &members);
    let bodies: HashMap<_, _> = members.into_iter().map(|(body, id)| (id, body)).collect();
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let original = circuit.clone();
    let mut provenance = None;
    let mut edit = CircuitEditor::new(circuit);
    let mut replacements = BTreeMap::new();
    let mut projections = ProjectionNames::default();
    let mut names = *next;
    let mut warnings = Vec::new();
    for id in order {
        let node = edit.node(id)?;
        projections.visit(id, node, &mut names)?;
        if !matches!(node.operator_kind(), Some(Op::Map | Op::MapIndex)) {
            continue;
        }
        let indexed = node.operator_kind() == Some(Op::MapIndex);
        let function = node.edge("function")?;
        let output_type = node.edge("outputType")?;
        let incoming = node
            .inputs()
            .next()
            .ok_or_else(|| Error::new("projection requires one input"))?;
        let single = readers.get(&incoming.operator) == Some(&1);
        let source = replacements
            .get(&incoming.operator)
            .copied()
            .unwrap_or(incoming)
            .operator;
        let producer = edit.node(source)?;
        let Some(kind) = producer.operator_kind() else {
            continue;
        };
        let projection = OutputProjection {
            producer: kind,
            indexed,
            single_reader: single,
            output_type: edit.type_id(output_type)?,
        };
        if !projection.accepts_producer() {
            continue;
        }
        let before = edit.expression(producer.edge("function")?)?;
        let Some(projected) = projection.apply(&mut edit, before, function, &mut warnings)? else {
            continue;
        };
        let producer = edit.node(source)?;
        let inputs = producer
            .inputs()
            .map(|p| replacements.get(&p.operator).copied().unwrap_or(p))
            .collect::<Vec<_>>();
        let properties: Vec<_> = [
            "balanced",
            "isLeft",
            "leftTimestampIndex",
            "rightTimestampIndex",
            "comparator",
        ]
        .into_iter()
        .filter_map(|key| Some((key, producer.field(key)?.clone())))
        .collect();
        let site = if indexed && projected.kind == Op::Constant {
            id
        } else {
            source
        };
        let new = projected.append(&mut edit, bodies[&id], site, &inputs, properties)?;
        if site == source {
            edit.copy_annotations(source, new)?;
        }
        let origins = match &mut provenance {
            Some(origins) => origins,
            None => provenance.insert(original.provenance()?),
        };
        if indexed && projected_join(projected.kind, indexed).is_some() {
            let origin = origins.fuse_origins(new, source, id)?;
            edit.set_intermediate_origin(new, origin)?;
        } else {
            origins.inherit_origin(new, site);
        }
        edit.place_before(bodies[&id], new, id)?;
        replacements.insert(
            id,
            OutputPort {
                operator: new,
                number: 0,
            },
        );
        projections.replaced(id);
    }
    let result = projections.finish(edit.finish_forwarding(&replacements)?)?;
    *next = names;
    Ok((result, warnings))
}
