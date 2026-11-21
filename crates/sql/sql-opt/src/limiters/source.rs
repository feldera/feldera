//! Compute one component of the source waterline for each declared LATENESS.

use super::filter::{arg, row_filter};
use super::{Bound, Projection, Stream, StreamKind};
use feldera_sql_ir::{
    BinaryOp, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, Node,
    NodeId, OperatorKind as Op, OutputPort, TypeId, TypeKind as Ty,
};

pub(super) struct Columns {
    pub declared: Vec<(usize, NodeId)>,
    pub key: Vec<usize>,
    pub width: usize,
    pub key_types: Vec<NodeId>,
    pub materialized: bool,
}

impl Columns {
    pub fn read(op: &Node) -> Result<Self, Error> {
        let Some(Field::Object(metadata)) = op.field("metadata") else {
            return Err(Error::new("missing LATENESS metadata"));
        };
        let Some(Field::Array(columns)) = metadata
            .get("columnMetadata")
            .or_else(|| metadata.get("columns"))
        else {
            return Err(Error::new("missing column metadata"));
        };
        let mut declared = Vec::new();
        let mut key = Vec::new();
        let mut key_types = Vec::new();
        for (index, col) in columns.iter().enumerate() {
            let Field::Object(col) = col else {
                return Err(Error::new("invalid column metadata"));
            };
            if let Some(late) = col.get("lateness").and_then(Field::node_id) {
                declared.push((index, late));
            }
            if matches!(col.get("isPrimaryKey"),Some(Field::Data(v)) if v.as_bool()==Some(true)) {
                key.push(index);
                key_types.push(
                    col.get("type")
                        .and_then(Field::node_id)
                        .ok_or_else(|| Error::new("missing key type"))?,
                );
            }
        }
        Ok(Self {
            declared,
            key,
            key_types,
            width: columns.len(),
            materialized: matches!(metadata.get("materialized"),Some(Field::Data(v)) if v.as_bool()==Some(true)),
        })
    }
}

pub(super) fn link(id: impl Into<NodeId>) -> Field {
    Field::Reference(id.into())
}

fn contribution(
    edit: &mut CircuitEditor,
    site: NodeId,
    parameter: ExpressionId,
    index: usize,
    lateness: NodeId,
) -> Result<ExpressionId, Error> {
    let data = edit.dereference(parameter)?;
    let field = edit.tuple_field(data, index)?;
    let ty = edit.expression_type(field)?;
    let value = if edit.nullable(ty)? {
        let nonnull = edit.with_nullability(ty, false)?;
        let minimum = edit.minimum_value(nonnull)?;
        let boolean = edit.empty_origin_node(
            field.into(),
            InnerKind::Type(Ty::Bool),
            [
                ("code", Field::Data("BOOL".into())),
                ("mayBeNull", Field::Data(false.into())),
            ],
        )?;
        let is_null = edit.make_expr(
            field.into(),
            Expr::IsNull,
            edit.type_id(boolean)?,
            [("expression", link(field))],
        )?;
        // Java deep-copies this read in the guarded arm. The original and
        // the copy keep their shared declaration but have distinct paths.
        let copied = edit.derive_inner(parameter.into(), [])?;
        let data = edit.dereference(edit.expression(copied)?)?;
        let copy = edit.tuple_field(data, index)?;
        let unwrapped = edit.with_nullability(ty, false)?;
        let unwrap = edit.make_expr(
            site,
            Expr::Unwrap,
            unwrapped,
            [
                ("expression", link(copy)),
                ("message", Field::Data("".into())),
            ],
        )?;
        edit.make_expr(
            site,
            Expr::If,
            edit.expression_type(minimum)?,
            [
                ("condition", link(is_null)),
                ("positive", link(minimum)),
                ("negative", link(unwrap)),
            ],
        )?
    } else {
        field
    };
    let output = edit.expression_type(value)?;
    let amount = edit.expression(lateness)?;
    let temporal = matches!(
        edit.type_kind(output)?,
        Ty::Timestamp
            | Ty::TimestampTz
            | Ty::Date
            | Ty::Time
            | Ty::LongInterval
            | Ty::ShortInterval
    );
    let amount = if temporal {
        amount
    } else {
        edit.cast(site, amount, output)?
    };
    edit.make_expr(
        site,
        if temporal {
            Expr::TimeAddSub
        } else {
            Expr::Binary
        },
        output,
        [
            ("left", link(value)),
            ("right", link(amount)),
            ("opcode", Field::Data("SUB_SATURATING".into())),
        ],
    )
}

fn timestamp_max(
    edit: &mut CircuitEditor,
    site: NodeId,
    tuple: TypeId,
    next: &mut u64,
) -> Result<ExpressionId, Error> {
    let left = arg(edit, tuple, next)?;
    let right = arg(edit, tuple, next)?;
    let mut fields = Vec::new();
    for (i, field) in edit
        .node(tuple.into())?
        .edges("tupFields")?
        .into_iter()
        .enumerate()
    {
        let a = edit.dereference(left)?;
        let a = edit.tuple_field(a, i)?;
        let b = edit.dereference(right)?;
        let b = edit.tuple_field(b, i)?;
        fields.push(edit.binary_expression(site, edit.type_id(field)?, BinaryOp::AggMax, a, b)?);
    }
    let result = edit.tuple_expr(tuple.into(), false, &fields)?;
    edit.closure_from_variables(result, &[left, right])
}

pub(super) struct SourceOutputs {
    pub data: Stream,
    pub error: Stream,
    pub bound: Bound,
}

/// Merge the bound declared on a view with the bound inherited from its input.
fn merge_view_bound(
    edit: &mut CircuitEditor,
    body: NodeId,
    declared: Bound,
    columns: &Columns,
    inherited: (Bound, &Projection),
    next: &mut u64,
) -> Result<Bound, Error> {
    let (input, projection) = inherited;
    let declared_shape = Projection::Tuple {
        fields: (0..columns.width)
            .map(|i| Projection::Scalar {
                monotone: columns.declared.iter().any(|&(col, _)| col == i),
            })
            .collect(),
        raw: false,
        nullable: false,
    };
    let function =
        projection.merge_view_waterline(edit, input.ty, &declared_shape, declared.ty, next)?;
    let value = edit.node(function.into())?.edge("body")?;
    let ty = edit.expression_type(edit.expression(value)?)?;
    let id = edit.append_operator(
        body,
        input.port.operator,
        Op::Apply2,
        &[input.port, declared.port],
        [("outputType", link(ty)), ("function", link(function))],
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

pub(super) struct LatenessSite<'a> {
    pub operator: NodeId,
    pub name: &'a str,
    pub columns: &'a Columns,
    pub kind: StreamKind,
    pub inherited: Option<(Bound, &'a Projection)>,
}

pub(super) fn source_bounds(
    edit: &mut CircuitEditor,
    body: NodeId,
    source: LatenessSite<'_>,
    next: &mut u64,
) -> Result<SourceOutputs, Error> {
    let LatenessSite {
        operator: site,
        name: table,
        columns,
        kind,
        inherited,
    } = source;
    let collection = edit.type_id(edit.node(site)?.edge("outputType")?)?;
    let row = edit.type_id(edit.node(collection.into())?.edge("elementType")?)?;
    let t = arg(edit, row, next)?;
    let fields = columns
        .declared
        .iter()
        .map(|&(index, lateness)| contribution(edit, site, t, index, lateness))
        .collect::<Result<Vec<_>, _>>()?;
    let timestamp = edit.tuple_expr(row.into(), false, &fields)?;
    let ty = edit.expression_type(timestamp)?;
    let initial = edit.minimum_value(ty)?;
    let max = timestamp_max(edit, site, ty, next)?;
    let indices = columns.declared.iter().map(|&(i, _)| i).collect::<Vec<_>>();
    let filter = row_filter(edit, table, row, &indices, ty, next)?;
    let init = edit.closure_from_variables(initial, &[])?;
    if !columns.key.is_empty() {
        return super::keyed::keyed_source(
            edit,
            body,
            &source,
            super::KeyProjection::from_columns(&columns.key, &indices),
            super::keyed::Functions {
                row,
                row_var: t,
                timestamp,
                max,
                init,
                filter,
            },
            next,
        );
    }
    let empty = edit.standard_type(t.into(), Ty::RawTuple)?;
    let unused = arg(edit, empty, next)?;
    let extract = edit.closure_from_variables(timestamp, &[t, unused])?;
    let output = edit.type_id(
        edit.node(edit.expression_type(max)?.into())?
            .edge("resultType")?,
    )?;
    let port = |operator| OutputPort {
        operator,
        number: 0,
    };
    let waterline = edit.append_operator(
        body,
        site,
        Op::Waterline,
        &[port(site)],
        [
            ("outputType", link(output)),
            ("function", link(max)),
            ("init", link(init)),
            ("extractTs", link(extract)),
        ],
    )?;
    let delay = edit.append_operator(
        body,
        site,
        Op::Delay,
        &[port(waterline)],
        [("outputType", link(output)), ("function", link(initial))],
    )?;
    let var = arg(edit, output, next)?;
    let value = edit.dereference(var)?;
    let clone = edit.make_expr(
        value.into(),
        Expr::Clone,
        output,
        [("expression", link(value))],
    )?;
    let extend = edit.closure_from_variables(clone, &[var])?;
    let extended = edit.append_operator(
        body,
        site,
        Op::Apply,
        &[port(waterline)],
        [("outputType", link(output)), ("function", link(extend))],
    )?;
    edit.annotate(extended, serde_json::json!({"class":"Waterline"}))?;
    let mut bound = Bound {
        port: port(extended),
        ty: output,
    };
    if let Some(input) = inherited {
        bound = merge_view_bound(edit, body, bound, columns, input, next)?;
    }
    let error_type = edit.zset_type(filter.error_type)?;
    let controlled = edit.append_operator(
        body,
        site,
        Op::ControlledKeyFilter,
        &[port(site), port(delay)],
        [
            ("outputType", link(collection)),
            ("function", link(filter.function)),
            ("errorType", link(error_type)),
            ("error", link(filter.error)),
        ],
    )?;
    Ok(SourceOutputs {
        data: Stream {
            port: port(controlled),
            ty: collection,
            kind,
        },
        error: Stream {
            port: OutputPort {
                operator: controlled,
                number: 1,
            },
            ty: error_type,
            kind: StreamKind::Delta,
        },
        bound,
    })
}
