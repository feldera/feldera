//! An indexed source checks both complete rows and updates that carry only a key.

use super::filter::{arg, late_error, truth, tuple_compare, RowFilter};
use super::source::{link, Columns, LatenessSite, SourceOutputs};
use super::{Bound, Stream, StreamKind};
use crate::rebuild::deindex_function;
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, NodeId,
    OperatorKind as Op, OutputPort, TypeId, TypeKind as Ty,
};

pub(super) struct Functions {
    pub row: TypeId,
    pub row_var: ExpressionId,
    pub timestamp: ExpressionId,
    pub max: ExpressionId,
    pub init: ExpressionId,
    pub filter: RowFilter,
}

fn read_columns(
    edit: &mut CircuitEditor,
    var: ExpressionId,
    indices: &[usize],
    separate_reads: bool,
) -> Result<ExpressionId, Error> {
    let shared = edit.dereference(var)?;
    let mut fields = Vec::with_capacity(indices.len());
    for &i in indices {
        let read = if separate_reads {
            let path = edit.derive_inner(var.into(), [])?;
            edit.dereference(edit.expression(path)?)?
        } else {
            shared
        };
        fields.push(edit.tuple_field(read, i)?);
    }
    edit.tuple_expr(var.into(), false, &fields)
}

fn clone_value(edit: &mut CircuitEditor, expr: ExpressionId) -> Result<ExpressionId, Error> {
    edit.make_expr(
        expr.into(),
        Expr::Clone,
        edit.expression_type(expr)?,
        [("expression", link(expr))],
    )
}

fn add_key_parameter(
    edit: &mut CircuitEditor,
    function: ExpressionId,
    key: ExpressionId,
    is_error: bool,
) -> Result<ExpressionId, Error> {
    let params = edit.node(function.into())?.edges("parameters")?;
    let body = edit.expression(edit.node(function.into())?.edge("body")?)?;
    let declaration = edit.inner_at(
        key.into(),
        InnerKind::Parameter,
        [
            ("type", link(edit.expression_type(key)?)),
            (
                "name",
                Field::Data(
                    edit.node(key.into())?
                        .data("variable")
                        .ok_or_else(|| Error::new("missing key variable"))?
                        .clone(),
                ),
            ),
        ],
    )?;
    let args = if is_error {
        vec![params[0], declaration, params[1], params[3]]
    } else {
        vec![params[0], declaration, params[1]]
    };
    edit.closure_with_parameters(body, &args)
}

struct KeyProjection {
    key: Vec<usize>,
    waterline: Vec<usize>,
}
impl KeyProjection {
    fn new(columns: &Columns) -> Self {
        let (key, waterline) = columns
            .key
            .iter()
            .enumerate()
            .filter_map(|(i, c)| {
                columns
                    .declared
                    .iter()
                    .position(|(col, _)| col == c)
                    .map(|j| (i, j))
            })
            .unzip();
        Self { key, waterline }
    }
    fn filter(
        &self,
        edit: &mut CircuitEditor,
        control: TypeId,
        key: TypeId,
        next: &mut u64,
    ) -> Result<ExpressionId, Error> {
        let w = arg(edit, control, next)?;
        let k = arg(edit, key, next)?;
        let pred = if self.key.is_empty() {
            truth(edit, w.into())?
        } else {
            let left = read_columns(edit, k, &self.key, false)?;
            let right = read_columns(edit, w, &self.waterline, true)?;
            tuple_compare(edit, left, right)?
        };
        edit.closure_from_variables(pred, &[w, k])
    }

    fn retain(
        &self,
        edit: &mut CircuitEditor,
        body: NodeId,
        source: Stream,
        bound: Bound,
        key: TypeId,
        next: &mut u64,
    ) -> Result<(), Error> {
        // The unary apply substitutes its argument during beta reduction.
        *next = next
            .checked_add(1)
            .ok_or_else(|| Error::new("generated names exhausted"))?;
        let w = arg(edit, bound.ty, next)?;
        let mut fields = Vec::new();
        for &index in &self.waterline {
            let read = edit.dereference(w)?;
            fields.push(edit.tuple_field(read, index)?);
        }
        let key_bound = edit.tuple_expr(w.into(), false, &fields)?;
        let pair = edit.tuple_expr(w.into(), true, &[key_bound])?;
        let ty = edit.expression_type(pair)?;
        let closure = edit.closure_from_variables(pair, &[w])?;
        let proj = edit.append_operator(
            body,
            source.port.operator,
            Op::Apply,
            &[bound.port],
            [("outputType", link(ty)), ("function", link(closure))],
        )?;
        edit.annotate(proj, serde_json::json!({"class":"Waterline"}))?;
        let control = arg(edit, ty, next)?;
        let data = arg(edit, key, next)?;
        let left = read_columns(edit, data, &self.key, false)?;
        let bound = edit.dereference(control)?;
        let right = edit.tuple_field(bound, 0)?;
        let compare = tuple_compare(edit, left, right)?;
        let predicate = edit.closure_from_variables(compare, &[data, control])?;
        edit.append_operator(
            body,
            source.port.operator,
            Op::IntegrateTraceRetainKeys,
            &[
                source.port,
                OutputPort {
                    operator: proj,
                    number: 0,
                },
            ],
            [
                ("outputType", link(source.ty)),
                ("function", link(predicate)),
                ("accumulate", Field::Data(false.into())),
            ],
        )?;
        Ok(())
    }
}

pub(super) fn keyed_source(
    edit: &mut CircuitEditor,
    body: NodeId,
    source: &LatenessSite<'_>,
    functions: Functions,
    next: &mut u64,
) -> Result<SourceOutputs, Error> {
    let site = source.operator;
    let columns = source.columns;
    let Functions {
        row,
        row_var,
        timestamp,
        max,
        init,
        filter,
    } = functions;
    let key = edit.tuple_type(site, false, columns.key_types.iter().copied())?;
    let indexed = edit.inner_at(
        site,
        InnerKind::Type(Ty::IndexedZSet),
        [
            ("keyType", link(key)),
            ("elementType", link(row)),
            ("name", Field::Data("IndexedWSet".into())),
            ("code", Field::Data("INDEXED_ZSET".into())),
            ("mayBeNull", Field::Data(false.into())),
        ],
    )?;
    let k = arg(edit, key, next)?;
    let ff = add_key_parameter(edit, filter.function, k, false)?;
    let err = add_key_parameter(edit, filter.error, k, true)?;
    let control = edit.expression_type(timestamp)?;
    let proj = KeyProjection::new(columns);
    let kfilter = proj.filter(edit, control, key, next)?;
    let w = arg(edit, control, next)?;
    let kerr = arg(edit, key, next)?;
    let weight = edit.standard_type(site, Ty::Weight)?;
    let weight = edit.variable(weight, next)?;
    let message = late_error(edit, source.name, kerr)?;
    let key_error = edit.closure_from_variables(message, &[w, kerr, weight])?;
    let ts = edit.closure_from_variables(timestamp, &[k, row_var])?;
    let error_type = edit.zset_type(filter.error_type)?;
    edit.replace_with_keyed_waterline(
        site,
        error_type,
        [
            ("keyFields", Field::Data(serde_json::json!(columns.key))),
            ("outputType", link(indexed)),
            ("initializer", link(init)),
            ("timestamp", link(ts)),
            ("lub", link(max)),
            ("filter", link(ff)),
            ("keyFilter", link(kfilter)),
            ("error", link(err)),
            ("keyError", link(key_error)),
        ],
    )?;
    let deindex = deindex_function(edit, site, key, row, next)?;
    let output = edit.type_id(
        edit.node(edit.expression_type(max)?.into())?
            .edge("resultType")?,
    )?;
    let boxed = edit.dynamic_box_type(output)?;
    let var = arg(edit, boxed, next)?;
    let read = edit.dereference(var)?;
    let unboxed = edit.make_expr(
        read.into(),
        Expr::Deref,
        output,
        [("expression", link(read))],
    )?;
    let cloned = clone_value(edit, unboxed)?;
    let fun = edit.closure_from_variables(cloned, &[var])?;
    let extend = edit.append_operator(
        body,
        site,
        Op::Apply,
        &[OutputPort {
            operator: site,
            number: 2,
        }],
        [("function", link(fun)), ("outputType", link(output))],
    )?;
    edit.annotate(extend, serde_json::json!({"class":"Waterline"}))?;
    let bound = Bound {
        port: OutputPort {
            operator: extend,
            number: 0,
        },
        ty: output,
    };
    let indexed = Stream {
        port: OutputPort {
            operator: site,
            number: 0,
        },
        ty: edit.type_id(indexed)?,
        kind: source.kind,
    };
    if !columns.materialized && !proj.key.is_empty() {
        proj.retain(edit, body, indexed, bound, key, next)?;
    }
    let ty = edit.zset_type(row)?;
    let data = edit.append_operator(
        body,
        site,
        Op::Deindex,
        &[indexed.port],
        [("function", link(deindex)), ("outputType", link(ty))],
    )?;
    Ok(SourceOutputs {
        data: Stream {
            port: OutputPort {
                operator: data,
                number: 0,
            },
            ty,
            kind: source.kind,
        },
        error: Stream {
            port: OutputPort {
                operator: site,
                number: 1,
            },
            ty: error_type,
            kind: StreamKind::Delta,
        },
        bound,
    })
}
