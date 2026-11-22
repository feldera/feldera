//! Buffer an EMIT FINAL view until its waterline passes the selected column.
use super::{
    Bound, LimiterError, Projection, Stream, WindowBounds,
    filter::{arg, bool_type, tuple_compare},
    source::link,
};
use crate::rebuild::deindex_function;
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, Node, NodeId,
    OperatorKind as Op, OutputPort, TypeId,
};

fn column(
    edit: &mut CircuitEditor,
    row: ExpressionId,
    index: usize,
) -> Result<ExpressionId, Error> {
    let row = edit.dereference(row)?;
    edit.tuple_field(row, index)
}

fn boxed(edit: &mut CircuitEditor, value: ExpressionId) -> Result<ExpressionId, Error> {
    let ty = edit.dynamic_box_type(edit.expression_type(value)?)?;
    edit.make_expr(
        value.into(),
        Expr::Unary,
        ty,
        [
            ("source", link(value)),
            ("opcode", Field::Data("TYPEDBOX".into())),
        ],
    )
}

/// The row column and its position after the waterline drops non-monotone columns.
#[derive(Clone, Copy, serde::Deserialize)]
pub struct FinalityColumn {
    pub row: usize,
    pub waterline: usize,
}

pub struct FinalityFunctions {
    pub bounds: ExpressionId,
    pub filter: Option<ExpressionId>,
    pub index: ExpressionId,
    key_type: TypeId,
    pub project_bound: Option<ExpressionId>,
}

impl FinalityColumn {
    pub(super) fn from_sink(
        sink: &Node,
        projection: Option<&Projection>,
        operator: u64,
        next: u64,
    ) -> Result<Option<Self>, LimiterError> {
        let Some(Field::Object(metadata)) = sink.field("metadata") else {
            return Ok(None);
        };
        let Some(Field::Data(index)) = metadata.get("emitFinalColumn") else {
            return Ok(None);
        };
        let row = match index.as_i64() {
            Some(-1) => return Ok(None),
            Some(index) if index >= 0 => usize::try_from(index)
                .map_err(|_| Error::new("EMIT FINAL column index exceeds row width"))?,
            _ => return Err(Error::new("invalid EMIT FINAL column index").into()),
        };
        if let Some(Projection::Tuple { fields, .. }) = projection {
            if fields.get(row).is_some_and(Projection::monotone) {
                let waterline = fields[..row].iter().filter(|p| p.monotone()).count();
                return Ok(Some(Self { row, waterline }));
            }
        }
        let view = sink.relation_name("viewName")?;
        let Some(Field::Array(columns)) = metadata.get("columns") else {
            return Err(Error::new("missing view columns for EMIT FINAL").into());
        };
        let Some(Field::Object(column)) = columns.get(row) else {
            return Err(Error::new("EMIT FINAL column index exceeds row width").into());
        };
        let Some(Field::Object(identifier)) = column.get("columnName") else {
            return Err(Error::new("missing EMIT FINAL column identifier").into());
        };
        let Some(Field::Data(serde_json::Value::String(name))) = identifier.get("name") else {
            return Err(Error::new("missing EMIT FINAL column name").into());
        };
        Err(LimiterError::MissingWaterline {
            operator, row, view: view.to_owned(), column: name.clone(), next,
        })
    }

    pub fn functions(
        self,
        edit: &mut CircuitEditor,
        site: NodeId,
        row_type: TypeId,
        control: TypeId,
        next: &mut u64,
    ) -> Result<FinalityFunctions, Error> {
        // The index parameter is allocated first, before the control and optional filter.
        let row = arg(edit, row_type, next)?;
        let key = column(edit, row, self.row)?;
        let key_type = edit.expression_type(key)?;
        let control_var = arg(edit, control, next)?;
        let mut upper = column(edit, control_var, self.waterline)?;
        let nullable = edit.nullable(key_type)?;
        if nullable {
            upper = edit.some_at(upper.into(), upper)?;
        }
        let minimum = edit.minimum_value(key_type)?;
        let minimum = boxed(edit, minimum)?;
        let upper = boxed(edit, upper)?;
        let pair = edit.tuple_expr(site, true, &[minimum, upper])?;
        let bounds = edit.closure_from_variables(pair, &[control_var])?;

        let filter = if nullable {
            let row = arg(edit, row_type, next)?;
            let key = column(edit, row, self.row)?;
            let ty = bool_type(edit, site)?;
            let test = edit.make_expr(site, Expr::IsNull, ty, [("expression", link(key))])?;
            let keep = edit.make_expr(
                site,
                Expr::Unary,
                ty,
                [
                    ("source", link(test)),
                    ("opcode", Field::Data("NOT".into())),
                ],
            )?;
            Some(edit.closure_from_variables(keep, &[row])?)
        } else {
            None
        };

        let key = edit.clone_if_needed(key)?;
        let value = edit.dereference(row)?;
        let value = edit.clone_if_needed(value)?;
        let pair = edit.tuple_expr(site, true, &[key, value])?;
        let index = edit.closure_from_variables(pair, &[row])?;

        let project_bound = if edit.node(control.into())?.edges("tupFields")?.len() > 1 {
            let arg = arg(edit, control, next)?;
            let field = column(edit, arg, self.waterline)?;
            let field = edit.clone_if_needed(field)?;
            let single = edit.tuple_expr(site, false, &[field])?;
            Some(edit.closure_from_variables(single, &[arg])?)
        } else {
            None
        };
        Ok(FinalityFunctions {
            bounds,
            filter,
            index,
            key_type,
            project_bound,
        })
    }
}

fn output(operator: NodeId) -> OutputPort {
    OutputPort {
        operator,
        number: 0,
    }
}

fn apply(
    edit: &mut CircuitEditor,
    body: NodeId,
    site: NodeId,
    input: Bound,
    function: ExpressionId,
) -> Result<Bound, Error> {
    let ty = edit.type_id(
        edit.node(edit.node(function.into())?.edge("type")?)?
            .edge("resultType")?,
    )?;
    let op = edit.append_operator(
        body,
        site,
        Op::Apply,
        &[input.port],
        [("outputType", link(ty)), ("function", link(function))],
    )?;
    Ok(Bound {
        port: output(op),
        ty,
    })
}

pub(super) fn emit_final(
    edit: &mut CircuitEditor,
    body: NodeId,
    sink: NodeId,
    mut data: Stream,
    mut bound: Bound,
    field: FinalityColumn,
    next: &mut u64,
) -> Result<(), Error> {
    let row = edit.type_id(edit.node(data.ty.into())?.edge("elementType")?)?;
    let f = field.functions(edit, sink, row, bound.ty, next)?;
    let control = apply(edit, body, sink, bound, f.bounds)?;
    if let Some(filter) = f.filter {
        let id = edit.append_operator(
            body,
            sink,
            Op::Filter,
            &[data.port],
            [("outputType", link(data.ty)), ("function", link(filter))],
        )?;
        data.port = output(id);
    }
    let key_type = f.key_type;
    data.ty = edit.indexed_zset_type(sink, key_type, row)?;
    data.port = output(edit.append_operator(
        body,
        sink,
        Op::MapIndex,
        &[data.port],
        [("outputType", link(data.ty)), ("function", link(f.index))],
    )?);
    let window_start = edit.node(body)?.edges("allOperators")?.len();
    let (input, result) = super::window(
        edit,
        body,
        sink,
        data,
        control,
        WindowBounds {
            lower_inclusive: true,
            upper_inclusive: false,
            lower_unbounded: true,
        },
    )?;
    let position = edit
        .node(sink)?
        .field("metadata")
        .and_then(|field| {
            let Field::Object(metadata) = field else {
                return None;
            };
            let Field::Object(properties) = metadata.get("properties")? else {
                return None;
            };
            let Field::Object(property) = properties.get("emit_final")? else {
                return None;
            };
            property.get("key_position")
        })
        .map(Field::source_range)
        .transpose()?;
    if let Some(position) = position {
        let operators = edit.node(body)?.edges("allOperators")?;
        edit.set_property_position(&operators[window_start..], position)?;
    }
    if let Some(project) = f.project_bound {
        bound = apply(edit, body, sink, bound, project)?;
    }

    retain_window_keys(edit, body, sink, input, bound, key_type, next)?;

    let map = deindex_function(edit, sink, key_type, row, next)?;
    let ty = edit.zset_type(row)?;
    let deindex = edit.append_operator(
        body,
        sink,
        Op::Deindex,
        &[result.port],
        [("outputType", link(ty)), ("function", link(map))],
    )?;
    edit.move_origin_definition(sink, control.port.operator)?;
    edit.place_before(body, control.port.operator, sink)?;
    edit.replace_stream_inputs(sink, &[output(deindex)], ty)
}

fn retain_window_keys(
    edit: &mut CircuitEditor,
    body: NodeId,
    sink: NodeId,
    input: Stream,
    bound: Bound,
    key_type: TypeId,
    next: &mut u64,
) -> Result<(), Error> {
    let minimum = edit.minimum_value(bound.ty)?;
    let delay = edit.append_operator(
        body,
        bound.port.operator,
        Op::Delay,
        &[bound.port],
        [("outputType", link(bound.ty)), ("function", link(minimum))],
    )?;
    let control_var = arg(edit, bound.ty, next)?;
    let data_var = arg(edit, key_type, next)?;
    let left = edit.dereference(data_var)?;
    let right = column(edit, control_var, 0)?;
    let compare = tuple_compare(edit, left, right)?;
    let key_ref = edit.expression_type(data_var)?;
    let data_param = edit.parameter_at(key_ref.into(), data_var)?;
    let control_param = edit.parameter_at(control_var.into(), control_var)?;
    let predicate = edit.closure_with_parameters(compare, &[data_param, control_param])?;
    edit.append_operator(
        body,
        sink,
        Op::IntegrateTraceRetainKeys,
        &[input.port, output(delay)],
        [
            ("outputType", link(input.ty)),
            ("function", link(predicate)),
            ("accumulate", Field::Data(true.into())),
        ],
    )?;

    Ok(())
}
