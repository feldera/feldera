//! Adapt collection and delta streams at window and error-view boundaries.

use feldera_sql_ir::{CircuitEditor, Error, Field, NodeId, OperatorKind as Op, OutputPort, TypeId};

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum StreamKind {
    Collection,
    Delta,
}

#[derive(Clone, Copy)]
pub struct Stream {
    pub port: OutputPort,
    pub ty: TypeId,
    pub kind: StreamKind,
}

fn stream(id: NodeId, ty: TypeId, kind: StreamKind) -> Stream {
    Stream {
        port: OutputPort {
            operator: id,
            number: 0,
        },
        ty,
        kind,
    }
}

fn adapt(
    edit: &mut CircuitEditor,
    body: NodeId,
    site: NodeId,
    value: Stream,
    to: StreamKind,
) -> Result<Stream, Error> {
    if value.kind == to {
        return Ok(value);
    }
    let op = if to == StreamKind::Delta {
        Op::Differentiate
    } else {
        Op::Integrate
    };
    let id = edit.append_operator(
        body,
        site,
        op,
        &[value.port],
        [("outputType", Field::Reference(value.ty.into()))],
    )?;
    Ok(stream(id, value.ty, to))
}

#[derive(Clone, Copy)]
pub struct WindowBounds {
    pub lower_inclusive: bool,
    pub upper_inclusive: bool,
    pub lower_unbounded: bool,
}

/// Return the delta input to the window and its result in the caller's stream kind.
pub fn window(
    edit: &mut CircuitEditor,
    body: NodeId,
    site: NodeId,
    data: Stream,
    control: super::Bound,
    bounds: WindowBounds,
) -> Result<(Stream, Stream), Error> {
    use feldera_sql_ir::TypeKind;
    if edit.type_kind(data.ty)? != TypeKind::IndexedZSet
        || edit.type_kind(control.ty)? != TypeKind::RawTuple
    {
        return Err(Error::new(
            "a window requires indexed data and a pair of key bounds",
        ));
    }
    let key = edit.type_id(edit.node(data.ty.into())?.edge("keyType")?)?;
    let bounds_type = edit.node(control.ty.into())?.edges("tupFields")?;
    let [lower, upper] = bounds_type.as_slice() else {
        return Err(Error::new("expected two window bounds"));
    };
    let lower = edit.type_id(*lower)?;
    if !edit.same_type(lower, edit.type_id(*upper)?)? {
        return Err(Error::new("window bounds must have matching types"));
    }
    let enclosed = if edit.type_kind(lower)? == TypeKind::TypedBox {
        let args = edit.node(lower.into())?.edges("typeArgs")?;
        let [inner, dynamic] = args.as_slice() else {
            return Err(Error::new("invalid boxed window bound"));
        };
        if edit.node(*dynamic)?.data("name").and_then(|n| n.as_str()) != Some("DynData") {
            return Err(Error::new("boxed window bound requires DynData"));
        }
        edit.type_id(*inner)?
    } else {
        lower
    };
    if !edit.same_type(key, enclosed)? {
        return Err(Error::new("window bound must have the indexed key type"));
    }
    let input = adapt(edit, body, site, data, StreamKind::Delta)?;
    let id = edit.append_operator(
        body,
        site,
        Op::Window,
        &[input.port, control.port],
        [
            ("outputType", Field::Reference(data.ty.into())),
            ("lowerInclusive", Field::Data(bounds.lower_inclusive.into())),
            ("upperInclusive", Field::Data(bounds.upper_inclusive.into())),
            ("lowerUnbounded", Field::Data(bounds.lower_unbounded.into())),
        ],
    )?;
    let result = adapt(
        edit,
        body,
        site,
        stream(id, data.ty, StreamKind::Delta),
        data.kind,
    )?;
    Ok((input, result))
}

/// Replace the input of ERROR_VIEW with the error streams collected before
/// visiting it. Its original input determines the required stream kind.
pub fn connect_error_view(
    edit: &mut CircuitEditor,
    body: NodeId,
    view: NodeId,
    errors: &[Stream],
    expected: StreamKind,
) -> Result<(), Error> {
    let old_count = edit.node(body)?.edges("allOperators")?.len();
    let collected = match errors {
        [] => {
            let output = edit.node(view)?.edge("outputType")?;
            let element = edit.type_id(edit.node(output)?.edge("elementType")?)?;
            let empty = edit.empty_zset(element)?;
            let ty = edit.expression_type(empty)?;
            let op = if expected == StreamKind::Delta {
                Op::IncrementalConstant
            } else {
                Op::Constant
            };
            let id = edit.append_operator(
                body,
                view,
                op,
                &[],
                [
                    ("outputType", Field::Reference(ty.into())),
                    ("function", Field::Reference(empty.into())),
                ],
            )?;
            stream(id, ty, expected)
        }
        [only] => *only,
        [first, ..] => {
            let ports = errors.iter().map(|e| e.port).collect::<Vec<_>>();
            let id = edit.append_operator(
                body,
                view,
                Op::Sum,
                &ports,
                [("outputType", Field::Reference(first.ty.into()))],
            )?;
            stream(id, first.ty, first.kind)
        }
    };
    let input = adapt(edit, body, view, collected, expected)?;
    let order = edit.node(body)?.edges("allOperators")?;
    if let Some(&first) = order.get(old_count) {
        edit.place_before(body, first, view)?;
    }
    edit.replace_view_input(view, input.port, input.ty)
}
