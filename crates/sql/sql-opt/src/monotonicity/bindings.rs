//! Adapt the single waterline of an Apply operator to the original closure's inputs.
use feldera_sql_ir::{CircuitEditor, Error, NodeId, TypeId};

pub struct WaterlineBindings {
    pub parameter: NodeId,
    pub statements: Vec<NodeId>,
}

/// An indexed closure takes one tuple of references to the retained fields.
/// A join takes three separate references to its key, left value and right value.
pub fn waterline_bindings(
    edit: &mut CircuitEditor,
    inputs: &[(&str, TypeId)],
) -> Result<WaterlineBindings, Error> {
    let row = match inputs {
        [(_, pair)] => *pair,
        [(_, key), (_, left), (_, right)] => edit.tuple_type(
            (*key).into(),
            true,
            [(*key).into(), (*left).into(), (*right).into()],
        )?,
        _ => {
            return Err(Error::new(
                "expected one indexed input or three join inputs",
            ));
        }
    };
    let arity = edit.node(row.into())?.edges("tupFields")?.len();
    if inputs.len() == 1 && !(1..=2).contains(&arity) {
        return Err(Error::new(
            "indexed waterline requires a projected key or value",
        ));
    }
    let reference = edit.reference_type(row)?;
    let mut fields = Vec::with_capacity(arity);
    for index in 0..arity {
        // Each occurrence has its own expression identity and shares the parameter type.
        let path = edit.named_variable(reference, "a")?;
        let value = edit.dereference(path)?;
        let field = edit.tuple_field(value, index)?;
        fields.push(edit.borrow_value(field)?);
    }
    let statements = if let [(name, _)] = inputs {
        let pair = edit.tuple_expr(row.into(), true, &fields)?;
        vec![edit.let_statement(name, pair)?]
    } else {
        inputs
            .iter()
            .zip(fields)
            .map(|((name, _), field)| edit.let_statement(name, field))
            .collect::<Result<_, _>>()?
    };
    let path = edit.named_variable(reference, "a")?;
    let parameter = edit.parameter_at(reference.into(), path)?;
    Ok(WaterlineBindings {
        parameter,
        statements,
    })
}
