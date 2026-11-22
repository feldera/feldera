//! Compare projected index keys under the bindings of their two row parameters.
mod bindings;
use bindings::{Bindings, Scope, Work};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Node, NodeId};
use std::collections::HashSet;

fn same_type(edit: &CircuitEditor, left: &Node, right: &Node) -> Result<bool, Error> {
    same_type_field(edit, left, right, "type")
}

fn same_type_field(
    edit: &CircuitEditor,
    left: &Node,
    right: &Node,
    field: &str,
) -> Result<bool, Error> {
    edit.same_type(
        edit.type_id(left.edge(field)?)?,
        edit.type_id(right.edge(field)?)?,
    )
}

fn same_type_list(
    edit: &CircuitEditor,
    left: &Node,
    right: &Node,
    field: &str,
) -> Result<bool, Error> {
    let left = left.edges(field)?;
    let right = right.edges(field)?;
    if left.len() != right.len() {
        return Ok(false);
    }
    for (a, b) in left.into_iter().zip(right) {
        if !edit.same_type(edit.type_id(a)?, edit.type_id(b)?)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn optional_child(
    left: &Node,
    right: &Node,
    field: &str,
    scope: Scope,
    pending: &mut Vec<Work>,
) -> Result<bool, Error> {
    match (left.field(field), right.field(field)) {
        (None, None) => Ok(true),
        (Some(_), Some(_)) => {
            pending.push(Work::Compare(left.edge(field)?, right.edge(field)?, scope));
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn compare_children(
    left: &Node,
    right: &Node,
    field: &str,
    scope: Scope,
    pending: &mut Vec<Work>,
) -> Result<bool, Error> {
    if left.field(field).is_none() || right.field(field).is_none() {
        return Ok(left.field(field).is_none() && right.field(field).is_none());
    }
    let l = left.edges(field)?;
    let r = right.edges(field)?;
    if l.len() != r.len() {
        return Ok(false);
    }
    pending.extend(
        l.into_iter()
            .zip(r)
            .rev()
            .map(|(l, r)| Work::Compare(l, r, scope)),
    );
    Ok(true)
}

fn same_path(edit: &CircuitEditor, left: &Node, right: &Node) -> Result<Option<bool>, Error> {
    let l = edit.node(left.edge("path")?)?.edges("components")?;
    let r = edit.node(right.edge("path")?)?.edges("components")?;
    if l.len() != r.len() {
        return Ok(Some(false));
    }
    for (l, r) in l.into_iter().zip(r) {
        let l = edit.node(l)?;
        let r = edit.node(r)?;
        if l.class() != "DBSPSimplePathSegment" || r.class() != "DBSPSimplePathSegment" {
            return Ok(None);
        }
        if l.data("identifier") != r.data("identifier")
            || !same_type_list(edit, l, r, "genericArgs")?
        {
            return Ok(Some(false));
        }
    }
    Ok(Some(true))
}

/// Some expression classes have additional equivalence rules on the Java side.
/// Return an undecided comparison when one of those rules is needed.
pub(super) fn equivalent(
    edit: &CircuitEditor,
    left: ExpressionId,
    right: ExpressionId,
) -> Result<Option<bool>, Error> {
    let left = edit.node(left.into())?;
    let right = edit.node(right.into())?;
    let [lp] = left
        .edges("parameters")?
        .try_into()
        .map_err(|_| Error::new("expected one index key parameter"))?;
    let [rp] = right
        .edges("parameters")?
        .try_into()
        .map_err(|_| Error::new("expected one index key parameter"))?;
    let lp = edit.node(lp)?;
    let rp = edit.node(rp)?;
    if !edit.same_type(
        edit.type_id(lp.edge("type")?)?,
        edit.type_id(rp.edge("type")?)?,
    )? {
        return Ok(Some(false));
    }
    equivalent_bodies(
        edit,
        left.edge("body")?,
        right.edge("body")?,
        Some((lp, rp)),
    )
}

/// With no enclosing parameter mapping, free variable paths match by identity.
/// Fields of a wide index are reduced against a single shared variable.
pub(super) fn equivalent_bodies(
    edit: &CircuitEditor,
    left: NodeId,
    right: NodeId,
    parameters: Option<(&Node, &Node)>,
) -> Result<Option<bool>, Error> {
    let names = parameters
        .map(|(l, r)| Ok::<_, Error>((bindings::name(l, "name")?, bindings::name(r, "name")?)))
        .transpose()?;
    compare_expressions(edit, left, right, names)
}

/// Resolve distinct variable occurrences in the scope enclosing the two roots.
/// Nested LET, block and closure declarations shadow those enclosing bindings.
pub(crate) fn compare_expressions<'a>(
    edit: &'a CircuitEditor,
    left: NodeId,
    right: NodeId,
    parameters: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<Option<bool>, Error> {
    let mut bindings = Bindings::default();
    let outer = parameters.into_iter().fold(None, |scope, (left, right)| {
        bindings.bind(left, right, scope)
    });
    let mut pending = vec![Work::Compare(left, right, outer)];
    let mut seen = HashSet::new();
    let mut active_binders = HashSet::new();
    while let Some(work) = pending.pop() {
        let (a, b, scope) = match work {
            Work::Compare(a, b, scope) => (a, b, scope),
            Work::FlatmapOutput(a, b) => {
                let left = edit.node(a)?;
                let right = edit.node(b)?;
                if left.field("passthroughIndexes") != right.field("passthroughIndexes") {
                    return Ok(Some(false));
                }
                if a != b {
                    // A shuffle compares by Java object identity. An older or detached
                    // snapshot may carry its contents without the live identity.
                    match (left.data("shuffleIdentity"), right.data("shuffleIdentity")) {
                        (Some(l), Some(r)) if l == r => {}
                        (Some(_), Some(_)) => return Ok(Some(false)),
                        _ => return Ok(None),
                    }
                }
                if !same_type(edit, left, right)?
                    || !compare_children(left, right, "postProcess", None, &mut pending)?
                {
                    return Ok(Some(false));
                }
                continue;
            }
            Work::Leave(a, b) => {
                active_binders.remove(&(a, b));
                continue;
            }
        };
        if !seen.insert((a, b, scope)) {
            continue;
        }
        let left = edit.node(a)?;
        let right = edit.node(b)?;
        let kind = edit.expr_kind(edit.expression(a)?)?;
        if left.class() != right.class() {
            return Ok(Some(false));
        }
        // A cyclic expression can revisit the same binder under fresh scopes.
        // A shared acyclic term is eligible for comparison again after leaving it.
        if matches!(kind, Expr::Let | Expr::Block | Expr::Closure | Expr::For) {
            if !active_binders.insert((a, b)) {
                return Err(Error::new("cycle in key expression bindings"));
            }
            pending.push(Work::Leave(a, b));
        }
        let children: &[&str] = match kind {
            Expr::VariablePath => {
                if a != b {
                    match bindings.variables(scope, left, right)? {
                        Some(true) => {}
                        result => return Ok(result),
                    }
                }
                &[]
            }
            Expr::Let => {
                bindings.let_expression(edit, left, right, scope, &mut pending)?;
                continue;
            }
            Expr::For => {
                let Some(variable) = left.data("variable").and_then(|v| v.as_str()) else {
                    return Ok(None);
                };
                if scope.is_none() {
                    return Ok(None);
                }
                // DBSPForExpression.equivalent enters the left name in both maps.
                let local = bindings.bind(variable, variable, scope);
                pending.push(Work::Compare(
                    left.edge("block")?,
                    right.edge("block")?,
                    local,
                ));
                pending.push(Work::Compare(
                    left.edge("iterated")?,
                    right.edge("iterated")?,
                    scope,
                ));
                continue;
            }
            Expr::Flatmap => {
                pending.push(Work::FlatmapOutput(a, b));
                &["collectionExpression"]
            }
            Expr::Closure => {
                if !bindings.closure(edit, left, right, scope, &mut pending)? {
                    return Ok(Some(false));
                }
                continue;
            }
            Expr::Block => match bindings.block(edit, left, right, scope, &mut pending)? {
                Some(true) => continue,
                answer => return Ok(answer),
            },
            Expr::Tuple | Expr::RawTuple | Expr::Array | Expr::Map => {
                if !same_type(edit, left, right)? {
                    return Ok(Some(false));
                }
                let lists: &[&str] = match kind {
                    Expr::Array => &["data"],
                    Expr::Map => &["keys", "values"],
                    _ => &["fields"],
                };
                for field in lists.iter().rev() {
                    if !compare_children(left, right, field, scope, &mut pending)? {
                        return Ok(Some(false));
                    }
                }
                &[]
            }
            Expr::Path => match same_path(edit, left, right)? {
                Some(true) => &[],
                result => return Ok(result),
            },
            Expr::Apply | Expr::Constructor | Expr::ApplyMethod => {
                if !compare_children(left, right, "arguments", scope, &mut pending)? {
                    return Ok(Some(false));
                }
                if kind == Expr::ApplyMethod {
                    &["self", "function"]
                } else {
                    &["function"]
                }
            }
            kind if kind.wire_name().ends_with("Literal") => {
                if !same_type(edit, left, right)? {
                    return Ok(Some(false));
                }
                match feldera_sql_expr::same_literal_value(
                    edit,
                    edit.expression(a)?,
                    edit.expression(b)?,
                )? {
                    Some(true) => &[],
                    result => return Ok(result),
                }
            }
            Expr::GeoPointConstructor => {
                if !same_type(edit, left, right)? {
                    return Ok(Some(false));
                }
                &["left", "right"]
            }
            Expr::Assignment => &["left", "right"],
            Expr::QualifyType => {
                if !same_type_list(edit, left, right, "types")? {
                    return Ok(Some(false));
                }
                &["expression"]
            }
            Expr::ZSet | Expr::IndexedZSet => {
                if !feldera_sql_expr::constant_rows::equal_collections(
                    edit,
                    edit.expression(a)?,
                    edit.expression(b)?,
                )? {
                    return Ok(Some(false));
                }
                &[]
            }
            Expr::Fold | Expr::NonLinearAggregate | Expr::MinMaxAggregate => {
                if !same_type_field(edit, left, right, "semigroup")? {
                    return Ok(Some(false));
                }
                match kind {
                    Expr::Fold => &["zero", "increment", "postProcess"],
                    Expr::NonLinearAggregate => {
                        &["zero", "increment", "postProcess", "emptySetResult"]
                    }
                    _ => &[
                        "zero",
                        "increment",
                        "postProcess",
                        "emptySetResult",
                        "comparedValue",
                    ],
                }
            }
            Expr::LinearAggregate => &["map", "postProcess", "emptySetResult"],
            Expr::MinMax => {
                if !same_type(edit, left, right)? {
                    return Ok(Some(false));
                }
                &["postProcessing"]
            }
            Expr::ConditionalIncrement => &["left", "right", "condition"],
            Expr::Field
            | Expr::CustomOrdField
            | Expr::Borrow
            | Expr::Clone
            | Expr::Deref
            | Expr::IsNull
            | Expr::Some
            | Expr::Unwrap
            | Expr::UnwrapCustomOrd
            | Expr::Lazy => &["expression"],
            Expr::Cast => {
                if !same_type(edit, left, right)? {
                    return Ok(Some(false));
                }
                &["source"]
            }
            Expr::TimeAddSub | Expr::Binary => &["left", "right"],
            Expr::Unary
            | Expr::Question
            | Expr::HandleError
            | Expr::UnsignedWrap
            | Expr::UnsignedUnwrap
            | Expr::DirectComparator
            | Expr::FieldComparator => &["source"],
            Expr::CustomOrd => &["source", "comparator"],
            Expr::Sort | Expr::EqualityComparator => &["comparator"],
            Expr::WindowBound => &["representation"],
            Expr::Return => &["argument"],
            Expr::If => &["condition", "positive", "negative"],
            Expr::Variant => {
                if !same_type(edit, left, right)? {
                    return Ok(Some(false));
                }
                // Java treats a SQL NULL on the left as a separate comparison case.
                if left.data("isSqlNull").and_then(|v| v.as_bool()) == Some(true) {
                    if left.data("isSqlNull") != right.data("isSqlNull") {
                        return Ok(Some(false));
                    }
                    &[]
                } else {
                    &["value"]
                }
            }
            Expr::Fail | Expr::NoExpression | Expr::EnumValue => &[],
            Expr::NoComparator | Expr::AsymmetricFieldComparator => {
                let type_fields: &[&str] = if kind == Expr::NoComparator {
                    &["tupleType"]
                } else {
                    &["leftType", "rightType"]
                };
                for field in type_fields {
                    if !same_type_field(edit, left, right, field)? {
                        return Ok(Some(false));
                    }
                }
                &[]
            }
            Expr::Static => {
                if a != b {
                    return Ok(Some(false));
                }
                &[]
            }
            _ => return Ok(None),
        };
        let attributes: &[&str] = match kind {
            Expr::AsymmetricFieldComparator => &["comparisons"],
            Expr::MinMax => &["aggregation"],
            Expr::MinMaxAggregate => &["operation"],
            Expr::DirectComparator => &["ascending"],
            Expr::FieldComparator => &["ascending", "fieldNo", "nullsFirst"],
            Expr::Field | Expr::CustomOrdField => &["fieldNo"],
            Expr::Borrow => &["mut"],
            Expr::Cast => &["safe"],
            Expr::Unary | Expr::Binary | Expr::ConditionalIncrement => &["opcode"],
            Expr::TimeAddSub => &["opcode", "shortUnits", "longUnits"],
            Expr::UnsignedWrap | Expr::UnsignedUnwrap => &["ascending", "nullsLast"],
            Expr::HandleError => &["runtimeBehavior", "hasSourcePosition", "index"],
            Expr::WindowBound => &["isPreceding"],
            Expr::EnumValue => &["enumName", "constructor"],
            _ => &[],
        };
        if attributes
            .iter()
            .any(|name| left.field(name) != right.field(name))
        {
            return Ok(Some(false));
        }
        for name in children.iter().rev() {
            let optional = matches!(
                (kind, *name),
                (Expr::If, "negative")
                    | (Expr::GeoPointConstructor, "left" | "right")
                    | (Expr::ConditionalIncrement, "condition")
                    | (Expr::Variant, "value")
                    | (
                        Expr::NonLinearAggregate | Expr::MinMaxAggregate,
                        "postProcess"
                    )
                    | (Expr::MinMax, "postProcessing")
            );
            if optional {
                if !optional_child(left, right, name, scope, &mut pending)? {
                    return Ok(Some(false));
                }
            } else {
                pending.push(Work::Compare(left.edge(name)?, right.edge(name)?, scope));
            }
        }
    }
    Ok(Some(true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::{Circuit, Field};
    use serde_json::json;

    #[test]
    fn nested_bindings_compare_initializers_in_their_enclosing_scope() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]
        }))?;
        let mut edit = CircuitEditor::new(circuit);
        let site = edit.circuit().root_id();
        let ty = edit.bool_type(site, false)?;

        // |row| { let row = row; |arg| LET value = row IN (value, arg) }
        // All names may change, but a reference to arg cannot become a reference to row.
        let mut key = |row, local, arg, alias, capture_arg| -> Result<ExpressionId, Error> {
            let outer = edit.named_variable(ty, row)?;
            let inner = edit.named_variable(ty, arg)?;
            let binding = edit.let_statement(local, outer)?;
            let read = edit.named_variable(ty, local)?;
            let alias = edit.named_variable(ty, alias)?;
            let captured = if capture_arg { inner } else { read };
            let pair = edit.tuple_expr(site, false, &[alias, captured])?;
            let pair = edit.let_expression(alias, read, pair)?;
            let lambda = edit.closure_from_variables(pair, &[inner])?;
            let block = edit.make_expr(
                site,
                Expr::Block,
                edit.expression_type(lambda)?,
                [
                    ("contents", Field::Array(vec![Field::Reference(binding)])),
                    ("lastExpression", Field::Reference(lambda.into())),
                ],
            )?;
            edit.closure_from_variables(block, &[outer])
        };
        let left = key("r", "r", "s", "x", true)?;
        let renamed = key("a", "b", "c", "d", true)?;
        let captured = key("a", "b", "c", "d", false)?;
        assert_eq!(equivalent(&edit, left, renamed)?, Some(true));
        assert_eq!(equivalent(&edit, left, captured)?, Some(false));
        Ok(())
    }

    #[test]
    fn shared_reads_are_compared_in_each_binding_scope() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]
        }))?;
        let mut edit = CircuitEditor::new(circuit);
        let site = edit.circuit().root_id();
        let ty = edit.bool_type(site, false)?;
        let mut key = |row_name, local_name| -> Result<_, Error> {
            let row = edit.named_variable(ty, row_name)?;
            let local = edit.named_variable(ty, local_name)?;
            let shadow = edit.let_expression(local, row, row)?;
            let pair = edit.tuple_expr(site, false, &[row, shadow])?;
            edit.closure_from_variables(pair, &[row])
        };
        let left = key("x", "x")?;
        let right = key("y", "z")?;
        assert_eq!(equivalent(&edit, left, right)?, Some(false));
        Ok(())
    }

    #[test]
    fn ordering_keys_bind_the_row_by_position_and_keep_null_order() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]
        }))?;
        let (circuit, roots) = circuit.with_inner_roots([json!({
            "class":"DBSPTypeInteger","id":1,"width":32,"signed":true,"mayBeNull":true,"code":"INT32"
        }),json!({
            "class":"DBSPTypeInteger","id":2,"width":64,"signed":false,"mayBeNull":false,"code":"UINT64"
        })])?;
        let mut edit = CircuitEditor::new(circuit);
        let ty = edit.type_id(roots[0])?;
        let unsigned = edit.type_id(roots[1])?;
        let mut key = |name, nulls_last| -> Result<_, Error> {
            let row = edit.named_variable(ty, name)?;
            let body = edit.make_expr(
                row.into(),
                Expr::UnsignedWrap,
                unsigned,
                [
                    ("source", Field::Reference(row.into())),
                    ("ascending", Field::Data(true.into())),
                    (
                        "nullsLast",
                        Field::Data(serde_json::Value::Bool(nulls_last)),
                    ),
                ],
            )?;
            edit.closure_from_variables(body, &[row])
        };
        let left = key("l", true)?;
        let right = key("r", true)?;
        let nulls_first = key("r", false)?;
        assert_eq!(equivalent(&edit, left, right)?, Some(true));
        assert_eq!(equivalent(&edit, left, nulls_first)?, Some(false));
        Ok(())
    }
}
