//! Substitute closure arguments using the lexical declarations in the closure body.
use crate::{Rewrite, Transformation, Visit};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionKind as Expr, Field, InnerKind, Node, NodeId, TypeKind,
};
use std::collections::{HashMap, HashSet};

fn name<'a>(node: &'a Node, field: &str) -> Result<&'a str, Error> {
    node.data(field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::new("missing binding name"))
}

fn children(node: &Node) -> Vec<NodeId> {
    fn collect(field: &Field, out: &mut Vec<NodeId>) {
        match field {
            Field::Reference(id) | Field::Definition(id) => out.push(*id),
            Field::Array(items) => items.iter().for_each(|item| collect(item, out)),
            Field::Object(items) => items.values().for_each(|item| collect(item, out)),
            Field::Data(_) => (),
        }
    }
    let mut edges = Vec::new();
    for (key, field) in node.fields() {
        if !matches!(key, "type" | "originalStruct") {
            collect(field, &mut edges);
        }
    }
    edges
}

struct Argument {
    value: NodeId,
    /// Each use of an argument that declares a local needs distinct declarations.
    freshen: bool,
}

fn has_declaration(edit: &CircuitEditor, root: NodeId) -> Result<bool, Error> {
    let mut work = vec![root];
    let mut seen = std::collections::HashSet::new();
    while let Some(id) = work.pop() {
        if !seen.insert(id) {
            continue;
        }
        let node = edit.node(id)?;
        match node.inner_kind() {
            Some(InnerKind::Parameter | InnerKind::Expression(Expr::Let | Expr::For)) => {
                return Ok(true)
            }
            Some(InnerKind::Type(_)) => continue,
            _ if node.class() == "DBSPLetStatement" => return Ok(true),
            _ => work.extend(children(node)),
        }
    }
    Ok(false)
}

struct CopyArgument;
impl Transformation for CopyArgument {
    fn preorder(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        let node = edit.node(id)?;
        Ok(match node.inner_kind() {
            Some(InnerKind::Type(_)) => Visit::Keep,
            Some(InnerKind::Expression(Expr::VariablePath | Expr::Path))
            | Some(InnerKind::Parameter) => Visit::Replace(edit.derive_unlocated_inner(id, [])?),
            _ => Visit::Children,
        })
    }

    fn postorder_rebuilt(
        &mut self,
        edit: &mut CircuitEditor,
        original: NodeId,
        rebuilt: NodeId,
    ) -> Result<NodeId, Error> {
        if rebuilt == original {
            edit.derive_inner(original, [])
        } else {
            Ok(rebuilt)
        }
    }
}

/// Resolve paths before substitution. In particular, a let initializer reads
/// the enclosing binding and its consumer reads the newly introduced binding.
fn resolve<'a>(
    edit: &'a CircuitEditor,
    id: NodeId,
    scope: &HashMap<&'a str, usize>,
    uses: &mut HashMap<NodeId, usize>,
    depth: usize,
) -> Result<(), Error> {
    if depth > 128 {
        return Err(Error::new("closure exceeds the expression nesting limit"));
    }
    let node = edit.node(id)?;
    match node.inner_kind() {
        Some(InnerKind::Type(_)) => (),
        Some(InnerKind::Expression(Expr::VariablePath)) => {
            if let Some(&index) = scope.get(name(node, "variable")?) {
                uses.insert(id, index);
            }
        }
        Some(InnerKind::Expression(Expr::Closure)) => {
            let mut inner = scope.clone();
            for parameter in node.edges("parameters")? {
                inner.remove(name(edit.node(parameter)?, "name")?);
            }
            resolve(edit, node.edge("body")?, &inner, uses, depth + 1)?;
        }
        Some(InnerKind::Expression(Expr::For)) => {
            resolve(edit, node.edge("iterated")?, scope, uses, depth + 1)?;
            let mut inner = scope.clone();
            inner.remove(name(node, "variable")?);
            resolve(edit, node.edge("block")?, &inner, uses, depth + 1)?;
        }
        Some(InnerKind::Expression(Expr::Let)) => {
            resolve(edit, node.edge("initializer")?, scope, uses, depth + 1)?;
            let mut inner = scope.clone();
            inner.remove(name(edit.node(node.edge("variable")?)?, "variable")?);
            resolve(edit, node.edge("consumer")?, &inner, uses, depth + 1)?;
        }
        Some(InnerKind::Expression(Expr::Block)) => {
            let mut inner = scope.clone();
            for statement in node.edges("contents")? {
                resolve(edit, statement, &inner, uses, depth + 1)?;
                let statement = edit.node(statement)?;
                if statement.class() == "DBSPLetStatement" {
                    inner.remove(name(statement, "variable")?);
                }
            }
            if let Some(last) = node.field("lastExpression").and_then(Field::node_id) {
                resolve(edit, last, &inner, uses, depth + 1)?;
            }
        }
        _ => {
            for child in children(node) {
                resolve(edit, child, scope, uses, depth + 1)?;
            }
        }
    }
    Ok(())
}

struct Substitute {
    uses: HashMap<NodeId, usize>,
    arguments: Vec<Argument>,
    depth: usize,
}
impl Transformation for Substitute {
    fn preorder(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        if let Some(index) = self.uses.get(&id) {
            let arg = &self.arguments[*index];
            let value = if arg.freshen {
                Rewrite::new(CopyArgument).apply(edit, arg.value)?
            } else {
                arg.value
            };
            return Ok(Visit::Replace(value));
        }
        Ok(
            if matches!(edit.node(id)?.inner_kind(), Some(InnerKind::Type(_))) {
                Visit::Keep
            } else {
                Visit::Children
            },
        )
    }

    fn postorder(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<NodeId, Error> {
        let node = edit.node(id)?;
        if !matches!(node.inner_kind(), Some(InnerKind::Expression(Expr::Apply))) {
            return Ok(id);
        }
        let function = edit.node(node.edge("function")?)?;
        if !matches!(
            function.inner_kind(),
            Some(InnerKind::Expression(Expr::Closure))
        ) {
            return Ok(id);
        }
        substitute(
            edit,
            function.edge("body")?,
            &function.edges("parameters")?,
            &node.edges("arguments")?,
            self.depth + 1,
        )
    }
}

fn substitute(
    edit: &mut CircuitEditor,
    body: NodeId,
    parameters: &[NodeId],
    values: &[NodeId],
    depth: usize,
) -> Result<NodeId, Error> {
    if depth > 128 {
        return Err(Error::new(
            "nested closure applications exceed the expression nesting limit",
        ));
    }
    if parameters.len() != values.len() {
        return Err(Error::new(
            "closure argument count differs from its parameter count",
        ));
    }
    let mut scope = HashMap::new();
    let mut arguments = Vec::with_capacity(values.len());
    for (i, (&parameter, &value)) in parameters.iter().zip(values).enumerate() {
        scope.insert(name(edit.node(parameter)?, "name")?, i);
        arguments.push(Argument {
            value,
            freshen: has_declaration(edit, value)?,
        });
    }
    let mut uses = HashMap::new();
    resolve(edit, body, &scope, &mut uses, 0)?;
    Rewrite::new(Substitute {
        uses,
        arguments,
        depth,
    })
    .apply(edit, body)
}

/// Count distinct paths bound by a parameter, respecting shadowing inside the body.
pub fn parameter_references(
    edit: &CircuitEditor,
    body: NodeId,
    parameter: NodeId,
) -> Result<usize, Error> {
    Ok(parameter_paths(edit, body, parameter)?.len())
}

/// Return the variable nodes bound by this parameter, including captures by nested closures.
pub fn parameter_paths(
    edit: &CircuitEditor,
    body: NodeId,
    parameter: NodeId,
) -> Result<HashSet<NodeId>, Error> {
    let scope = HashMap::from([(name(edit.node(parameter)?, "name")?, 0)]);
    let mut uses = HashMap::new();
    resolve(edit, body, &scope, &mut uses, 0)?;
    Ok(uses.into_keys().collect())
}

pub fn substitute_arguments(
    edit: &mut CircuitEditor,
    body: NodeId,
    parameters: &[NodeId],
    values: &[NodeId],
) -> Result<NodeId, Error> {
    substitute(edit, body, parameters, values, 0)
}

/// Reduce an applied closure before inserting its result in the surrounding expression.
pub fn reduce_closure_body(
    edit: &mut CircuitEditor,
    body: NodeId,
    parameters: &[NodeId],
    values: &[NodeId],
) -> Result<NodeId, Error> {
    reduce_closure_body_with_warnings(edit, body, parameters, values, None)
}

pub fn reduce_closure_body_with_warnings(
    edit: &mut CircuitEditor,
    body: NodeId,
    parameters: &[NodeId],
    values: &[NodeId],
    mut warnings: Option<&mut Vec<crate::SimplifyWarning>>,
) -> Result<NodeId, Error> {
    let mut body = substitute_arguments(edit, body, parameters, values)?;
    for _ in 0..10 {
        let reduced = simplify_closure_with_warnings(edit, body, warnings.as_deref_mut())?;
        if reduced == body {
            break;
        }
        body = reduced;
    }
    Ok(body)
}

pub fn simplify_closure_expression(
    edit: &mut CircuitEditor,
    body: NodeId,
) -> Result<NodeId, Error> {
    simplify_closure_with_warnings(edit, body, None)
}

pub fn simplify_closure_with_warnings(
    edit: &mut CircuitEditor,
    body: NodeId,
    warnings: Option<&mut Vec<crate::SimplifyWarning>>,
) -> Result<NodeId, Error> {
    Rewrite::translating_expressions(SimplifyArgument(crate::casts::SimplifyDefault { warnings }))
        .apply(edit, body)
}

struct SimplifyArgument<'a>(crate::casts::SimplifyDefault<'a>);
impl Transformation for SimplifyArgument<'_> {
    fn preorder(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        crate::casts::visit_expression(edit, id)
    }

    fn postorder_rebuilt(
        &mut self,
        edit: &mut CircuitEditor,
        original: NodeId,
        id: NodeId,
    ) -> Result<NodeId, Error> {
        // A field or borrow in the operand can expose a literal to its parent's cast.
        let folded = self.0.postorder_rebuilt(edit, original, id)?;
        if folded != id {
            return Ok(folded);
        }
        let node = edit.node(id)?;
        match node.inner_kind() {
            Some(InnerKind::Expression(Expr::Apply | Expr::ApplyMethod)) if original != id => {
                edit.derive_unlocated_inner(id, [])
            }
            Some(InnerKind::Expression(Expr::Flatmap)) => {
                crate::flatmap::rebuild(edit, original, id)
            }
            Some(InnerKind::Expression(kind @ (Expr::Borrow | Expr::Deref))) => {
                let source = edit.node(node.edge("expression")?)?;
                match (kind, source.inner_kind()) {
                    (Expr::Borrow, Some(InnerKind::Expression(Expr::Deref)))
                    | (Expr::Deref, Some(InnerKind::Expression(Expr::Borrow))) => {
                        source.edge("expression")
                    }
                    _ => Ok(id),
                }
            }
            Some(InnerKind::Expression(Expr::Field)) => {
                let row = edit.expression(node.edge("expression")?)?;
                let index = node
                    .data("fieldNo")
                    .and_then(|n| n.as_u64())
                    .and_then(|n| usize::try_from(n).ok())
                    .ok_or_else(|| Error::new("expected a tuple field index"))?;
                match edit.expr_kind(row)? {
                    Expr::Clone => {
                        let value = edit.node(row.into())?.edge("expression")?;
                        edit.derive_inner(id, [("expression", Field::Reference(value))])
                    }
                    Expr::If => {
                        let branch = edit.node(row.into())?;
                        let condition = branch.edge("condition")?;
                        let positive = edit.expression(branch.edge("positive")?)?;
                        let negative = branch.field("negative").and_then(Field::node_id);
                        let positive = edit.tuple_field(positive, index)?;
                        let positive = edit.clone_if_needed(positive)?;
                        let mut fields = vec![
                            ("condition", Field::Reference(condition)),
                            ("positive", Field::Reference(positive.into())),
                        ];
                        if let Some(negative) = negative {
                            let value = edit.tuple_field(edit.expression(negative)?, index)?;
                            fields.push((
                                "negative",
                                Field::Reference(edit.clone_if_needed(value)?.into()),
                            ));
                        }
                        edit.make_expr(
                            row.into(),
                            Expr::If,
                            edit.expression_type(positive)?,
                            fields,
                        )
                        .map(Into::into)
                    }
                    Expr::Tuple | Expr::RawTuple => edit.tuple_field(row, index).map(Into::into),
                    _ if original != id => edit.tuple_field(row, index).map(Into::into),
                    _ => Ok(id),
                }
            }
            Some(InnerKind::Expression(Expr::IsNull)) => {
                let value = edit.expression(node.edge("expression")?)?;
                if edit.expr_kind(value)? == Expr::If {
                    let branch = edit.node(value.into())?;
                    let condition = branch.edge("condition")?;
                    let positive = edit.expression(branch.edge("positive")?)?;
                    let negative = edit.expression(branch.edge("negative")?)?;
                    let positive = edit.null_test(positive)?;
                    let negative = edit.null_test(negative)?;
                    return edit
                        .make_expr(
                            value.into(),
                            Expr::If,
                            edit.expression_type(positive)?,
                            [
                                ("condition", Field::Reference(condition)),
                                ("positive", Field::Reference(positive.into())),
                                ("negative", Field::Reference(negative.into())),
                            ],
                        )
                        .map(Into::into);
                }
                let simplified = edit.null_test(value)?;
                let result = edit.node(simplified.into())?;
                if matches!(
                    result.inner_kind(),
                    Some(InnerKind::Expression(Expr::IsNull))
                ) && result.edge("expression")? == value.into()
                {
                    Ok(id)
                } else {
                    Ok(simplified.into())
                }
            }
            Some(InnerKind::Expression(Expr::If)) => {
                let condition = edit.node(node.edge("condition")?)?;
                if matches!(
                    condition.inner_kind(),
                    Some(InnerKind::Expression(Expr::BoolLiteral))
                ) {
                    match condition.data("value").and_then(|v| v.as_bool()) {
                        Some(true) => return node.edge("positive"),
                        Some(false) => {
                            if let Some(branch) = node.field("negative").and_then(Field::node_id) {
                                return Ok(branch);
                            }
                            let ty = node.edge("type")?;
                            return edit.empty_origin_node(
                                id,
                                InnerKind::Expression(Expr::VoidLiteral),
                                [("type", Field::Reference(ty))],
                            );
                        }
                        None => (),
                    }
                }
                if let Some(negative) = node.field("negative").and_then(Field::node_id) {
                    let positive = node.edge("positive")?;
                    let literal = |id| -> Result<Option<bool>, Error> {
                        let value = edit.node(id)?;
                        Ok(matches!(
                            value.inner_kind(),
                            Some(InnerKind::Expression(Expr::BoolLiteral))
                        )
                        .then(|| value.data("value").and_then(|v| v.as_bool()))
                        .flatten())
                    };
                    if literal(positive)? == Some(true)
                        && literal(negative)? == Some(false)
                        && edit.same_type(
                            edit.expression_type(edit.expression(positive)?)?,
                            edit.expression_type(edit.expression(node.edge("condition")?)?)?,
                        )?
                    {
                        return node.edge("condition");
                    }
                }
                Ok(id)
            }
            Some(InnerKind::Expression(Expr::Clone)) => {
                let expression = edit.expression(node.edge("expression")?)?;
                match edit.expr_kind(expression)? {
                    Expr::Apply | Expr::ApplyMethod | Expr::Cast | Expr::If | Expr::Clone => {
                        Ok(expression.into())
                    }
                    _ => {
                        let result = edit.clone_if_needed(expression)?;
                        if result == expression {
                            Ok(result.into())
                        } else {
                            // Rebuilding a clone can already have returned its cloned child.
                            // Retain that node so sibling expressions still share the value.
                            Ok(id)
                        }
                    }
                }
            }
            Some(InnerKind::Expression(Expr::Cast)) => {
                let source = edit.expression(node.edge("source")?)?;
                let from = edit.expression_type(source)?;
                let to = edit.type_id(node.edge("type")?)?;
                if edit.same_type(from, to)? {
                    return Ok(source.into());
                }
                if edit.expr_kind(source)?.wire_name().ends_with("Literal")
                    && !edit.nullable(from)?
                    && edit.same_type_ignoring_nullability(from, to)?
                {
                    if matches!(
                        edit.type_kind(to)?,
                        TypeKind::Decimal
                            | TypeKind::Timestamp
                            | TypeKind::TimestampTz
                            | TypeKind::Date
                            | TypeKind::Time
                            | TypeKind::Binary
                            | TypeKind::ShortInterval
                            | TypeKind::LongInterval
                    ) {
                        let ty = edit.with_nullability(from, true)?;
                        return edit
                            .derive_inner(source.into(), [("type", Field::Reference(ty.into()))]);
                    }
                    let ty = match edit.type_kind(to)? {
                        TypeKind::Integer => edit.derive_unlocated_inner(to.into(), [])?,
                        TypeKind::Uuid => edit.uuid_type(to.into(), true)?.into(),
                        TypeKind::Bool => edit.empty_origin_node(
                            to.into(),
                            InnerKind::Type(TypeKind::Bool),
                            [
                                ("mayBeNull", Field::Data(true.into())),
                                ("code", Field::Data("BOOL".into())),
                            ],
                        )?,
                        TypeKind::Double | TypeKind::Real => {
                            edit.float_literal_type(to, true)?.into()
                        }
                        _ => to.into(),
                    };
                    return edit
                        .derive_unlocated_inner(source.into(), [("type", Field::Reference(ty))]);
                }
                Ok(id)
            }
            _ => Ok(id),
        }
    }
}
