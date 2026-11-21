//! Specialize a waterline closure for the two values of its validity flag.
use crate::expression_visitor::{rewrite_expressions, Context};
use feldera_sql_expr::{
    reduce_closure_body, simplify_closure_expression, Rewrite, Transformation, Visit,
};
use feldera_sql_ir::{
    Circuit, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, Node,
    NodeId, TypeId, TypeKind as Ty,
};

pub(super) fn has_waterline(node: &Node) -> bool {
    node.data("annotations")
        .and_then(|annotations| annotations.as_array())
        .is_some_and(|annotations| annotations.iter().any(|a| a["class"] == "Waterline"))
}

fn specialize_branch(
    edit: &mut CircuitEditor,
    body: NodeId,
    param: NodeId,
    row: ExpressionId,
    tuple: TypeId,
    valid: bool,
) -> Result<ExpressionId, Error> {
    let flag_type = edit.empty_origin_node(
        body,
        InnerKind::Type(Ty::Bool),
        [
            ("code", Field::Data("BOOL".into())),
            ("mayBeNull", Field::Data(false.into())),
        ],
    )?;
    let flag = edit.empty_origin_node(
        body,
        InnerKind::Expression(Expr::BoolLiteral),
        [
            ("type", Field::Reference(flag_type)),
            ("value", Field::Data(valid.into())),
        ],
    )?;
    let value = edit.dereference(row)?;
    let payload = edit.tuple_field(value, 1)?;
    let argument = edit.tuple_with_type(tuple, &[edit.expression(flag)?, payload])?;
    let argument = edit.borrow_value(argument)?;
    let branch = reduce_closure_body(edit, body, &[param], &[argument.into()])?;
    edit.expression(branch)
}

struct Specialize<'a> {
    next: &'a mut u64,
}

impl Transformation for Specialize<'_> {
    fn preorder(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        Ok(
            if matches!(edit.node(id)?.inner_kind(), Some(InnerKind::Type(_))) {
                Visit::Keep
            } else {
                Visit::Children
            },
        )
    }

    fn postorder(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<NodeId, Error> {
        let closure = edit.node(id)?;
        if !matches!(
            closure.inner_kind(),
            Some(InnerKind::Expression(Expr::Closure))
        ) {
            return Ok(id);
        }
        let body = closure.edge("body")?;
        let params = closure.edges("parameters")?;
        let [param] = params.as_slice() else {
            return Ok(id);
        };
        let reference = edit.type_id(edit.node(*param)?.edge("type")?)?;
        if edit.type_kind(reference)? != Ty::Ref {
            return Ok(id);
        }
        let tuple = edit.type_id(edit.node(reference.into())?.edge("type")?)?;
        if !matches!(edit.type_kind(tuple)?, Ty::Tuple | Ty::RawTuple) {
            return Ok(id);
        }
        let fields = edit.node(tuple.into())?.edges("tupFields")?;
        let [flag, _] = fields.as_slice() else {
            return Ok(id);
        };
        let flag = edit.type_id(*flag)?;
        if edit.type_kind(flag)? != Ty::Bool || edit.nullable(flag)? {
            return Ok(id);
        }

        let row = edit.variable(reference, self.next)?;
        let positive = specialize_branch(edit, body, *param, row, tuple, true)?;
        let negative = specialize_branch(edit, body, *param, row, tuple, false)?;
        let value = edit.dereference(row)?;
        let condition = edit.tuple_field(value, 0)?;
        let branch = edit.make_expr(
            id,
            Expr::If,
            edit.expression_type(positive)?,
            [
                ("condition", Field::Reference(condition.into())),
                ("positive", Field::Reference(positive.into())),
                ("negative", Field::Reference(negative.into())),
            ],
        )?;
        Ok(edit.closure_from_variables(branch, &[row])?.into())
    }
}

pub fn simplify_waterlines(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    rewrite_expressions(circuit, next, |edit, root, context, next| {
        let selected = match context {
            Context::Operator(id) => has_waterline(edit.node(id)?),
            Context::Inherited => true,
            Context::Declaration => false,
        };
        if !selected {
            return Ok(root);
        }
        let root = simplify_closure_expression(edit, root)?;
        Rewrite::translating_expressions(Specialize { next }).apply(edit, root)
    })
}
