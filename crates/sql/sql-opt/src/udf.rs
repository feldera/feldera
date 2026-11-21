//! Inline SQL function calls in operator expressions. Function declarations are
//! the source of the bodies and retain their original definitions.
use crate::expression_visitor::{rewrite_expressions, Context};
use crate::fanout::expensive;
use crate::function_calls::{contains_now, function_name};
use feldera_sql_expr::{reduce_closure_body, Rewrite, Transformation, Visit};
use feldera_sql_ir::{Circuit, CircuitEditor, Error, Field, InnerKind, NodeId};
use std::collections::HashMap;

struct Function {
    body: NodeId,
    parameters: Vec<NodeId>,
    expensive: Option<bool>,
}

struct Inline<'a>(&'a mut HashMap<String, Function>);

impl Transformation for Inline<'_> {
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
        let node = edit.node(id)?;
        let Some(name) = function_name(edit.circuit(), node)? else {
            return Ok(id);
        };
        let Some(function) = self.0.get_mut(name.as_ref()) else {
            return Ok(id);
        };
        let arguments = node.edges("arguments")?;
        let costly = match function.expensive {
            Some(costly) => costly,
            None => {
                let closure = edit.closure_with_parameters(
                    edit.expression(function.body)?,
                    &function.parameters,
                )?;
                let costly = expensive(edit.circuit(), closure.into())?;
                function.expensive = Some(costly);
                costly
            }
        };
        if costly && !contains_now(edit.circuit(), &arguments)? {
            return Ok(id);
        }
        reduce_closure_body(edit, function.body, &function.parameters, &arguments)
    }
}

pub fn inline_udfs(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let mut functions = HashMap::new();
    for id in circuit.root().edges("declarations")? {
        let decl = circuit.node(id).expect("validated declaration");
        let item = circuit.node(decl.edge("item")?).expect("declared item");
        if item.class() != "DBSPFunctionItem" {
            continue;
        }
        let function = circuit
            .node(item.edge("function")?)
            .expect("declared function");
        let Some(body) = function.field("body").and_then(Field::node_id) else {
            continue;
        };
        let name = function
            .data("name")
            .and_then(|n| n.as_str())
            .ok_or_else(|| Error::new("declared function requires a name"))?;
        functions.insert(
            name.to_owned(),
            Function {
                body,
                parameters: function.edges("parameters")?,
                expensive: None,
            },
        );
    }
    if functions.is_empty() {
        return Ok(circuit);
    }
    rewrite_expressions(circuit, next, |edit, root, context, _| {
        if matches!(context, Context::Declaration) {
            return Ok(root);
        }
        Rewrite::translating_expressions(Inline(&mut functions)).apply(edit, root)
    })
}
