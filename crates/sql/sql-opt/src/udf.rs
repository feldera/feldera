//! Inline SQL function calls in operator expressions. Function declarations are
//! the source of the bodies and retain their original definitions.
use crate::expression_visitor::{rewrite_expressions, Context};
use crate::fanout::expensive;
use crate::function_calls::{contains_now, function_name};
use feldera_sql_expr::{reduce_closure_body, Rewrite, Transformation, Visit};
use feldera_sql_ir::{Circuit, CircuitEditor, Error, Field, InnerKind, NodeId};
use std::collections::{HashMap, HashSet};

struct Function {
    body: NodeId,
    parameters: Vec<NodeId>,
    expensive: Option<bool>,
}

impl Function {
    fn is_expensive(&mut self, edit: &mut CircuitEditor) -> Result<bool, Error> {
        if let Some(costly) = self.expensive {
            return Ok(costly);
        }
        let closure =
            edit.closure_with_parameters(edit.expression(self.body)?, &self.parameters)?;
        let costly = expensive(edit.circuit(), closure.into())?;
        self.expensive = Some(costly);
        Ok(costly)
    }
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
        if function.is_expensive(edit)? && !contains_now(edit.circuit(), &arguments)? {
            return Ok(id);
        }
        reduce_closure_body(edit, function.body, &function.parameters, &arguments)
    }
}

fn declarations(circuit: &Circuit) -> Result<HashMap<String, Function>, Error> {
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
    Ok(functions)
}

/// A costly UDF needs its call site only when an argument can read the clock.
/// The inventory covers executable operators except the built-in clock source,
/// whose only call is now(). A clock anywhere in the inventory is sufficient
/// to request the full graph and inspect the actual arguments.
pub fn may_inline_udfs(circuit: Circuit, calls: &[(String, usize)]) -> Result<bool, Error> {
    let functions = declarations(&circuit)?;
    let mut edit = CircuitEditor::new(circuit);
    let names: HashSet<_> = calls.iter().map(|(name, _)| name.as_str()).collect();
    let clock = calls
        .iter()
        .any(|(name, arity)| crate::is_clock_call(name, *arity));
    for (name, mut function) in functions {
        if name == "now"
            || (names.contains(name.as_str()) && (clock || !function.is_expensive(&mut edit)?))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn inline_udfs(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let mut functions = declarations(&circuit)?;
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
