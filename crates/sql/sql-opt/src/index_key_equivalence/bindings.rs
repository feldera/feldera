//! Pair local declarations while comparing expressions under alpha-renaming.
use super::{optional_child, same_type};
use feldera_sql_ir::{CircuitEditor, Error, Node, NodeId};

/// Each pending comparison sees the bindings in scope at that expression.
/// Two branches may use the same variable names for different declarations.
pub(super) type Scope = Option<usize>;
pub(super) enum Work {
    Compare(NodeId, NodeId, Scope),
    Leave(NodeId, NodeId),
}

#[derive(Default)]
pub(super) struct Bindings<'a> {
    pairs: Vec<(&'a str, &'a str, Scope)>,
}

fn name<'a>(node: &'a Node, field: &str) -> Result<&'a str, Error> {
    node.data(field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::new(format!("missing {field} on {}", node.class())))
}

impl<'a> Bindings<'a> {
    fn bind(&mut self, left: &'a str, right: &'a str, parent: Scope) -> Scope {
        let index = self.pairs.len();
        self.pairs.push((left, right, parent));
        Some(index)
    }

    pub fn variables(
        &self,
        mut scope: Scope,
        left: &Node,
        right: &Node,
    ) -> Result<Option<bool>, Error> {
        let left = name(left, "variable")?;
        let right = name(right, "variable")?;
        while let Some(index) = scope {
            let (l, r, parent) = self.pairs[index];
            if l == left || r == right {
                return Ok(Some(l == left && r == right));
            }
            scope = parent;
        }
        Ok(None)
    }

    pub fn let_expression(
        &mut self,
        edit: &'a CircuitEditor,
        left: &Node,
        right: &Node,
        scope: Scope,
        pending: &mut Vec<Work>,
    ) -> Result<(), Error> {
        let local = self.bind(
            name(edit.node(left.edge("variable")?)?, "variable")?,
            name(edit.node(right.edge("variable")?)?, "variable")?,
            scope,
        );
        pending.push(Work::Compare(
            left.edge("consumer")?,
            right.edge("consumer")?,
            local,
        ));
        pending.push(Work::Compare(
            left.edge("initializer")?,
            right.edge("initializer")?,
            scope,
        ));
        Ok(())
    }

    pub fn closure(
        &mut self,
        edit: &'a CircuitEditor,
        left: &Node,
        right: &Node,
        mut scope: Scope,
        pending: &mut Vec<Work>,
    ) -> Result<bool, Error> {
        let left_params = left.edges("parameters")?;
        let right_params = right.edges("parameters")?;
        if left_params.len() != right_params.len() {
            return Ok(false);
        }
        for (l, r) in left_params.into_iter().zip(right_params) {
            let l = edit.node(l)?;
            let r = edit.node(r)?;
            if !same_type(edit, l, r)? {
                return Ok(false);
            }
            scope = self.bind(name(l, "name")?, name(r, "name")?, scope);
        }
        pending.push(Work::Compare(
            left.edge("body")?,
            right.edge("body")?,
            scope,
        ));
        Ok(true)
    }

    pub fn block(
        &mut self,
        edit: &'a CircuitEditor,
        left: &Node,
        right: &Node,
        mut scope: Scope,
        pending: &mut Vec<Work>,
    ) -> Result<Option<bool>, Error> {
        let statements = |block: &Node| -> Result<Vec<&Node>, Error> {
            let mut result = Vec::new();
            for id in block.edges("contents")? {
                let node = edit.node(id)?;
                if node.class() != "DBSPComment" {
                    result.push(node);
                }
            }
            Ok(result)
        };
        let left_statements = statements(left)?;
        let right_statements = statements(right)?;
        if left_statements.len() != right_statements.len() {
            return Ok(Some(false));
        }
        // Process initializers before consumers, even if their names shadow.
        let start = pending.len();
        for (l, r) in left_statements.into_iter().zip(right_statements) {
            if l.class() != r.class() {
                return Ok(Some(false));
            }
            match l.class() {
                "DBSPLetStatement" => {
                    if l.data("mutable") != r.data("mutable")
                        || !optional_child(l, r, "initializer", scope, pending)?
                    {
                        return Ok(Some(false));
                    }
                    scope = self.bind(name(l, "variable")?, name(r, "variable")?, scope);
                }
                "DBSPExpressionStatement" => pending.push(Work::Compare(
                    l.edge("expression")?,
                    r.edge("expression")?,
                    scope,
                )),
                _ => return Ok(None),
            }
        }
        if !optional_child(left, right, "lastExpression", scope, pending)? {
            return Ok(Some(false));
        }
        pending[start..].reverse();
        Ok(Some(true))
    }
}
