//! Rebuild a normalized expression tree with the lazy cells selected by value numbering.
use super::{inner, plan::RewritePlan};
use crate::binding_placement::{bind_lazy_expression, lazy_statements, read_lazy_value};
use feldera_sql_expr::{Rewrite, Transformation, Visit};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionKind as Expr, Field, InnerKind, NodeId};
use std::collections::HashMap;

struct Materializer<'a> {
    plan: &'a RewritePlan,
    // A cell's first occurrence owns its variable and the translated initializer.
    cells: HashMap<u64, [NodeId; 2]>,
    next_name: &'a mut u64,
    unsupported_loop: bool,
}

impl RewritePlan {
    /// The reference translator reports an unimplemented FOR after visiting its children.
    pub fn apply(
        &self,
        edit: &mut CircuitEditor,
        tree: NodeId,
        next_name: &mut u64,
    ) -> Result<Option<NodeId>, Error> {
        let materializer = Materializer {
            plan: self,
            cells: HashMap::new(),
            next_name,
            unsupported_loop: false,
        };
        let mut rewrite = Rewrite::translating_expressions(materializer);
        let result = rewrite.apply(edit, tree)?;
        Ok((!rewrite.rule.unsupported_loop).then_some(result))
    }
}

impl Materializer<'_> {
    fn bindings(&self, cells: &[u64]) -> Result<Vec<[NodeId; 2]>, Error> {
        cells
            .iter()
            .map(|cell| {
                self.cells
                    .get(cell)
                    .copied()
                    .ok_or_else(|| Error::new("CSE initializer precedes its first read"))
            })
            .collect()
    }

    fn bind_scope(
        &self,
        edit: &mut CircuitEditor,
        before: NodeId,
        rebuilt: NodeId,
    ) -> Result<NodeId, Error> {
        let scope = inner(before);
        let Some(anchors) = self.plan.insertions.get(&scope) else {
            return Ok(rebuilt);
        };
        let node = edit.node(rebuilt)?;
        match node.inner_kind() {
            Some(InnerKind::Expression(Expr::Block)) => {
                let mut contents = Vec::new();
                let statements = node.edges("contents")?;
                for (original, statement) in edit
                    .node(before)?
                    .edges("contents")?
                    .into_iter()
                    .zip(statements)
                {
                    contents.push(Field::Reference(statement));
                    let anchor = inner(original);
                    if let Some(cells) = anchors.get(&anchor) {
                        let bindings = self.bindings(cells)?;
                        contents.extend(
                            lazy_statements(edit, &bindings)?
                                .into_iter()
                                .map(Field::Reference),
                        );
                    }
                }
                edit.derive_unlocated_inner(rebuilt, [("contents", Field::Array(contents))])
            }
            Some(InnerKind::Expression(kind @ (Expr::Closure | Expr::Let))) => {
                let kind = *kind;
                let field = if kind == Expr::Closure {
                    "body"
                } else {
                    "consumer"
                };
                let body = edit.expression(node.edge(field)?)?;
                let bindings = self.bindings(
                    anchors
                        .get(&scope)
                        .ok_or_else(|| Error::new("unknown CSE binding scope"))?,
                )?;
                let bound = bind_lazy_expression(edit, &bindings, body)?;
                if kind == Expr::Let {
                    let variable = edit.expression(edit.node(rebuilt)?.edge("variable")?)?;
                    let initializer = edit.expression(edit.node(rebuilt)?.edge("initializer")?)?;
                    return Ok(edit.let_expression(variable, initializer, bound)?.into());
                }
                let parameters = edit
                    .node(rebuilt)?
                    .edges("parameters")?
                    .into_iter()
                    .map(|p| edit.node(p)?.edge("type"))
                    .collect::<Result<Vec<_>, _>>()?;
                let ty = edit.closure_type(bound, &parameters)?;
                edit.derive_inner(
                    rebuilt,
                    [
                        (field, Field::Reference(bound.into())),
                        ("type", Field::Reference(ty)),
                    ],
                )
            }
            _ => Err(Error::new("invalid CSE binding scope")),
        }
    }
}

impl Transformation for Materializer<'_> {
    fn preorder(&mut self, edit: &mut CircuitEditor, node: NodeId) -> Result<Visit, Error> {
        if self.unsupported_loop {
            return Ok(Visit::Keep);
        }
        Ok(match edit.node(node)?.inner_kind() {
            Some(InnerKind::Type(_) | InnerKind::Parameter) => Visit::Keep,
            Some(InnerKind::Expression(Expr::Let)) => {
                Visit::IndependentFields(&["initializer", "consumer"])
            }
            _ => Visit::Children,
        })
    }

    fn rebuild_unchanged(&self, node: NodeId) -> bool {
        matches!(node, NodeId::Inner(id) if self.plan.reads.get(&id).is_some_and(|read| read.first == id))
    }

    fn rebuild_null_test(
        &mut self,
        edit: &mut CircuitEditor,
        node: NodeId,
    ) -> Result<NodeId, Error> {
        edit.rebuild_written_null_test(node)
    }

    fn postorder_rebuilt(
        &mut self,
        edit: &mut CircuitEditor,
        original: NodeId,
        rebuilt: NodeId,
    ) -> Result<NodeId, Error> {
        let id = inner(original);
        if self.unsupported_loop {
            return Ok(original);
        }
        let constructor = match edit.node(original)?.inner_kind() {
            Some(InnerKind::Expression(Expr::For)) => {
                self.unsupported_loop = true;
                return Ok(original);
            }
            Some(InnerKind::Expression(Expr::Variant))
                if edit
                    .node(original)?
                    .data("isSqlNull")
                    .and_then(|v| v.as_bool())
                    == Some(true) =>
            {
                edit.derive_unlocated_inner(original, [("isSqlNull", Field::Data(false.into()))])?
            }
            Some(InnerKind::Expression(Expr::Flatmap)) => {
                feldera_sql_expr::rebuild_flatmap(edit, original, rebuilt)?
            }
            Some(InnerKind::Expression(Expr::Clone)) if original == rebuilt => {
                let child = edit.expression(edit.node(original)?.edge("expression")?)?;
                if edit.expr_kind(child)? == Expr::Clone {
                    child.into()
                } else {
                    original
                }
            }
            Some(InnerKind::Expression(Expr::Let)) if original != rebuilt => {
                let node = edit.node(rebuilt)?;
                let variable = edit.expression(node.edge("variable")?)?;
                let init = edit.expression(node.edge("initializer")?)?;
                let consumer = edit.expression(node.edge("consumer")?)?;
                edit.let_expression(variable, init, consumer)?.into()
            }
            _ if original != rebuilt && edit.node(original)?.class() == "DBSPLetStatement" => {
                let node = edit.node(rebuilt)?;
                let init = edit.expression(node.edge("initializer")?)?;
                let name = node
                    .data("variable")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| Error::new("CSE let statement requires a name"))?
                    .to_owned();
                let statement = edit.let_statement(&name, init)?;
                let mutable = edit
                    .node(original)?
                    .field("mutable")
                    .cloned()
                    .unwrap_or(Field::Data(false.into()));
                edit.derive_inner(statement, [("mutable", mutable)])?
            }
            _ => rebuilt,
        };
        let rebuilt = self.bind_scope(edit, original, constructor)?;
        let Some(read) = self.plan.reads.get(&id) else {
            return Ok(rebuilt);
        };
        let source = if id == read.first {
            edit.node(original)?.edge("type")?
        } else {
            self.cells
                .get(&read.first)
                .ok_or_else(|| Error::new("CSE read before its initializer"))?[0]
        };
        let [variable, value] = read_lazy_value(edit, source, read.borrowed, self.next_name)?;
        if id == read.first {
            self.cells.insert(id, [variable.into(), rebuilt]);
        }
        Ok(value.into())
    }
}
