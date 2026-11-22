//! Evaluate children before their parents, with lexical bindings in effect at each read.
use super::{
    AnalysisError, Interpreter, Merge, Scope, Source, preserves_cast, report::Finding, text,
};
use crate::lexical_scope::expression_children;
use feldera_sql_ir::{Error, ExpressionKind as Expr, Field, InnerKind, Node, NodeId};
use std::collections::HashSet;
use std::rc::Rc;

enum Visit<'a> {
    Enter(NodeId),
    Finish(NodeId),
    // A statement publishes its initializer by name. LET expressions publish
    // it at the declaration's variable node; other same-name reads are masked.
    Bind(&'a str, Option<NodeId>),
    BindLet(NodeId, NodeId),
    Restore(usize),
}

impl<'a> Interpreter<'a> {
    fn value(&self, id: NodeId) -> Result<Source, AnalysisError> {
        self.values
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::new(format!("lineage read {id:?} before evaluating it")).into())
    }

    pub(super) fn eval(
        &mut self,
        root: NodeId,
        mut scope: Scope<'a>,
    ) -> Result<Source, AnalysisError> {
        let mut pending = vec![Visit::Enter(root)];
        let mut active = HashSet::new();
        let mut undo = Vec::new();
        while let Some(step) = pending.pop() {
            let id = match step {
                Visit::Enter(id) => id,
                Visit::Finish(id) => {
                    let value = self.result(id, self.edit.node(id)?, &scope)?;
                    self.values.insert(id, value);
                    active.remove(&id);
                    continue;
                }
                Visit::Bind(name, init) => {
                    let value = init
                        .map(|id| self.value(id))
                        .transpose()?
                        .unwrap_or_default();
                    undo.push((name, scope.insert(name, value)));
                    continue;
                }
                Visit::BindLet(variable, init) => {
                    let value = self.value(init)?;
                    self.values.insert(variable, value);
                    let name = text(self.edit.node(variable)?, "variable")?;
                    undo.push((name, scope.remove(name)));
                    continue;
                }
                Visit::Restore(len) => {
                    while undo.len() > len {
                        let (name, prior) = undo.pop().expect("binding to restore");
                        if let Some(value) = prior {
                            scope.insert(name, value);
                        } else {
                            scope.remove(name);
                        }
                    }
                    continue;
                }
            };
            if self.values.contains_key(&id) {
                continue;
            }
            if !active.insert(id) {
                return Err(Error::new("cycle in lineage expression").into());
            }
            let node = self.edit.node(id)?;
            pending.push(Visit::Finish(id));
            match node.inner_kind() {
                Some(InnerKind::Expression(Expr::Closure)) => {
                    pending.push(Visit::Restore(undo.len()));
                    for id in node.edges("parameters")? {
                        let name = text(self.edit.node(id)?, "name")?;
                        undo.push((name, scope.remove(name)));
                    }
                    pending.push(Visit::Enter(node.edge("body")?));
                }
                Some(InnerKind::Expression(Expr::Let)) => {
                    let init = node.edge("initializer")?;
                    pending.push(Visit::Restore(undo.len()));
                    pending.push(Visit::Enter(node.edge("consumer")?));
                    pending.push(Visit::BindLet(node.edge("variable")?, init));
                    pending.push(Visit::Enter(init));
                }
                Some(InnerKind::Expression(Expr::Block)) => {
                    pending.push(Visit::Restore(undo.len()));
                    if let Some(last) = node.field("lastExpression").and_then(Field::node_id) {
                        pending.push(Visit::Enter(last));
                    }
                    for id in node.edges("contents")?.into_iter().rev() {
                        let stmt = self.edit.node(id)?;
                        if stmt.class() == "DBSPLetStatement" {
                            let init = stmt.field("initializer").and_then(Field::node_id);
                            pending.push(Visit::Bind(text(stmt, "variable")?, init));
                            if let Some(init) = init {
                                pending.push(Visit::Enter(init));
                            }
                        } else {
                            pending.push(Visit::Enter(id));
                        }
                    }
                }
                Some(InnerKind::Expression(Expr::If)) => {
                    if let Some(negative) = node.field("negative").and_then(Field::node_id) {
                        pending.push(Visit::Enter(negative));
                    }
                    pending.push(Visit::Enter(node.edge("positive")?));
                    pending.push(Visit::Enter(node.edge("condition")?));
                }
                Some(InnerKind::Expression(Expr::Return | Expr::Assignment)) => {}
                _ => pending.extend(
                    expression_children(node)
                        .into_iter()
                        .rev()
                        .map(Visit::Enter),
                ),
            }
        }
        self.value(root)
    }

    fn result(
        &mut self,
        id: NodeId,
        node: &Node,
        scope: &Scope<'a>,
    ) -> Result<Source, AnalysisError> {
        let child = |name| self.value(node.edge(name)?);
        let reporting = self.comparisons.is_some();
        let intersection = if reporting {
            Merge::CommonSources
        } else {
            Merge::CopiedColumns
        };
        Ok(match node.inner_kind() {
            Some(InnerKind::Type(_)) => return Ok(Source::Unknown),
            Some(InnerKind::Expression(Expr::Return | Expr::Assignment)) => {
                self.unhandled = true;
                Source::Unknown
            }
            Some(InnerKind::Expression(Expr::VariablePath)) => scope
                .get(text(node, "variable")?)
                .cloned()
                .unwrap_or_default(),
            Some(InnerKind::Expression(Expr::Borrow)) => {
                let value = child("expression")?;
                if reporting && matches!(value, Source::Unknown) {
                    Source::Unknown
                } else {
                    Source::Ref(Rc::new(value))
                }
            }
            Some(InnerKind::Expression(Expr::Deref)) => match child("expression")? {
                Source::Ref(inner) => (*inner).clone(),
                _ => Source::Unknown,
            },
            Some(InnerKind::Expression(
                Expr::Clone | Expr::Some | Expr::Unwrap | Expr::UnwrapCustomOrd,
            )) => child("expression")?,
            Some(InnerKind::Expression(Expr::HandleError)) => child("source")?,
            Some(InnerKind::Expression(Expr::Cast)) => {
                let source = child("source")?;
                if preserves_cast(self.edit, node, self.lossless_casts)? {
                    source
                } else {
                    Source::Unknown
                }
            }
            Some(InnerKind::Expression(Expr::Tuple | Expr::RawTuple)) => {
                if let Some(Field::Array(_)) = node.field("fields") {
                    let fields = node
                        .edges("fields")?
                        .into_iter()
                        .map(|f| self.value(f))
                        .collect::<Result<Vec<_>, _>>()?;
                    if reporting && fields.iter().all(|s| matches!(s, Source::Unknown)) {
                        Source::Unknown
                    } else {
                        Source::Tuple(fields.into())
                    }
                } else if self.track_constants {
                    Source::Constant(id)
                } else {
                    Source::Unknown
                }
            }
            Some(InnerKind::Expression(kind))
                if self.track_constants && kind.wire_name().ends_with("Literal") =>
            {
                Source::Constant(id)
            }
            Some(InnerKind::Expression(Expr::Field | Expr::CustomOrdField)) => {
                let field = node
                    .data("fieldNo")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| Error::new("missing field number"))?
                    as usize;
                match child("expression")? {
                    Source::Tuple(fields) => fields.get(field).cloned().unwrap_or_default(),
                    _ => Source::Unknown,
                }
            }
            Some(InnerKind::Expression(Expr::If)) => {
                let positive = child("positive")?;
                if let Some(negative) = node.field("negative").and_then(Field::node_id) {
                    positive.combine(&self.value(negative)?, self.edit, intersection)?
                } else {
                    Source::Unknown
                }
            }
            Some(InnerKind::Expression(Expr::Binary))
                if node.data("opcode").and_then(|v| v.as_str()) == Some("COALESCE") =>
            {
                child("left")?.combine(&child("right")?, self.edit, intersection)?
            }
            Some(InnerKind::Expression(Expr::Binary)) if reporting => {
                let opcode = text(node, "opcode")?;
                if matches!(
                    opcode,
                    "LT" | "GT" | "LTE" | "GTE" | "EQ" | "NEQ" | "IS_DISTINCT"
                ) {
                    let left = child("left")?;
                    let right = child("right")?;
                    if let (Source::Constant(literal), Source::Tables(columns))
                    | (Source::Tables(columns), Source::Constant(literal)) = (left, right)
                    {
                        if self.edit.node(literal)?.class().ends_with("Literal") {
                            self.comparisons
                                .as_mut()
                                .expect("report enabled")
                                .push(Finding::comparison(columns.as_ref(), id, literal)?);
                        }
                    }
                }
                Source::Unknown
            }
            Some(InnerKind::Expression(Expr::Closure)) => child("body")?,
            Some(InnerKind::Expression(Expr::Let)) => child("consumer")?,
            Some(InnerKind::Expression(Expr::Block)) => node
                .field("lastExpression")
                .and_then(Field::node_id)
                .map(|id| self.value(id))
                .transpose()?
                .unwrap_or_default(),
            _ => Source::Unknown,
        })
    }
}
