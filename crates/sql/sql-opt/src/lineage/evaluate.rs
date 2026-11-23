//! Evaluate children before their parents, with lexical bindings in effect at each read.
use super::{
    AnalysisError, DeclarationValues, Interpreter, Merge, Source, preserves_cast, report::Finding,
    text,
};
use crate::lexical_scope::{References, expression_children, inspect_references};
use feldera_sql_ir::{Error, ExpressionKind as Expr, Field, InnerKind, Node, NodeId};
use std::collections::HashSet;
use std::rc::Rc;

enum Visit {
    Enter(NodeId),
    Finish(NodeId),
    BindStatement(NodeId, Option<NodeId>),
    BindLet(NodeId, NodeId),
}

impl<'a> Interpreter<'a> {
    fn value(&self, id: NodeId) -> Result<Source, AnalysisError<'static>> {
        self.values
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::new(format!("lineage read {id:?} before evaluating it")).into())
    }

    pub(super) fn eval(
        &mut self,
        closure: NodeId,
        mut declarations: DeclarationValues,
    ) -> Result<Source, AnalysisError<'a>> {
        // Resolve the entire closure before interpreting its first expression.
        // Shared reads must denote the same declaration at every use.
        let inspection = inspect_references(self.edit.circuit(), closure, false)?;
        if let Some(diagnostic) = inspection.diagnostic {
            return Err(AnalysisError::Binding(diagnostic));
        }
        let root = self.edit.node(closure)?.edge("body")?;
        let mut pending = vec![Visit::Enter(root)];
        let mut active = HashSet::new();
        while let Some(step) = pending.pop() {
            let id = match step {
                Visit::Enter(id) => id,
                Visit::Finish(id) => {
                    let value =
                        self.result(id, self.edit.node(id)?, &inspection.bindings, &declarations)?;
                    self.values.insert(id, value);
                    active.remove(&id);
                    continue;
                }
                Visit::BindStatement(statement, init) => {
                    let value = init
                        .map(|id| self.value(id))
                        .transpose()?
                        .unwrap_or_default();
                    declarations.insert(statement, value);
                    continue;
                }
                Visit::BindLet(variable, init) => {
                    let value = self.value(init)?;
                    self.values.insert(variable, value);
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
                    pending.push(Visit::Enter(node.edge("body")?));
                }
                Some(InnerKind::Expression(Expr::Let)) => {
                    let init = node.edge("initializer")?;
                    pending.push(Visit::Enter(node.edge("consumer")?));
                    pending.push(Visit::BindLet(node.edge("variable")?, init));
                    pending.push(Visit::Enter(init));
                }
                Some(InnerKind::Expression(Expr::Block)) => {
                    if let Some(last) = node.field("lastExpression").and_then(Field::node_id) {
                        pending.push(Visit::Enter(last));
                    }
                    for id in node.edges("contents")?.into_iter().rev() {
                        let stmt = self.edit.node(id)?;
                        if stmt.class() == "DBSPLetStatement" {
                            let init = stmt.field("initializer").and_then(Field::node_id);
                            pending.push(Visit::BindStatement(id, init));
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
        references: &References,
        declarations: &DeclarationValues,
    ) -> Result<Source, AnalysisError<'static>> {
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
            Some(InnerKind::Expression(Expr::VariablePath)) => references
                .get(&id)
                .and_then(|declaration| declarations.get(declaration))
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
