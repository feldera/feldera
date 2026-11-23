//! Name static initializers and construct the declarations referenced by generated code.
use crate::lexical_scope::expression_children;
use feldera_sql_expr::{Rewrite, Transformation, Visit, error_source_reports_position};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind, Field, InnerKind, NodeId,
};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

/// The classic printer renders the initializer and its type in the operator's context.
pub fn static_initializer(
    edit: &mut CircuitEditor,
    origin: NodeId,
    initializer: ExpressionId,
    rendered: &[u8],
) -> Result<ExpressionId, Error> {
    let digest = Sha256::digest(rendered);
    let prefix = u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 has 32 bytes"));
    let name = format!("STATIC_{prefix:016x}");
    let ty = edit.expression_type(initializer)?;
    edit.make_expr(
        origin,
        ExpressionKind::Static,
        ty,
        [
            ("initializer", Field::Reference(initializer.into())),
            ("name", Field::Data(name.into())),
        ],
    )
}

pub fn static_item(
    edit: &mut CircuitEditor,
    expression: ExpressionId,
) -> Result<(NodeId, ExpressionId), Error> {
    let id = expression.into();
    let name = edit
        .node(id)?
        .data("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::new("a static declaration needs a name"))?
        .to_owned();
    let ty = edit.expression_type(expression)?;
    let item = edit.inner_at(
        id,
        InnerKind::Other("DBSPStaticItem".into()),
        [("expression", Field::Reference(id))],
    )?;
    Ok((item, edit.named_path(ty, &name)?))
}

/// Static initializers share one error table across operators. Each initializer
/// gets a fresh expression translation, starting at the next table slot.
pub fn renumber_static_errors(
    edit: &mut CircuitEditor,
    initializer: ExpressionId,
    next: i32,
) -> Result<(ExpressionId, i32), Error> {
    let mut slots = StaticErrorSlots {
        next,
        repeated_visits: HashMap::new(),
        first_slot: HashMap::new(),
    };
    slots.assign(edit, initializer.into())?;
    let next = slots.next;
    let id = Rewrite::translating_expressions(slots).apply(edit, initializer.into())?;
    Ok((edit.expression(id)?, next))
}

struct StaticErrorSlots {
    next: i32,
    repeated_visits: HashMap<NodeId, i32>,
    first_slot: HashMap<NodeId, i32>,
}

impl StaticErrorSlots {
    /// The visitor reserves a slot on every visit to a handler, but retains the
    /// first translation. Count subsequent visits to a shared subtree in O(1).
    fn assign(&mut self, edit: &CircuitEditor, root: NodeId) -> Result<(), Error> {
        let mut pending = vec![(root, false)];
        let mut active = HashSet::new();
        while let Some((id, leaving)) = pending.pop() {
            if let Some(&visits) = self.repeated_visits.get(&id) {
                self.next = self.next.wrapping_add(visits);
                continue;
            }
            let node = edit.node(id)?;
            let children = expression_children(node);
            if !leaving {
                if !active.insert(id) {
                    return Err(Error::new("cycle in static initializer"));
                }
                pending.push((id, true));
                pending.extend(children.into_iter().rev().map(|child| (child, false)));
                continue;
            }
            active.remove(&id);
            // A translated LET skips both its initializer and consumer on later visits.
            let mut visits = 0_i32;
            if !matches!(
                node.inner_kind(),
                Some(InnerKind::Expression(ExpressionKind::Let))
            ) {
                for child in children {
                    visits = visits.wrapping_add(self.repeated_visits[&child]);
                }
            }
            if matches!(
                node.inner_kind(),
                Some(InnerKind::Expression(ExpressionKind::HandleError))
            ) {
                self.first_slot.insert(id, self.next);
                self.next = self.next.wrapping_add(1);
                visits = visits.wrapping_add(1);
            }
            self.repeated_visits.insert(id, visits);
        }
        Ok(())
    }
}

impl Transformation for StaticErrorSlots {
    fn preorder(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        Ok(
            if matches!(edit.node(id)?.inner_kind(), Some(InnerKind::Type(_))) {
                Visit::Keep
            } else {
                Visit::Children
            },
        )
    }

    fn postorder_rebuilt(
        &mut self,
        edit: &mut CircuitEditor,
        original: NodeId,
        rebuilt: NodeId,
    ) -> Result<NodeId, Error> {
        let Some(&slot) = self.first_slot.get(&original) else {
            return Ok(rebuilt);
        };
        let handler = edit.node(rebuilt)?;
        let source = edit.expression(handler.edge("source")?)?;
        let located = handler.data("hasSourcePosition").and_then(|v| v.as_bool()) == Some(true);
        let index = if located && error_source_reports_position(edit, source)? {
            slot
        } else {
            0
        };
        if handler.data("index").and_then(|v| v.as_i64()) == Some(i64::from(index)) {
            return Ok(rebuilt);
        }
        edit.derive_inner(rebuilt, [("index", Field::Data(index.into()))])
    }
}
