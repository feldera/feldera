//! Traverse and rebuild the inner IR in the reference visitor's order.
use super::{rebuild_aggregate_cache, rebuild_result_type};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionKind, Field, InnerKind, Node, NodeId};
use std::collections::{HashMap, HashSet};

/// A rule can prune a subtree, descend into one child, or rewrite all of its children.
pub enum Visit {
    Keep,
    Follow(NodeId),
    Replace(NodeId),
    Children,
    /// Visit these fields in constructor order. Each child has its own
    /// translation cache because earlier children may introduce bindings.
    IndependentFields(&'static [&'static str]),
}

pub trait Transformation {
    fn preorder(&mut self, editor: &mut CircuitEditor, node: NodeId) -> Result<Visit, Error>;

    /// A declaration can introduce a binding between two independent field visits.
    /// Return its replacement here, or None to visit the child normally.
    fn independent_child(
        &mut self,
        _editor: &mut CircuitEditor,
        _parent: NodeId,
        _field: &str,
        _child: NodeId,
    ) -> Result<Option<NodeId>, Error> {
        Ok(None)
    }

    fn postorder(&mut self, _editor: &mut CircuitEditor, node: NodeId) -> Result<NodeId, Error> {
        Ok(node)
    }

    /// Some consumers retain the freshly constructed expression before the visitor's
    /// same-fields check. Request its constructor even when all operands were retained.
    fn rebuild_unchanged(&self, _node: NodeId) -> bool {
        false
    }

    /// Most rewrites simplify a rebuilt null test. Visitors that preserve the
    /// written test can use its location and its translated operand here.
    fn rebuild_null_test(
        &mut self,
        editor: &mut CircuitEditor,
        node: NodeId,
    ) -> Result<NodeId, Error> {
        rebuild_result_type(editor, node, Some(ExpressionKind::IsNull), true)
    }

    /// A projected row can change the field number as well as the receiver.
    fn rebuild_field(
        &mut self,
        editor: &mut CircuitEditor,
        _original: NodeId,
        rebuilt: NodeId,
        translating: bool,
    ) -> Result<NodeId, Error> {
        rebuild_result_type(editor, rebuilt, Some(ExpressionKind::Field), translating)
    }

    /// Some constructor rules retain a child of the original expression even
    /// after visiting it. Both operands remain available for that decision.
    fn postorder_rebuilt(
        &mut self,
        editor: &mut CircuitEditor,
        _original: NodeId,
        rebuilt: NodeId,
    ) -> Result<NodeId, Error> {
        self.postorder(editor, rebuilt)
    }
}

/// Rebuild changed expressions at each use, as the compiler visitor does.
/// An unchanged subtree can retain its original identity at every use.
pub struct Rewrite<T> {
    pub rule: T,
    translations: HashMap<NodeId, NodeId>,
    expression_translation: bool,
}

impl<T: Transformation> Rewrite<T> {
    pub fn new(rule: T) -> Self {
        Self {
            rule,
            translations: HashMap::new(),
            expression_translation: false,
        }
    }

    /// ExpressionTranslator shares replacements and retains each raw tuple's declared type.
    pub fn translating_expressions(rule: T) -> Self {
        Self {
            expression_translation: true,
            ..Self::new(rule)
        }
    }

    pub fn apply(&mut self, arena: &mut CircuitEditor, root: NodeId) -> Result<NodeId, Error> {
        enum Step {
            Visit(NodeId),
            Independent(NodeId, &'static str, NodeId),
            RestoreTranslations,
            Follow(NodeId),
            Rebuild(NodeId, Children, usize),
            RebuildFields(NodeId, &'static [&'static str], usize),
        }

        let mut pending = vec![Step::Visit(root)];
        let mut active = HashSet::new();
        let mut enclosing = Vec::new();
        // Child results stay in field order, including separate results for
        // repeated uses when the visitor rebuilds an expression at each use.
        let mut results = Vec::new();
        let mut edges = Vec::new();
        while let Some(step) = pending.pop() {
            let (id, result) = match step {
                Step::Independent(parent, field, child) => {
                    if let Some(result) =
                        self.rule.independent_child(arena, parent, field, child)?
                    {
                        results.push(result);
                    } else {
                        enclosing.push(std::mem::take(&mut self.translations));
                        pending.extend([Step::RestoreTranslations, Step::Visit(child)]);
                    }
                    continue;
                }
                Step::RestoreTranslations => {
                    self.translations = enclosing.pop().expect("enclosing translation cache");
                    continue;
                }
                Step::Visit(id) => {
                    if let Some(&result) = self.translations.get(&id) {
                        results.push(result);
                        continue;
                    }
                    if arena.node(id)?.inner_kind().is_none() {
                        return Err(Error::new("expected an inner root"));
                    }
                    if !active.insert(id) {
                        return Err(Error::new("cycle in expression rewrite"));
                    }
                    match self.rule.preorder(arena, id)? {
                        Visit::Keep => (id, id),
                        Visit::Replace(result) => (id, result),
                        Visit::Follow(child) => {
                            pending.push(Step::Follow(id));
                            pending.push(Step::Visit(child));
                            continue;
                        }
                        Visit::Children => {
                            let children =
                                Children::new(arena.node(id)?, self.expression_translation);
                            for (_, field) in children.fields() {
                                field.collect_inner_edges(&mut edges);
                            }
                            pending.push(Step::Rebuild(id, children, results.len()));
                            pending.extend(edges.drain(..).rev().map(Step::Visit));
                            continue;
                        }
                        Visit::IndependentFields(names) => {
                            let node = arena.node(id)?;
                            pending.push(Step::RebuildFields(id, names, results.len()));
                            for &name in names.iter().rev() {
                                if let Some(field) = node.field(name) {
                                    field.collect_inner_edges(&mut edges);
                                    pending.extend(
                                        edges
                                            .drain(..)
                                            .rev()
                                            .map(|child| Step::Independent(id, name, child)),
                                    );
                                }
                            }
                            continue;
                        }
                    }
                }
                Step::Follow(id) => (id, results.pop().expect("followed child")),
                Step::Rebuild(id, children, start) => {
                    let rebuilt = self.rebuild(arena, id, children, results.drain(start..))?;
                    (id, self.rule.postorder_rebuilt(arena, id, rebuilt)?)
                }
                Step::RebuildFields(id, names, start) => {
                    let node = arena.node(id)?;
                    let fields = names
                        .iter()
                        .filter_map(|&name| Some((name, node.field(name)?)));
                    let changed = changed_fields(fields, results.drain(start..))?;
                    let rebuilt = if changed.is_empty() && !self.rule.rebuild_unchanged(id) {
                        id
                    } else if matches!(
                        node.inner_kind(),
                        Some(InnerKind::Expression(ExpressionKind::Block))
                    ) {
                        let block = arena.derive_unlocated_inner(id, changed)?;
                        rebuild_result_type(arena, block, Some(ExpressionKind::Block), false)?
                    } else {
                        arena.derive_inner(id, changed)?
                    };
                    (id, self.rule.postorder_rebuilt(arena, id, rebuilt)?)
                }
            };
            active.remove(&id);
            if result == id || self.expression_translation {
                self.translations.insert(id, result);
            }
            results.push(result);
        }
        Ok(results.pop().expect("rewritten root"))
    }

    fn rebuild(
        &mut self,
        arena: &mut CircuitEditor,
        id: NodeId,
        children: Children,
        results: impl Iterator<Item = NodeId>,
    ) -> Result<NodeId, Error> {
        let changed = changed_fields(children.fields(), results)?;
        if changed.is_empty() && !self.rule.rebuild_unchanged(id) {
            return Ok(id);
        }
        let expr = children.expression;
        let transformed = if self.expression_translation
            && matches!(
                expr,
                Some(ExpressionKind::Apply | ExpressionKind::ApplyMethod | ExpressionKind::Block)
            ) {
            arena.derive_unlocated_inner(id, changed)?
        } else {
            arena.derive_inner(id, changed)?
        };
        if children.aggregate_list {
            rebuild_aggregate_cache(arena, transformed)
        } else if children.initialized_let {
            let let_node = arena.node(transformed)?;
            let initializer = let_node.edge("initializer")?;
            let ty = arena.node(initializer)?.edge("type")?;
            arena.derive_inner(
                transformed,
                [
                    ("type", Field::Reference(ty)),
                    ("mutable", Field::Data(false.into())),
                ],
            )
        } else if expr == Some(ExpressionKind::Field) {
            self.rule
                .rebuild_field(arena, id, transformed, self.expression_translation)
        } else if expr == Some(ExpressionKind::IsNull) {
            self.rule.rebuild_null_test(arena, transformed)
        } else {
            rebuild_result_type(arena, transformed, expr, self.expression_translation)
        }
    }
}

fn changed_fields<'a, 'b>(
    fields: impl Iterator<Item = (&'a str, &'b Field)>,
    mut results: impl Iterator<Item = NodeId>,
) -> Result<Vec<(&'a str, Field)>, Error> {
    let mut changed = Vec::new();
    for (name, field) in fields {
        let mut field = field.clone();
        if field.map_inner_edges(&mut |_| Ok(results.next().expect("visited edge")))? {
            changed.push((name, field));
        }
    }
    Ok(changed)
}

struct Children {
    node: Node,
    expression: Option<ExpressionKind>,
    aggregate_list: bool,
    initialized_let: bool,
}

impl Children {
    fn new(node: &Node, translating: bool) -> Self {
        Self {
            node: node.clone(),
            expression: match node.inner_kind() {
                Some(InnerKind::Expression(kind)) => Some(*kind),
                _ => None,
            },
            aggregate_list: translating
                && matches!(node.inner_kind(), Some(InnerKind::AggregateList)),
            initialized_let: node.class() == "DBSPLetStatement"
                && node.field("initializer").and_then(Field::node_id).is_some(),
        }
    }

    fn fields(&self) -> impl Iterator<Item = (&str, &Field)> {
        let derived_type = self.initialized_let
            || matches!(
                self.expression,
                Some(
                    ExpressionKind::Closure
                        | ExpressionKind::Clone
                        | ExpressionKind::If
                        | ExpressionKind::Let
                        | ExpressionKind::Lazy
                        | ExpressionKind::HandleError
                        | ExpressionKind::Some
                        | ExpressionKind::CustomOrd
                        | ExpressionKind::Borrow
                        | ExpressionKind::Field
                        | ExpressionKind::RawTuple
                        | ExpressionKind::Unwrap
                )
            );
        // originalStruct links to the printer's source record outside the expression tree.
        self.node.fields().filter(move |(name, _)| {
            *name != "originalStruct"
                && !(self.aggregate_list && *name == "cachedEmptySetResult")
                && !(derived_type && *name == "type")
        })
    }
}
