use feldera_sql_expr::{contains_type, Rewrite, Transformation, Visit};
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionKind as Expr, Field, InnerKind, NodeId,
    OperatorKind, TypeKind,
};

use std::collections::{HashMap, HashSet};

#[derive(Default)]
struct DeclareComparators {
    paths: HashMap<NodeId, NodeId>,
    declaration_names: HashSet<String>,
}

impl Transformation for DeclareComparators {
    fn preorder(&mut self, editor: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        let node = editor.node(id)?;
        match node.inner_kind() {
            Some(InnerKind::Type(_) | InnerKind::Expression(Expr::EqualityComparator)) => {
                Ok(Visit::Keep)
            }
            Some(InnerKind::Expression(
                Expr::DirectComparator | Expr::NoComparator | Expr::FieldComparator,
            )) => {
                if let Some(&path) = self.paths.get(&id) {
                    return Ok(Visit::Replace(path));
                }
                let ty = node.edge("type")?;
                let name = editor
                    .node(ty)?
                    .data("name")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| Error::new("comparator type has no struct name"))?
                    .to_owned();
                if self.declaration_names.insert(name.clone()) {
                    let item = editor.inner_at(
                        id,
                        InnerKind::Other("DBSPComparatorItem".into()),
                        [("expression", Field::Reference(id))],
                    )?;
                    editor.declare_item(item)?;
                }
                let reference = editor.named_path(editor.type_id(ty)?, &name)?.into();
                self.paths.insert(id, reference);
                Ok(Visit::Replace(reference))
            }
            _ => Ok(Visit::Children),
        }
    }
}

pub(super) fn has_ordered_index(circuit: &Circuit, id: NodeId) -> Result<bool, Error> {
    let op = circuit
        .node(id)
        .ok_or_else(|| Error::new("unknown index operator"))?;
    if !matches!(
        op.operator_kind(),
        Some(OperatorKind::MapIndex | OperatorKind::FlatMapIndex)
    ) {
        return Ok(false);
    }
    contains_type(circuit, op.edge("outputType")?, TypeKind::WithCustomOrd)
}

/// Move comparator definitions used by custom-ordered index keys into circuit declarations.
pub fn declare_comparators(circuit: Circuit) -> Result<Circuit, Error> {
    let operators = circuit.operator_order(BodyOrder::Postorder)?;
    let mut editor = CircuitEditor::new(circuit);
    let mut rule = DeclareComparators::default();
    for declaration in editor.circuit().root().edges("declarations")? {
        let item = editor.node(editor.node(declaration)?.edge("item")?)?;
        let (owner, field) = match item.class() {
            "DBSPComparatorItem" => (editor.node(item.edge("expression")?)?.edge("type")?, "name"),
            "DBSPStaticItem" => (item.edge("expression")?, "name"),
            "DBSPFunctionItem" => (item.edge("function")?, "name"),
            "DBSPStructItem" => (item.edge("type")?, "hashName"),
            _ => return Err(Error::new("unknown circuit declaration item")),
        };
        let name = editor
            .node(owner)?
            .data(field)
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::new("circuit declaration has no name"))?;
        rule.declaration_names.insert(name.to_owned());
    }
    let mut rewrite = Rewrite::new(rule);
    for id in operators {
        let op = editor.node(id)?;
        if has_ordered_index(editor.circuit(), id)? {
            let function = op.edge("function")?;
            let replacement = rewrite.apply(&mut editor, function)?;
            editor.clear_annotations(id)?;
            if replacement != function {
                editor.set_operator_field(id, "function", Field::Reference(replacement))?;
            }
        }
    }
    editor.finish()
}
