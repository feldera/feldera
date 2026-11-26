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

fn has_ordered_index(circuit: &Circuit, id: NodeId) -> Result<bool, Error> {
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

/// Index functions that can contain a comparator declaration, in visitor order.
pub fn ordered_indexes(circuit: &Circuit) -> Result<Vec<NodeId>, Error> {
    let mut ordered = Vec::new();
    for id in circuit.operator_order(BodyOrder::Postorder)? {
        if has_ordered_index(circuit, id)? {
            ordered.push(id);
        }
    }
    Ok(ordered)
}

/// Rewrite the selected functions together so shared comparators have one path.
pub fn rewrite_index_functions(
    editor: &mut CircuitEditor,
    functions: &[NodeId],
    declaration_names: HashSet<String>,
) -> Result<Vec<NodeId>, Error> {
    let mut rewrite = Rewrite::new(DeclareComparators {
        declaration_names,
        paths: HashMap::new(),
    });
    functions
        .iter()
        .map(|&id| rewrite.apply(editor, id))
        .collect()
}

/// Move comparator definitions used by custom-ordered index keys into circuit declarations.
pub fn declare_comparators(circuit: Circuit) -> Result<Circuit, Error> {
    let operators = ordered_indexes(&circuit)?;
    let mut editor = CircuitEditor::new(circuit);
    let mut declaration_names = HashSet::new();
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
        declaration_names.insert(name.to_owned());
    }
    let functions = operators
        .iter()
        .map(|&id| editor.node(id)?.edge("function"))
        .collect::<Result<Vec<_>, _>>()?;
    let replacements = rewrite_index_functions(&mut editor, &functions, declaration_names)?;
    for ((id, original), replacement) in operators.into_iter().zip(functions).zip(replacements) {
        editor.clear_annotations(id)?;
        if replacement != original {
            editor.set_operator_field(id, "function", Field::Reference(replacement))?;
        }
    }
    editor.finish()
}
