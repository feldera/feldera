//! Lower UNNEST to an outer row closure and an iterator that builds the output tuples.
mod collection;
mod input;
pub use input::trim_flatmap_input;
mod projection;

use feldera_sql_expr::{FlatmapColumns, SimplifyWarning};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, NodeId, TypeId,
};

fn link(id: impl Into<NodeId>) -> Field {
    Field::Reference(id.into())
}

/// The element type is retained from the collection. MAP owns a cached key/value tuple type.
pub fn lower_flatmap(
    edit: &mut CircuitEditor,
    site: NodeId,
    element_type: TypeId,
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<ExpressionId, Error> {
    let node = edit.node(site)?;
    let columns = FlatmapColumns::read(node)?;
    let input = edit.type_id(node.edge("inputRowType")?)?;
    let extract = node.edge("collectionExpression")?;
    let ordinal = node
        .field("ordinalityIndexType")
        .and_then(Field::node_id)
        .map(|id| edit.type_id(id))
        .transpose()?;
    let post = node
        .field("postProcess")
        .map(|_| node.edges("postProcess"))
        .transpose()?;
    let borrowed = edit.reference_type(input)?;
    let row = edit.variable(borrowed, next)?;
    let mut statements = Vec::new();
    let mut values = Vec::new();
    for (i, &column) in columns.passthrough.iter().enumerate() {
        let input = edit.dereference(row)?;
        let field = edit.tuple_field(input, column)?;
        let owned = edit.clone_if_needed(field)?;
        // Omitted columns still reserve their variable names in the Java prefix.
        let var = edit.variable(edit.expression_type(owned)?, next)?;
        if columns.output.contains(&i) {
            let name = edit
                .node(var.into())?
                .data("variable")
                .and_then(|v| v.as_str())
                .ok_or_else(|| Error::new("missing flatmap binding name"))?
                .to_owned();
            statements.push(link(edit.let_statement(&name, owned)?));
        }
        values.push(edit.clone_if_needed(var)?);
    }
    let element = projection::Element::new(edit, site, element_type, ordinal, next)?;
    values.extend(element.columns(edit, site, post.as_deref(), warnings)?);
    if values.len() != columns.width {
        return Err(Error::new(
            "flatmap column count differs from shuffle width",
        ));
    }
    let fields = columns
        .output
        .into_iter()
        .map(|index| values[index])
        .collect::<Vec<_>>();
    let tuple = edit.tuple_expr(site, false, &fields)?;
    let produce = edit.closure_from_variables(tuple, &[element.var])?;
    let items = collection::extract(edit, site, extract, row, &mut statements, warnings)?;
    let mut iter = edit.iterator_method(site, "into_iter", items, &[])?;
    if ordinal.is_some() {
        iter = edit.iterator_method(site, "enumerate", iter, &[])?;
    }
    let rows = edit.iterator_method(site, "map", iter, &[produce])?;
    let block = edit.empty_origin_node(
        site,
        InnerKind::Expression(Expr::Block),
        [
            ("contents", Field::Array(statements)),
            ("lastExpression", link(rows)),
            ("type", link(edit.expression_type(rows)?)),
        ],
    )?;
    edit.closure_from_variables(edit.expression(block)?, &[row])
}
