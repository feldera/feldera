//! Build field-comparator chains for ordered indexes and projected TopK inputs.
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, NodeId, TypeId,
    TypeKind,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashSet};

fn comparator_type(edit: &mut CircuitEditor, site: NodeId, text: &str) -> Result<TypeId, Error> {
    let digest = Sha256::digest(text.as_bytes());
    let prefix = u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 has 32 bytes"));
    let ty = edit.empty_origin_node(
        site,
        InnerKind::Type(TypeKind::Comparator),
        [
            ("name", Field::Data(format!("CMP_{prefix:016x}").into())),
            ("code", Field::Data("COMPARATOR".into())),
            ("mayBeNull", Field::Data(false.into())),
        ],
    )?;
    edit.type_id(ty)
}

pub(super) fn empty_comparator(
    edit: &mut CircuitEditor,
    site: NodeId,
    row: TypeId,
    printed_row: &str,
) -> Result<ExpressionId, Error> {
    let ty = comparator_type(edit, site, printed_row)?;
    edit.make_expr(
        site,
        Expr::NoComparator,
        ty,
        [("tupleType", Field::Reference(row.into()))],
    )
}

pub(super) fn field_comparator(
    edit: &mut CircuitEditor,
    site: NodeId,
    source: ExpressionId,
    field: usize,
    ascending: bool,
    nulls_first: bool,
) -> Result<ExpressionId, Error> {
    let name = edit
        .node(edit.expression_type(source)?.into())?
        .data("name")
        .and_then(|value| value.as_str())
        .ok_or_else(|| Error::new("missing comparator type name"))?;
    let ty = comparator_type(
        edit,
        site,
        &format!("{name}.{field} {ascending} {nulls_first}"),
    )?;
    edit.make_expr(
        site,
        Expr::FieldComparator,
        ty,
        [
            ("source", Field::Reference(source.into())),
            ("ascending", Field::Data(ascending.into())),
            ("nullsFirst", Field::Data(nulls_first.into())),
            ("fieldNo", Field::Data(field.into())),
        ],
    )
}

struct SortField {
    site: NodeId,
    column: usize,
    ascending: bool,
    nulls_first: bool,
}

struct FieldOrder {
    base: NodeId,
    fields: Vec<SortField>,
}

impl FieldOrder {
    /// A direct or asymmetric comparator can depend on the whole row.
    fn read(edit: &CircuitEditor, mut id: NodeId) -> Result<Option<Self>, Error> {
        let mut fields = Vec::new();
        let mut visited = HashSet::new();
        loop {
            let node = edit.node(id)?;
            match node.inner_kind() {
                Some(InnerKind::Expression(Expr::NoComparator)) => {
                    return Ok(Some(Self { base: id, fields }));
                }
                Some(InnerKind::Expression(Expr::FieldComparator)) => {
                    if !visited.insert(id) {
                        return Err(Error::new("cycle in field comparator chain"));
                    }
                    let column = node
                        .data("fieldNo")
                        .and_then(|v| v.as_u64())
                        .and_then(|i| i.try_into().ok())
                        .ok_or_else(|| Error::new("missing comparator field number"))?;
                    let flag = |name| {
                        node.data(name).and_then(|v| v.as_bool()).ok_or_else(|| {
                            Error::new(format!("missing {name} on field comparator"))
                        })
                    };
                    fields.push(SortField {
                        site: id,
                        column,
                        ascending: flag("ascending")?,
                        nulls_first: flag("nullsFirst")?,
                    });
                    id = node.edge("source")?;
                }
                _ => return Ok(None),
            }
        }
    }
}

/// All columns needed by the rank order and the tie comparison, in input order.
pub fn topk_compared_fields(
    edit: &CircuitEditor,
    orders: [NodeId; 2],
) -> Result<Option<BTreeSet<usize>>, Error> {
    let mut columns = BTreeSet::new();
    for id in orders {
        let Some(order) = FieldOrder::read(edit, id)? else {
            return Ok(None);
        };
        columns.extend(order.fields.into_iter().map(|field| field.column));
    }
    Ok(Some(columns))
}

/// Rebuild both orders against the projected row, preserving each source location and sort direction.
pub fn project_topk_comparators(
    edit: &mut CircuitEditor,
    orders: [NodeId; 2],
    row: TypeId,
    printed_row: &str,
    used: &[bool],
) -> Result<Vec<ExpressionId>, Error> {
    let raw = edit.type_kind(row)? == TypeKind::RawTuple;
    let retained = used.iter().enumerate().filter(|(_, keep)| raw || **keep);
    let mut positions = vec![None; used.len()];
    let width = edit.node(row.into())?.edges("tupFields")?.len();
    if retained.clone().count() != width {
        return Err(Error::new(
            "projected TopK row width differs from its field usage",
        ));
    }
    for (new, (old, _)) in retained.enumerate() {
        positions[old] = Some(new);
    }
    orders
        .into_iter()
        .map(|id| {
            let order = FieldOrder::read(edit, id)?
                .ok_or_else(|| Error::new("expected a chain of TopK field comparisons"))?;
            let mut source = empty_comparator(edit, order.base, row, printed_row)?;
            for field in order.fields.into_iter().rev() {
                let new = positions
                    .get(field.column)
                    .copied()
                    .flatten()
                    .ok_or_else(|| {
                        Error::new("TopK comparator reads a discarded or missing column")
                    })?;
                source = field_comparator(
                    edit,
                    field.site,
                    source,
                    new,
                    field.ascending,
                    field.nulls_first,
                )?;
            }
            Ok(source)
        })
        .collect()
}
