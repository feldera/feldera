//! Determine which constant subexpressions can be stored in a static.
use crate::lexical_scope::{closed_over, expression_children, resolve, References};
use feldera_sql_ir::{
    Circuit, Error, ExpressionKind as Expr, Field, InnerKind, Node, NodeId, TypeKind,
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashSet};

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StaticCandidates {
    Plan {
        candidates: BTreeSet<u64>,
        cells: BTreeSet<u64>,
        reads: BTreeMap<u64, u64>,
    },
    UntypedArray(u64),
}

#[derive(Default)]
struct Constants {
    /// Values known at compilation, such as literal elements in an array.
    constructed: HashSet<NodeId>,
    /// Values independent of the current row, including calls with constant arguments.
    row_independent: HashSet<NodeId>,
}

fn all_in(node: &Node, fields: &[&str], constants: &HashSet<NodeId>) -> bool {
    let mut inputs = Vec::new();
    for field in fields.iter().filter_map(|name| node.field(name)) {
        field.collect_inner_edges(&mut inputs);
    }
    inputs.iter().all(|id| constants.contains(id))
}

impl Constants {
    fn classify(
        &mut self,
        circuit: &Circuit,
        id: NodeId,
        kind: Expr,
        references: &References,
    ) -> Result<bool, Error> {
        let node = circuit.node(id).expect("visited expression");
        let literal = kind.wire_name().ends_with("Literal");
        let constructed = match kind {
            Expr::Array | Expr::ZSet => all_in(node, &["data"], &self.constructed),
            Expr::Map => all_in(node, &["keys", "values"], &self.constructed),
            Expr::RawTuple | Expr::Tuple => all_in(node, &["fields"], &self.constructed),
            Expr::GeoPointConstructor => all_in(node, &["left", "right"], &self.constructed),
            Expr::Variant => all_in(node, &["value"], &self.constructed),
            Expr::IndexedZSet => true,
            _ => literal,
        };
        if constructed {
            self.constructed.insert(id);
        }
        let independent = match kind {
            Expr::Unary | Expr::Question | Expr::HandleError | Expr::Cast => {
                self.row_independent.contains(&node.edge("source")?)
            }
            Expr::IsNull
            | Expr::Some
            | Expr::QualifyType
            | Expr::Unwrap
            | Expr::Clone
            | Expr::Borrow
            | Expr::Deref => self.row_independent.contains(&node.edge("expression")?),
            Expr::Binary => all_in(node, &["left", "right"], &self.row_independent),
            Expr::Apply => all_in(node, &["arguments"], &self.row_independent),
            Expr::ApplyMethod => all_in(node, &["self", "arguments"], &self.row_independent),
            Expr::Constructor => all_in(node, &["function", "arguments"], &self.row_independent),
            Expr::Tuple | Expr::RawTuple => all_in(node, &["fields"], &self.row_independent),
            Expr::If => all_in(
                node,
                &["condition", "positive", "negative"],
                &self.row_independent,
            ),
            Expr::Closure => closed_over(circuit, id, None, references)?,
            Expr::Path | Expr::EnumValue => true,
            // A VARIANT constructor can occur inside a compile-time collection.
            // The static pass does not lift the VARIANT constructor on its own.
            Expr::Variant => false,
            _ => constructed,
        };
        if independent {
            self.row_independent.insert(id);
        }
        Ok(independent)
    }
}

fn holds_reference(circuit: &Circuit, ty: NodeId) -> Result<bool, Error> {
    let mut pending = vec![ty];
    let mut seen = HashSet::new();
    while let Some(id) = pending.pop() {
        if !seen.insert(id) {
            continue;
        }
        let node = circuit.node(id).expect("validated type");
        match node.inner_kind() {
            Some(InnerKind::Type(TypeKind::Ref)) => return Ok(true),
            Some(InnerKind::Type(TypeKind::Tuple | TypeKind::RawTuple)) => {
                pending.extend(node.edges("tupFields")?)
            }
            _ => {}
        }
    }
    Ok(false)
}

fn can_lift(circuit: &Circuit, kind: Expr, node: &Node) -> Result<bool, Error> {
    if matches!(kind, Expr::Closure | Expr::Path) || kind.wire_name().ends_with("Literal") {
        return Ok(false);
    }
    if matches!(kind, Expr::Tuple | Expr::RawTuple) {
        let Some(Field::Array(fields)) = node.field("fields") else {
            return Ok(false);
        };
        if fields.iter().filter_map(Field::node_id).any(|id| {
            matches!(
                circuit.node(id).and_then(Node::inner_kind),
                Some(InnerKind::Expression(Expr::Closure))
            )
        }) {
            return Ok(false);
        }
    }
    Ok(!holds_reference(circuit, node.edge("type")?)?)
}

/// Report the first untyped array in visitor order so the caller can attach its original SQL span.
pub fn static_candidates(circuit: &Circuit, closure: NodeId) -> Result<StaticCandidates, Error> {
    let mut finished = HashSet::new();
    let mut path = HashSet::new();
    let mut pending = vec![(closure, false)];
    let mut expressions = Vec::new();
    while let Some((id, leaving)) = pending.pop() {
        if finished.contains(&id) {
            continue;
        }
        let node = circuit.node(id).expect("validated expression graph");
        if !leaving {
            if !path.insert(id) {
                return Err(Error::new("cycle in static initializer"));
            }
            pending.push((id, true));
            pending.extend(
                expression_children(node)
                    .into_iter()
                    .rev()
                    .map(|id| (id, false)),
            );
            continue;
        }
        path.remove(&id);
        finished.insert(id);
        let (NodeId::Inner(number), Some(InnerKind::Expression(kind))) = (id, node.inner_kind())
        else {
            continue;
        };
        if *kind == Expr::Array {
            let ty = circuit
                .node(node.edge("type")?)
                .expect("validated array type");
            let [element] = ty
                .edges("typeArgs")?
                .try_into()
                .map_err(|_| Error::new("array type needs one element type"))?;
            if matches!(
                circuit.node(element).and_then(Node::inner_kind),
                Some(InnerKind::Type(TypeKind::Any))
            ) {
                return Ok(StaticCandidates::UntypedArray(number));
            }
        }
        expressions.push((id, number, *kind));
    }
    let references = resolve(circuit, closure)?;
    let mut constants = Constants::default();
    let mut candidates = BTreeSet::new();
    for (id, number, kind) in expressions {
        if constants.classify(circuit, id, kind, &references)?
            && can_lift(circuit, kind, circuit.node(id).expect("visited expression"))?
        {
            candidates.insert(number);
        }
    }
    let mut cells = BTreeSet::new();
    for id in finished {
        let node = circuit.node(id).expect("visited declaration");
        if !matches!(node.class(), "DBSPLetStatement" | "DBSPLetExpression") {
            continue;
        }
        let Some(initializer) = node.field("initializer").and_then(Field::node_id) else {
            continue;
        };
        let init = circuit.node(initializer).expect("validated initializer");
        if matches!(init.inner_kind(), Some(InnerKind::Expression(Expr::Lazy))) {
            let NodeId::Inner(value) = init.edge("expression")? else {
                return Err(Error::new("lazy cell must hold an expression"));
            };
            if candidates.contains(&value) {
                let NodeId::Inner(cell) = id else {
                    unreachable!("inner declaration")
                };
                cells.insert(cell);
            }
        }
    }
    let reads = references
        .into_iter()
        .filter_map(|(variable, declaration)| match (variable, declaration) {
            (NodeId::Inner(var), NodeId::Inner(decl)) if cells.contains(&decl) => Some((var, decl)),
            _ => None,
        })
        .collect();
    Ok(StaticCandidates::Plan {
        candidates,
        cells,
        reads,
    })
}
