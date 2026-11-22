//! Identify which output columns of a map depend on a single indexed value
//! field. Reading the pair itself prevents substitution of a smaller value.

use feldera_sql_ir::{CircuitEditor, Error, ExpressionKind as Expr, InnerKind, NodeId};
use std::collections::BTreeSet;

#[derive(Copy, Clone, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub(super) struct Access {
    pub part: usize,
    pub field: usize,
    pub expression: NodeId,
}

#[derive(Default)]
pub(super) struct Reads {
    pub values: BTreeSet<usize>,
    pub key: bool,
    pub unsupported: bool,
    pub accesses: Vec<Access>,
    pub expensive: bool,
}

pub(super) fn kind(edit: &CircuitEditor, id: NodeId) -> Option<Expr> {
    match edit.node(id).ok()?.inner_kind()? {
        InnerKind::Expression(kind) => Some(*kind),
        _ => None,
    }
}

#[derive(Clone, serde::Deserialize, serde::Serialize)]
pub(super) struct Binding {
    hidden: BTreeSet<String>,
    pub parameters: Vec<NodeId>,
    pub names: Vec<String>,
    /// Maps borrow a pair. Joins borrow the key and each input value separately.
    pub pair: bool,
    pub value: usize,
}

impl Binding {
    pub fn new(
        edit: &CircuitEditor,
        function: NodeId,
        pair: bool,
        input: usize,
    ) -> Result<Self, Error> {
        let parameters = edit.node(function)?.edges("parameters")?;
        let names = parameters
            .iter()
            .map(|p| {
                edit.node(*p)?
                    .data("name")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
                    .ok_or_else(|| Error::new("missing reader parameter name"))
            })
            .collect::<Result<_, _>>()?;
        if (pair && parameters.len() != 1) || (!pair && parameters.len() <= input + 1) {
            return Err(Error::new("invalid aggregate reader arity"));
        }
        Ok(Self {
            hidden: BTreeSet::new(),
            parameters,
            names,
            pair,
            value: if pair { 1 } else { input + 1 },
        })
    }

    pub fn parameter(&self, name: &str) -> Option<usize> {
        if self.hidden.contains(name) {
            return None;
        }
        self.names.iter().position(|p| p == name)
    }

    pub fn nested(&self, edit: &CircuitEditor, parameters: &[NodeId]) -> Result<Self, Error> {
        let mut nested = self.clone();
        for parameter in parameters {
            let name = edit
                .node(*parameter)?
                .data("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| Error::new("missing closure parameter name"))?;
            nested.hidden.insert(name.to_owned());
        }
        Ok(nested)
    }

    pub fn access(&self, edit: &CircuitEditor, id: NodeId) -> Option<Access> {
        let field = |id| usize::try_from(edit.node(id).ok()?.data("fieldNo")?.as_u64()?).ok();
        if kind(edit, id) != Some(Expr::Field) {
            return None;
        }
        let deref = edit.node(id).ok()?.edge("expression").ok()?;
        if kind(edit, deref) != Some(Expr::Deref) {
            return None;
        }
        let source = edit.node(deref).ok()?.edge("expression").ok()?;
        let (var, part) = if self.pair {
            if kind(edit, source) != Some(Expr::Field) {
                return None;
            }
            (
                edit.node(source).ok()?.edge("expression").ok()?,
                Some(field(source)?),
            )
        } else {
            (source, None)
        };
        if kind(edit, var) != Some(Expr::VariablePath) {
            return None;
        }
        let name = edit.node(var).ok()?.data("variable")?.as_str()?;
        let parameter = self.parameter(name)?;
        Some(Access {
            part: part.unwrap_or(parameter),
            field: field(id)?,
            expression: id,
        })
    }

    pub fn value_name(&self) -> &str {
        &self.names[if self.pair { 0 } else { self.value }]
    }
}

impl Reads {
    pub fn collect(
        &mut self,
        edit: &CircuitEditor,
        id: NodeId,
        binding: &Binding,
    ) -> Result<(), Error> {
        if let Some(read) = binding.access(edit, id) {
            self.accesses.push(read);
            if read.part == binding.value {
                self.values.insert(read.field);
            } else {
                self.key = true;
            }
            return Ok(());
        }
        let node = edit.node(id)?;
        match kind(edit, id) {
            Some(Expr::Apply) => {
                if kind(edit, node.edge("function")?) != Some(Expr::Path) {
                    self.unsupported = true;
                }
                let function = edit.node(node.edge("function")?)?;
                let tumble = function
                    .edge("path")
                    .ok()
                    .and_then(|id| edit.node(id).ok())
                    .and_then(|path| path.edges("components").ok())
                    .and_then(|ids| ids.last().copied())
                    .and_then(|id| edit.node(id).ok()?.data("identifier")?.as_str())
                    .is_some_and(|s| s.starts_with("tumble_"));
                self.expensive |= !tumble;
                for arg in node.edges("arguments")? {
                    self.collect(edit, arg, binding)?;
                }
            }
            Some(Expr::Binary) => {
                self.collect(edit, node.edge("left")?, binding)?;
                self.collect(edit, node.edge("right")?, binding)?;
            }
            Some(Expr::Closure) => {
                let scope = binding.nested(edit, &node.edges("parameters")?)?;
                self.collect(edit, node.edge("body")?, &scope)?;
            }
            Some(Expr::VariablePath) => {
                if let Some(index) = node
                    .data("variable")
                    .and_then(|v| v.as_str())
                    .and_then(|n| binding.parameter(n))
                {
                    if binding.pair || index == binding.value {
                        self.unsupported = true;
                    } else {
                        self.key = true;
                    }
                }
            }
            Some(Expr::Cast | Expr::Unary | Expr::UnsignedUnwrap | Expr::UnsignedWrap) => {
                self.collect(edit, node.edge("source")?, binding)?
            }
            Some(Expr::Deref | Expr::Field | Expr::Clone | Expr::Unwrap | Expr::Some) => {
                self.collect(edit, node.edge("expression")?, binding)?
            }
            _ if node.class().ends_with("Literal") => {}
            _ => self.unsupported = true,
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array_agg::build::{link, links, rebind, Parameters};
    use feldera_sql_ir::{Circuit, Field, TypeKind};
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn join_arguments_remain_bound_inside_nested_closures() {
        let graph = Circuit::from_json(
            json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]}),
        )
        .unwrap();
        let mut edit = CircuitEditor::new(graph);
        let tuple = edit
            .inner(
                InnerKind::Type(TypeKind::Tuple),
                [("tupFields", links(&[]))],
            )
            .unwrap();
        let reference = edit
            .inner(InnerKind::Type(TypeKind::Ref), [("type", link(tuple))])
            .unwrap();
        let mut params = Vec::new();
        for name in ["key", "row"] {
            params.push(
                edit.inner(
                    InnerKind::Parameter,
                    [
                        ("name", Field::Data(name.into())),
                        ("type", link(reference)),
                    ],
                )
                .unwrap(),
            );
        }
        let reader = edit
            .inner(
                InnerKind::Expression(Expr::Closure),
                [("parameters", links(&params))],
            )
            .unwrap();
        let binding = Binding::new(&edit, reader, false, 0).unwrap();
        let key = edit
            .inner(
                InnerKind::Expression(Expr::VariablePath),
                [
                    ("type", link(reference)),
                    ("variable", Field::Data("key".into())),
                ],
            )
            .unwrap();
        let lambda_type = edit
            .inner(
                InnerKind::Type(TypeKind::Function),
                [
                    ("resultType", link(reference)),
                    ("parameterTypes", links(&[])),
                ],
            )
            .unwrap();
        let lambda = edit
            .inner(
                InnerKind::Expression(Expr::Closure),
                [
                    ("parameters", links(&[])),
                    ("body", link(key)),
                    ("type", link(lambda_type)),
                ],
            )
            .unwrap();
        let mut uses = Reads::default();
        uses.collect(&edit, lambda, &binding).unwrap();
        assert!(uses.key);
        let shadowed = binding.nested(&edit, &params[..1]).unwrap();
        let mut uses = Reads::default();
        uses.collect(&edit, lambda, &shadowed).unwrap();
        assert!(!uses.key);
        let mut next = 10;
        let mut fresh = Parameters::new(&mut edit, &binding, tuple, &mut next).unwrap();
        let rewritten = rebind(
            &mut edit,
            lambda,
            &binding,
            &mut fresh,
            &mut BTreeMap::new(),
        )
        .unwrap();
        let inner = edit.node(rewritten).unwrap().edge("body").unwrap();
        assert_eq!(
            edit.node(inner).unwrap().data("variable"),
            Some(&json!("t_10"))
        );
        assert_eq!(
            rebind(
                &mut edit,
                lambda,
                &shadowed,
                &mut fresh,
                &mut BTreeMap::new()
            )
            .unwrap(),
            lambda
        );
    }
}
