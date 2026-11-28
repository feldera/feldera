//! Structural equality of the types used in optimizer expressions.
use super::{CircuitEditor, Error, NodeId, Ty, TypeId};
use crate::Node;
use std::collections::{BTreeMap, HashMap};

type Pair = (NodeId, NodeId);

enum Visit {
    Compare(Pair),
    Finish(Pair),
}

impl CircuitEditor {
    /// The ordered arguments of a DBSP user type, including specialized encodings
    /// such as indexed Z-sets that store each argument in a named field.
    pub fn user_type_arguments(&self, ty: TypeId) -> Result<Option<Vec<NodeId>>, Error> {
        generic_arguments(self.type_kind(ty)?)
            .map(|args| args.read(self.node(ty.into())?))
            .transpose()
    }

    pub fn same_type(&self, left: TypeId, right: TypeId) -> Result<bool, Error> {
        self.equal_types(left, right, false)
    }

    /// Ignore only the outer SQL nullability. Nested fields and elements keep theirs.
    pub fn same_type_ignoring_nullability(
        &self,
        left: TypeId,
        right: TypeId,
    ) -> Result<bool, Error> {
        self.equal_types(left, right, true)
    }

    fn equal_types(
        &self,
        left: TypeId,
        right: TypeId,
        ignore_outer_null: bool,
    ) -> Result<bool, Error> {
        if left == right {
            return Ok(true);
        }
        if !ignore_outer_null && self.nullable(left)? != self.nullable(right)? {
            return Ok(false);
        }
        let mut pending = Vec::new();
        if !self.compare_type_members(left, right, &mut pending)? {
            return Ok(false);
        }

        // Shared fields can refer to an entire nested row. Compare each pair
        // once and reject a reference back to an unfinished comparison.
        let root = (left.0, right.0);
        let mut finished = HashMap::new();
        while let Some(visit) = pending.pop() {
            let pair = match visit {
                Visit::Finish(pair) => {
                    finished.insert(pair, true);
                    continue;
                }
                Visit::Compare(pair) => pair,
            };
            if pair.0 == pair.1 || finished.get(&pair) == Some(&true) {
                continue;
            }
            if pair == root || finished.insert(pair, false).is_some() {
                return Err(Error::new("cycle in compared types"));
            }
            let left = self.type_id(pair.0)?;
            let right = self.type_id(pair.1)?;
            pending.push(Visit::Finish(pair));
            if self.nullable(left)? != self.nullable(right)?
                || !self.compare_type_members(left, right, &mut pending)?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn compare_type_members(
        &self,
        left: TypeId,
        right: TypeId,
        pending: &mut Vec<Visit>,
    ) -> Result<bool, Error> {
        let kind = self.type_kind(left)?;
        let other = self.type_kind(right)?;
        let a = self.node(left.0)?;
        let b = self.node(right.0)?;
        // DBSPTypeUser compares the Rust type name and its ordered arguments.
        // Specialized encodings such as ZSet store the arguments in named fields.
        if let (Some(left_args), Some(right_args)) =
            (generic_arguments(kind), generic_arguments(other))
        {
            return Ok((kind == other || a.data("name").is_some())
                && a.data("name") == b.data("name")
                && compare_lists(left_args.read(a)?, right_args.read(b)?, pending));
        }
        if kind != other {
            return Ok(false);
        }
        if kind == Ty::Stream
            && [a, b]
                .iter()
                .any(|node| node.data("kind").is_none() || node.data("nesting").is_none())
        {
            return Err(Error::new(
                "stream type comparison requires kind and nesting",
            ));
        }
        let properties: &[&str] = match kind {
            Ty::Integer => &["width", "signed"],
            Ty::Decimal => &["precision", "scale"],
            Ty::String | Ty::Binary => &["fixed", "precision"],
            Ty::ShortInterval | Ty::LongInterval => &["units"],
            Ty::Struct => &["hashName"],
            Ty::Stream => &["kind", "nesting"],
            _ => &["code"],
        };
        if properties.iter().any(|p| a.data(p) != b.data(p)) {
            return Ok(false);
        }
        match kind {
            Ty::Ref | Ty::Stream => {
                let field = if kind == Ty::Ref {
                    "type"
                } else {
                    "elementType"
                };
                pending.push(Visit::Compare((a.edge(field)?, b.edge(field)?)));
            }
            Ty::Tuple | Ty::RawTuple => {
                return Ok(compare_lists(
                    a.edges("tupFields")?,
                    b.edges("tupFields")?,
                    pending,
                ));
            }
            Ty::Function => {
                if !compare_lists(
                    a.edges("parameterTypes")?,
                    b.edges("parameterTypes")?,
                    pending,
                ) {
                    return Ok(false);
                }
                pending.push(Visit::Compare((
                    a.edge("resultType")?,
                    b.edge("resultType")?,
                )));
            }
            Ty::Struct => return self.compare_struct_fields(a, b, pending),
            _ => {}
        }
        Ok(true)
    }

    fn compare_struct_fields(
        &self,
        a: &Node,
        b: &Node,
        pending: &mut Vec<Visit>,
    ) -> Result<bool, Error> {
        if a.sql_identifier_name()? != b.sql_identifier_name()? {
            return Ok(false);
        }
        let left = a.edges("fields")?;
        let right = b.edges("fields")?;
        if left.len() != right.len() {
            return Ok(false);
        }
        let mut by_name = BTreeMap::new();
        for id in right {
            let field = self.node(id)?;
            by_name.insert(field.sql_identifier_name()?, field);
        }
        for id in left.into_iter().rev() {
            let field = self.node(id)?;
            let Some(other) = by_name.get(field.sql_identifier_name()?) else {
                return Ok(false);
            };
            if field.data("index") != other.data("index") {
                return Ok(false);
            }
            pending.push(Visit::Compare((field.edge("type")?, other.edge("type")?)));
        }
        Ok(true)
    }
}

fn compare_lists(left: Vec<NodeId>, right: Vec<NodeId>, pending: &mut Vec<Visit>) -> bool {
    if left.len() != right.len() {
        return false;
    }
    pending.extend(left.into_iter().zip(right).rev().map(Visit::Compare));
    true
}

enum Arguments {
    List,
    Named(&'static [&'static str]),
}

impl Arguments {
    fn read(self, node: &Node) -> Result<Vec<NodeId>, Error> {
        match self {
            Self::List => node.edges("typeArgs"),
            Self::Named(names) => names.iter().map(|name| node.edge(name)).collect(),
        }
    }
}

fn generic_arguments(kind: Ty) -> Option<Arguments> {
    Some(match kind {
        Ty::ZSet => Arguments::Named(&["elementType"]),
        Ty::IndexedZSet => Arguments::Named(&["keyType", "elementType"]),
        Ty::Comparator => Arguments::Named(&[]),
        Ty::Array
        | Ty::BTreeMap
        | Ty::Lazy
        | Ty::Map
        | Ty::Option
        | Ty::Result
        | Ty::Semigroup
        | Ty::SqlResult
        | Ty::TypedBox
        | Ty::User
        | Ty::Vec
        | Ty::Weight
        | Ty::WithCustomOrd => Arguments::List,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Circuit, Field, InnerKind};
    use serde_json::json;

    fn editor() -> Result<CircuitEditor, Error> {
        Ok(CircuitEditor::new(Circuit::from_json(json!({
            "class":"DBSPCircuit", "id":0, "allOperators":[], "declarations":[]
        }))?))
    }

    #[test]
    fn deep_shared_rows_compare_leaf_and_outer_nullability_separately() -> Result<(), Error> {
        let mut edit = editor()?;
        let site = edit.circuit().root_id();
        let mut left = edit.bool_type(site, false)?;
        let mut right = left;
        let mut different = edit.bool_type(site, true)?;
        for _ in 0..256 {
            left = edit.tuple_type(site, false, [left.into(), left.into()])?;
            right = edit.tuple_type(site, false, [right.into(), right.into()])?;
            different = edit.tuple_type(site, false, [different.into(), different.into()])?;
        }
        assert_ne!(left, right);
        assert!(edit.same_type(left, right)?);
        let optional = edit.with_nullability(right, true)?;
        assert!(!edit.same_type(left, optional)?);
        assert!(edit.same_type_ignoring_nullability(left, optional)?);
        assert!(!edit.same_type_ignoring_nullability(left, different)?);
        Ok(())
    }

    #[test]
    fn recursive_rows_are_rejected_before_comparison() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit", "id":0, "allOperators":[], "declarations":[]
        }))?;
        let recursive_rows = [1, 2].map(|id| {
            json!({
                "class":"DBSPTypeTuple", "id":id, "mayBeNull":false,
                "tupFields":[{"node":id}]
            })
        });
        // The decoder accepts acyclic types only, so `same_type` keeps its cycle
        // check for types that a pass builds.
        let err = circuit.with_inner_roots(recursive_rows).unwrap_err();
        assert!(
            err.to_string().contains("inner node 1 contains itself"),
            "{err}"
        );
        Ok(())
    }

    #[test]
    fn function_types_compare_their_result_and_ordered_parameters() -> Result<(), Error> {
        let mut edit = editor()?;
        let site = edit.circuit().root_id();
        let boolean = edit.bool_type(site, false)?;
        let integer = edit.integer_type(boolean, 32, true, false)?;
        let b = edit.named_variable(boolean, "b")?;
        let n = edit.named_variable(integer, "n")?;
        let mut signature = |result, args: &[NodeId]| -> Result<TypeId, Error> {
            let id = edit.closure_type(result, args)?;
            edit.type_id(id)
        };
        let params = [integer.into(), boolean.into()];
        let first = signature(n, &params)?;
        let same = signature(n, &params)?;
        let other_result = signature(b, &params)?;
        let swapped = signature(n, &[boolean.into(), integer.into()])?;
        let shorter = signature(n, &[integer.into()])?;
        assert!(edit.same_type(first, same)?);
        for other in [other_result, swapped, shorter] {
            assert!(!edit.same_type(first, other)?);
        }
        Ok(())
    }

    #[test]
    fn specialized_and_generic_collections_compare_by_name_and_arguments() -> Result<(), Error> {
        let mut edit = editor()?;
        let site = edit.circuit().root_id();
        let key = edit.bool_type(site, false)?;
        let value = edit.integer_type(key, 32, true, false)?;
        let indexed = edit.indexed_zset_type(site, key, value)?;
        let mut generic = |name: &str, args: [TypeId; 2]| -> Result<TypeId, Error> {
            let id = edit.inner_at(
                site,
                InnerKind::Type(Ty::User),
                [
                    ("name", Field::Data(name.into())),
                    ("code", Field::Data("USER".into())),
                    ("mayBeNull", Field::Data(false.into())),
                    (
                        "typeArgs",
                        Field::Array(args.into_iter().map(|ty| Field::Reference(ty.0)).collect()),
                    ),
                ],
            )?;
            edit.type_id(id)
        };
        let same = generic("IndexedWSet", [key, value])?;
        let reversed = generic("IndexedWSet", [value, key])?;
        let renamed = generic("Other", [key, value])?;
        assert!(edit.same_type(indexed, same)?);
        assert!(edit.same_type(same, indexed)?);
        assert!(!edit.same_type(indexed, reversed)?);
        assert!(!edit.same_type(indexed, renamed)?);
        Ok(())
    }
}
