//! Derive closure parameter types after an unused-field projection.
use feldera_sql_ir::{CircuitEditor, Error, NodeId, TypeId, TypeKind};
use std::collections::HashSet;

/// Usage bits follow the leaves of the input type in declaration order.
/// References preserve the projection depth. At the cutoff a tuple is atomic.
pub fn compressed_field_type(
    edit: &mut CircuitEditor,
    input: TypeId,
    depth: usize,
    usage: &[bool],
) -> Result<TypeId, Error> {
    enum Step {
        Visit(TypeId, usize),
        Reference(TypeId),
        Tuple {
            original: TypeId,
            start: usize,
            raw: bool,
        },
    }
    let mut bits = usage.iter().copied();
    let mut pending = vec![Step::Visit(input, depth)];
    let mut active = HashSet::new();
    let mut results: Vec<(TypeId, bool)> = Vec::new();
    while let Some(step) = pending.pop() {
        match step {
            Step::Visit(ty, remaining) => {
                let kind = edit.type_kind(ty)?;
                let tuple = matches!(kind, TypeKind::Tuple | TypeKind::RawTuple);
                if kind == TypeKind::Ref || tuple && remaining > 0 {
                    if !active.insert(NodeId::from(ty)) {
                        return Err(Error::new("cycle in projected parameter type"));
                    }
                    if kind == TypeKind::Ref {
                        let base = edit.type_id(edit.node(ty.into())?.edge("type")?)?;
                        pending.push(Step::Reference(ty));
                        pending.push(Step::Visit(base, remaining));
                    } else {
                        pending.push(Step::Tuple {
                            original: ty,
                            start: results.len(),
                            raw: kind == TypeKind::RawTuple,
                        });
                        for field in edit.node(ty.into())?.edges("tupFields")?.into_iter().rev() {
                            pending.push(Step::Visit(edit.type_id(field)?, remaining - 1));
                        }
                    }
                } else {
                    let used = bits
                        .next()
                        .ok_or_else(|| Error::new("missing input field usage bit"))?;
                    let output = if used || tuple {
                        ty
                    } else {
                        edit.tuple_type(ty.into(), false, [])?
                    };
                    results.push((output, used));
                }
            }
            Step::Reference(original) => {
                active.remove(&NodeId::from(original));
                let (ty, _) = results.last_mut().expect("reference target was visited");
                *ty = edit.reference_type(*ty)?;
            }
            Step::Tuple {
                original,
                start,
                raw,
            } => {
                active.remove(&NodeId::from(original));
                // An empty tuple is a value even if the reader selects no fields.
                let used = results.len() == start || results[start..].iter().any(|(_, used)| *used);
                let kept = results
                    .drain(start..)
                    .filter(|(_, used)| *used || raw)
                    .map(|(ty, _)| ty.into());
                let ty = edit.tuple_type(original.into(), raw, kept)?;
                let ty = edit.with_nullability(ty, edit.nullable(original)?)?;
                results.push((ty, used));
            }
        }
    }
    if bits.next().is_some() {
        return Err(Error::new("extra usage bits after the last input field"));
    }
    Ok(results.pop().expect("visited root type").0)
}
