//! Combine compacted bounds from the two indexed inputs of a join.

use super::pass::{apply_bound, apply_bound_pair, projected_bound};
use super::{Bound, Projection, apply_waterlines, minimum_bound, retention::retain_keys};
use feldera_sql_expr::SimplifyWarning;
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, NodeId, OutputPort, TypeId};
use serde::Deserialize;
use std::collections::BTreeMap;

struct InputBound<'a> {
    key: Option<(ExpressionId, &'a Projection)>,
    value: Option<ExpressionId>,
}

/// Read a compacted (key, value) bound. A missing key shifts the value to slot zero.
fn input_bound<'a>(
    edit: &mut CircuitEditor,
    parameter: ExpressionId,
    shape: &'a Projection,
) -> Result<InputBound<'a>, Error> {
    let Projection::Tuple { fields, .. } = shape else {
        return Err(Error::new("expected an indexed join waterline"));
    };
    let [key, value] = fields.as_slice() else {
        return Err(Error::new(
            "expected a key and a value in the join waterline",
        ));
    };
    let mut field = |index| {
        let input = edit.dereference(parameter)?;
        edit.tuple_field(input, index)
    };
    let key = key
        .monotone()
        .then(|| field(0).map(|read| (read, key)))
        .transpose()?;
    let value = value
        .monotone()
        .then(|| field(usize::from(key.is_some())))
        .transpose()?;
    Ok(InputBound { key, value })
}

/// Build the apply closure over the available input bounds. The join's transfer
/// function expects (key, left value, right value), with empty tuples for absent
/// components. If both sides bound the key, merge their fields pointwise.
pub fn join_waterline(
    edit: &mut CircuitEditor,
    sides: [Option<(TypeId, &Projection)>; 2],
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<ExpressionId, Error> {
    let mut names = *next;
    let mut types = Vec::new();
    let mut parameters = Vec::new();
    let [left, right] = sides.map(|side| -> Result<_, Error> {
        let Some((ty, shape)) = side else {
            return Ok(InputBound {
                key: None,
                value: None,
            });
        };
        let param = super::filter::arg(edit, ty, &mut names)?;
        types.push(ty);
        parameters.push(param);
        input_bound(edit, param, shape)
    });
    let (left, right) = (left?, right?);
    let &site = types
        .first()
        .ok_or_else(|| Error::new("join needs at least one waterline"))?;
    let key = match (left.key, right.key) {
        (Some((l, shape)), Some((r, other))) => shape.merge(edit, l, other, r)?,
        (Some((key, _)), None) | (None, Some((key, _))) => edit.clone_if_needed(key)?,
        (None, None) => edit.tuple_expr(site.into(), false, &[])?,
    };
    let mut value = |field| match field {
        Some(read) => edit.clone_if_needed(read),
        None => edit.tuple_expr(site.into(), false, &[]),
    };
    let fields = [key, value(left.value)?, value(right.value)?];
    let body = edit.tuple_expr(key.into(), true, &fields)?;
    let closure = edit.closure_from_variables(body, &parameters)?;
    let result = apply_waterlines(edit, closure.into(), &types, &mut names, warnings)?;
    *next = names;
    Ok(result)
}

#[derive(Deserialize)]
pub(super) struct JoinFacts {
    pub expansion: Option<BinaryJoinExpansion>,
}

#[derive(Deserialize)]
pub(super) struct BinaryJoinExpansion {
    inputs: [JoinSide; 2],
    /// Left delta, right delta, then both deltas, in visitor order.
    terms: [JoinTerm; 3],
    /// The sum preserves its own input order when projecting each term.
    sum_inputs: [u64; 3],
}

#[derive(Deserialize)]
struct JoinSide {
    projection: Option<Projection>,
    integral_monotone: bool,
}

#[derive(Deserialize)]
struct JoinTerm {
    id: u64,
    left: Option<Projection>,
    right: Option<Projection>,
    output: Option<Projection>,
}

pub(super) struct JoinInputs<'a> {
    pub owner: NodeId,
    pub data: [OutputPort; 2],
    pub bounds: [Option<Bound>; 2],
    pub functions: &'a BTreeMap<u64, NodeId>,
}

impl JoinTerm {
    fn insert(
        &self,
        edit: &mut CircuitEditor,
        join: &JoinInputs<'_>,
        bounds: [Option<Bound>; 2],
        next: &mut u64,
        warnings: &mut Vec<SimplifyWarning>,
    ) -> Result<Option<(Bound, &Projection)>, Error> {
        let Some(output) = &self.output else {
            return Ok(None);
        };
        if matches!(bounds, [None, None]) {
            return Ok(None);
        }
        fn side(
            bound: Option<Bound>,
            projection: Option<&Projection>,
        ) -> Result<Option<(TypeId, &Projection)>, Error> {
            match bound {
                None => Ok(None),
                Some(b) => projection
                    .map(|p| Some((b.ty, p)))
                    .ok_or_else(|| Error::new("missing join input projection")),
            }
        }
        let projections = [
            side(bounds[0], self.left.as_ref())?,
            side(bounds[1], self.right.as_ref())?,
        ];
        let function = join_waterline(edit, projections, next, warnings)?;
        let body = edit.circuit().root_id();
        let merged = match bounds {
            [Some(left), Some(right)] => apply_bound_pair(edit, [left, right], function)?,
            [Some(bound), None] | [None, Some(bound)] => apply_bound(edit, body, bound, function)?,
            _ => unreachable!("checked above"),
        };
        let transfer = join
            .functions
            .get(&self.id)
            .ok_or_else(|| Error::new("missing transfer for expanded join term"))?;
        let transfer = apply_waterlines(edit, *transfer, &[merged.ty], next, warnings)?;
        let bound = apply_bound(edit, body, merged, transfer)?;
        edit.place_before(body, merged.port.operator, join.owner)?;
        Ok(Some((bound, output)))
    }
}

impl BinaryJoinExpansion {
    pub fn insert<'a>(
        &self,
        edit: &mut CircuitEditor,
        join: &JoinInputs<'_>,
        output: Option<&'a Projection>,
        next: &mut u64,
        warnings: &mut Vec<SimplifyWarning>,
    ) -> Result<Option<(Bound, &'a Projection)>, Error> {
        let body = edit.circuit().root_id();
        for (side, opposite) in [(0, 1), (1, 0)] {
            let Some(bound) = join.bounds[side] else {
                continue;
            };
            let Some(projection @ Projection::Tuple { fields, .. }) = &self.inputs[side].projection
            else {
                return Err(Error::new(
                    "missing indexed projection on bounded join input",
                ));
            };
            let [key, _] = fields.as_slice() else {
                return Err(Error::new("join requires a key and value"));
            };
            if key.monotone() {
                retain_keys(
                    edit,
                    body,
                    join.owner,
                    join.data[opposite],
                    projection,
                    bound,
                    next,
                )?
                .place_before_owner(edit, body, join.owner)?;
            }
        }

        let [left, right] = join.bounds;
        let term_inputs = [
            [left, right.filter(|_| self.inputs[1].integral_monotone)],
            [left.filter(|_| self.inputs[0].integral_monotone), right],
            [left, right],
        ];
        let mut results = BTreeMap::new();
        for (term, sides) in self.terms.iter().zip(term_inputs) {
            if let Some(result) = term.insert(edit, join, sides, next, warnings)? {
                results.insert(term.id, result);
            }
        }
        let Some(dst) = output else {
            return Ok(None);
        };
        let [Some(a), Some(b), Some(c)] = self.sum_inputs.map(|id| results.get(&id)) else {
            return Ok(None);
        };
        let mut projected = Vec::with_capacity(3);
        for &(bound, src) in [a, b, c] {
            projected.push(projected_bound(edit, body, bound, src, dst, next)?);
        }
        let result = minimum_bound(edit, body, &projected, next)?;
        edit.place_before(body, projected[0].port.operator, join.owner)?;
        Ok(Some((result, dst)))
    }
}
