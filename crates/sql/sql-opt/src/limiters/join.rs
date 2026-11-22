//! Combine compacted bounds from the two indexed inputs of a join.
mod asof;
mod filtered;
mod star;
pub(super) use star::StarJoinExpansion;

use super::pass::{apply_bound, apply_bound_pair, projected_bound, projected_minimum};
use super::{Bound, Projection, apply_waterlines, retention::retain_keys};
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
    sum_inputs: Vec<u64>,
    unmatched: Option<UnmatchedRows>,
    /// A fused predicate filters each term before the sum reads it.
    #[serde(default)]
    filters: Vec<UnaryTerm>,
}

#[derive(Deserialize)]
struct UnaryTerm {
    input: u64,
    id: u64,
    output: Option<Projection>,
}

/// The antijoin and null-padding map supply the fourth term of a left join.
#[derive(Deserialize)]
struct UnmatchedRows {
    output: Option<Projection>,
    pad_id: u64,
    padded: Option<Projection>,
}

#[derive(Deserialize)]
struct JoinSide {
    projection: Option<Projection>,
    integral_monotone: bool,
}

#[derive(Deserialize)]
pub(super) struct JoinTerm {
    id: u64,
    left: Option<Projection>,
    right: Option<Projection>,
    output: Option<Projection>,
}

pub(super) struct JoinInputs<'a> {
    pub owner: NodeId,
    pub data: [OutputPort; 2],
    /// Trace retention tests the original ports. Delta terms read the expanded ports.
    pub original_bounds: [Option<Bound>; 2],
    pub expanded_bounds: [Option<Bound>; 2],
    pub functions: &'a BTreeMap<u64, NodeId>,
}

impl JoinTerm {
    pub(super) fn insert(
        &self,
        edit: &mut CircuitEditor,
        owner: NodeId,
        functions: &BTreeMap<u64, NodeId>,
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
        let transfer = functions
            .get(&self.id)
            .ok_or_else(|| Error::new("missing transfer for expanded join term"))?;
        let transfer = apply_waterlines(edit, *transfer, &[merged.ty], next, warnings)?;
        let bound = apply_bound(edit, body, merged, transfer)?;
        edit.place_before(body, merged.port.operator, owner)?;
        Ok(Some((bound, output)))
    }
}

impl UnaryTerm {
    fn apply<'a>(
        &'a self,
        edit: &mut CircuitEditor,
        owner: NodeId,
        functions: &BTreeMap<u64, NodeId>,
        bounds: &mut BTreeMap<u64, (Bound, &'a Projection)>,
        next: &mut u64,
        warnings: &mut Vec<SimplifyWarning>,
    ) -> Result<(), Error> {
        let Some(projection) = &self.output else {
            return Ok(());
        };
        let (input, _) = bounds
            .get(&self.input)
            .ok_or_else(|| Error::new("bounded join projection needs an input waterline"))?;
        let transfer = functions
            .get(&self.id)
            .ok_or_else(|| Error::new("missing waterline transfer for join projection"))?;
        let function = apply_waterlines(edit, *transfer, &[input.ty], next, warnings)?;
        let body = edit.circuit().root_id();
        let bound = apply_bound(edit, body, *input, function)?;
        edit.place_before(body, bound.port.operator, owner)?;
        bounds.insert(self.id, (bound, projection));
        Ok(())
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
        if matches!(join.original_bounds, [None, None]) {
            return Ok(None);
        }
        let expected = 3 + usize::from(self.unmatched.is_some());
        if self.sum_inputs.len() != expected {
            return Err(Error::new(
                "unexpected number of terms in incremental join sum",
            ));
        }
        let body = edit.circuit().root_id();
        for (side, opposite) in [(0, 1), (1, 0)] {
            // For a left join, an unmatched left row can be retracted. Its
            // antijoin key bound determines when the right trace can discard it.
            if side == 0 && self.unmatched.is_some() {
                continue;
            }
            let Some(bound) = join.original_bounds[side] else {
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

        let [left, right] = join.expanded_bounds;
        let term_inputs = [
            [left, right.filter(|_| self.inputs[1].integral_monotone)],
            [left.filter(|_| self.inputs[0].integral_monotone), right],
            [left, right],
        ];
        let mut results = BTreeMap::new();
        for (term, sides) in self.terms.iter().zip(term_inputs) {
            if let Some(result) =
                term.insert(edit, join.owner, join.functions, sides, next, warnings)?
            {
                results.insert(term.id, result);
            }
        }
        if let Some(unmatched) = &self.unmatched {
            if let Some(result) = unmatched.insert(edit, join, &self.inputs, next, warnings)? {
                results.insert(unmatched.pad_id, result);
            }
        }
        for filter in &self.filters {
            filter.apply(
                edit,
                join.owner,
                join.functions,
                &mut results,
                next,
                warnings,
            )?;
        }
        let inputs = self
            .sum_inputs
            .iter()
            .map(|id| results.get(id).copied())
            .collect::<Option<Vec<_>>>();
        let result = match (output, inputs) {
            (Some(dst), Some(inputs)) => {
                Some((projected_minimum(edit, join.owner, inputs, dst, next)?, dst))
            }
            _ => None,
        };
        if edit.node(join.owner)?.operator_kind()
            == Some(feldera_sql_ir::OperatorKind::JoinFilterMap)
        {
            let [both, left, right] = self.filters.as_slice() else {
                return Err(Error::new(
                    "filtered join requires its three delta predicates",
                ));
            };
            if [left.input, right.input, both.input] != self.terms.each_ref().map(|term| term.id) {
                return Err(Error::new(
                    "expanded join filters read the wrong delta terms",
                ));
            }
            filtered::retain_values(
                edit,
                join,
                [
                    results.get(&right.id).copied(),
                    results.get(&left.id).copied(),
                ],
                next,
                warnings,
            )?;
        }
        Ok(result)
    }
}

impl UnmatchedRows {
    fn insert(
        &self,
        edit: &mut CircuitEditor,
        join: &JoinInputs<'_>,
        sides: &[JoinSide; 2],
        next: &mut u64,
        warnings: &mut Vec<SimplifyWarning>,
    ) -> Result<Option<(Bound, &Projection)>, Error> {
        let Some(output) = &self.output else {
            return Ok(None);
        };
        let [Some(left), Some(right)] = join.expanded_bounds else {
            return Err(Error::new(
                "bounded left antijoin needs two input waterlines",
            ));
        };
        let [Some(l), Some(r)] = sides.each_ref().map(|side| side.projection.as_ref()) else {
            return Err(Error::new(
                "bounded left antijoin needs two input projections",
            ));
        };
        let bound = projected_minimum(edit, join.owner, [(left, l), (right, r)], output, next)?;

        // Both traces use the intersection of their key projections. The value
        // portion must have the shape of the right trace, including empty tuples.
        let target = output.with_unbounded_value_from(r)?;
        let body = edit.circuit().root_id();
        let control = projected_bound(edit, body, bound, output, &target, next)?;
        edit.place_before(body, control.port.operator, join.owner)?;
        retain_keys(edit, body, join.owner, join.data[1], &target, control, next)?
            .place_before_owner(edit, body, join.owner)?;

        let Some(padded) = &self.padded else {
            return Ok(None);
        };
        let function = join
            .functions
            .get(&self.pad_id)
            .ok_or_else(|| Error::new("missing null-padding transfer for left join"))?;
        let function = apply_waterlines(edit, *function, &[bound.ty], next, warnings)?;
        let padded_bound = apply_bound(edit, body, bound, function)?;
        edit.place_before(body, padded_bound.port.operator, join.owner)?;
        Ok(Some((padded_bound, padded)))
    }
}
