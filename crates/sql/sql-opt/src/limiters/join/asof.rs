//! A preceding ASOF match becomes final once the right waterline passes it.
//! Right-side deletions can still expose the last match below both waterlines.
use super::{
    Bound, JoinInputs, JoinTerm, Projection, apply_bound, apply_bound_pair, apply_waterlines,
};
use crate::limiters::{filter::arg, minimum_pair, retention::ValueRetention};
use crate::rebuild::input_type;
use feldera_sql_expr::SimplifyWarning;
use feldera_sql_ir::{CircuitEditor, Error, ExpressionKind, OutputPort};

fn timestamp_bound(
    edit: &mut CircuitEditor,
    source: Option<Bound>,
    projection: Option<&Projection>,
    index: usize,
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<Option<Bound>, Error> {
    let (Some(source), Some(Projection::Tuple { fields, .. })) = (source, projection) else {
        return Ok(None);
    };
    let [key, value] = fields.as_slice() else {
        return Err(Error::new("ASOF input projection requires a key and value"));
    };
    let Projection::Tuple { fields, .. } = value else {
        return Ok(None);
    };
    let field = fields
        .get(index)
        .ok_or_else(|| Error::new("ASOF timestamp outside its projection"))?;
    if !field.monotone() {
        return Ok(None);
    }
    let parameter = arg(edit, source.ty, next)?;
    let row = edit.dereference(parameter)?;
    let values = edit.tuple_field(row, usize::from(key.monotone()))?;
    let compressed = fields[..index].iter().filter(|f| f.monotone()).count();
    let timestamp = edit.tuple_field(values, compressed)?;
    let closure = edit.closure_from_variables(timestamp, &[parameter])?;
    let closure = apply_waterlines(edit, closure.into(), &[source.ty], next, warnings)?;
    Ok(Some(apply_bound(
        edit,
        edit.circuit().root_id(),
        source,
        closure,
    )?))
}

fn timestamp_projection(
    edit: &CircuitEditor,
    stream: OutputPort,
    index: usize,
) -> Result<Projection, Error> {
    let stream = input_type(edit, stream)?;
    let value = edit.node(stream.into())?.edge("elementType")?;
    let width = edit.node(value)?.edges("tupFields")?.len();
    if index >= width {
        return Err(Error::new("ASOF timestamp outside its value tuple"));
    }
    Ok(Projection::Tuple {
        fields: (0..width)
            .map(|i| Projection::Scalar {
                monotone: i == index,
            })
            .collect(),
        raw: false,
        nullable: false,
    })
}

impl JoinTerm {
    pub(in crate::limiters) fn insert_asof(
        &self,
        edit: &mut CircuitEditor,
        join: &JoinInputs<'_>,
        next: &mut u64,
        warnings: &mut Vec<SimplifyWarning>,
    ) -> Result<Option<(Bound, &Projection)>, Error> {
        let node = edit.node(join.owner)?;
        let comparator = edit.expression(node.edge("comparator")?)?;
        if edit.expr_kind(comparator)? != ExpressionKind::DirectComparator
            || edit
                .node(comparator.into())?
                .data("ascending")
                .and_then(|v| v.as_bool())
                != Some(true)
        {
            return Ok(None);
        }
        let column = |name| {
            node.data(name)
                .and_then(|v| v.as_u64())
                .and_then(|n| usize::try_from(n).ok())
                .ok_or_else(|| Error::new(format!("invalid ASOF {name}")))
        };
        let (left_index, right_index) = (
            column("leftTimestampIndex")?,
            column("rightTimestampIndex")?,
        );
        let result = self.insert(
            edit,
            join.owner,
            join.functions,
            join.expanded_bounds,
            next,
            warnings,
        )?;
        let Some(right) = timestamp_bound(
            edit,
            join.original_bounds[1],
            self.right.as_ref(),
            right_index,
            next,
            warnings,
        )?
        else {
            return Ok(result);
        };
        let body = edit.circuit().root_id();

        let parameter = arg(edit, right.ty, next)?;
        let read = edit.dereference(parameter)?;
        let tuple = edit.tuple_expr(read.into(), false, &[read])?;
        let closure = edit.closure_from_variables(tuple, &[parameter])?;
        let project = apply_waterlines(edit, closure.into(), &[right.ty], next, warnings)?;
        let bound = apply_bound(edit, body, right, project)?;
        let left_projection = timestamp_projection(edit, join.data[0], left_index)?;
        ValueRetention::Matching.insert(
            edit,
            join.owner,
            join.data[0],
            &left_projection,
            bound,
            next,
        )?;
        edit.place_before(body, right.port.operator, join.owner)?;

        if let Some(left) = timestamp_bound(
            edit,
            join.original_bounds[0],
            self.left.as_ref(),
            left_index,
            next,
            warnings,
        )? {
            let l = arg(edit, left.ty, next)?;
            let r = arg(edit, right.ty, next)?;
            let lhs = edit.dereference(l)?;
            let rhs = edit.dereference(r)?;
            let min = minimum_pair(edit, lhs, rhs)?;
            let tuple = edit.tuple_expr(min.into(), false, &[min])?;
            let closure = edit.closure_from_variables(tuple, &[l, r])?;
            let project =
                apply_waterlines(edit, closure.into(), &[left.ty, right.ty], next, warnings)?;
            let bound = apply_bound_pair(edit, [left, right], project)?;
            let right_projection = timestamp_projection(edit, join.data[1], right_index)?;
            ValueRetention::MatchingAndLast.insert(
                edit,
                join.owner,
                join.data[1],
                &right_projection,
                bound,
                next,
            )?;
            edit.place_before(body, left.port.operator, join.owner)?;
        }
        Ok(result)
    }
}
