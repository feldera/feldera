//! Retain values in one input trace using the filtered delta from the other.
use super::{Bound, JoinInputs, Projection};
use crate::limiters::filter::arg;
use crate::limiters::pass::apply_bound;
use crate::limiters::{apply_waterlines, retention::ValueRetention};
use crate::projection::join_projection;
use crate::rebuild::input_type;
use feldera_sql_expr::SimplifyWarning;
use feldera_sql_ir::{CircuitEditor, Error};

pub(super) fn retain_values(
    edit: &mut CircuitEditor,
    join: &JoinInputs<'_>,
    opposite: [Option<(Bound, &Projection)>; 2],
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<(), Error> {
    let closure = edit.node(join.owner)?.edge("function")?;
    let columns = join_projection(edit, closure)?;
    let signature = edit.node(closure)?.edge("type")?;
    let joined_row = edit.type_id(edit.node(signature)?.edge("resultType")?)?;
    let body = edit.circuit().root_id();
    let mut retained_left = false;
    for (side, bound) in opposite.into_iter().enumerate() {
        let Some((bound, projection)) = bound else {
            continue;
        };
        let Projection::Tuple { fields, .. } = projection else {
            return Err(Error::new("filtered join bound requires an output tuple"));
        };
        if fields.len() != columns.len() {
            return Err(Error::new(
                "join output and waterline projection differ in width",
            ));
        }

        let data = join.data[side];
        let stream = input_type(edit, data)?;
        let row = edit.type_id(edit.node(stream.into())?.edge("elementType")?)?;
        let width = edit.node(row.into())?.edges("tupFields")?.len();
        // Java allocates this parameter even when there are no fields to retain.
        let projected_type = projection.projected_type(edit, joined_row)?;
        let parameter = arg(edit, projected_type, next)?;
        let mut reads = Vec::new();
        let mut selected = Vec::with_capacity(width);
        for column in 0..width {
            // Parameter zero is the shared key. Repeated columns use their first output.
            let output = columns
                .iter()
                .position(|&input| input == (side + 1, column));
            if let Some(index) = output.filter(|&index| fields[index].monotone()) {
                let compact_index = fields[..index]
                    .iter()
                    .filter(|field| field.monotone())
                    .count();
                let value = edit.dereference(parameter)?;
                reads.push(edit.tuple_field(value, compact_index)?);
                selected.push(fields[index].clone());
            } else {
                selected.push(Projection::Scalar { monotone: false });
            }
        }
        if reads.is_empty() {
            continue;
        }
        if side == 1 && retained_left && join.data[0] == data {
            return Err(Error::new(
                "both retained inputs of a self-join read the same trace",
            ));
        }
        retained_left = side == 0;
        let tuple = edit.tuple_expr(parameter.into(), false, &reads)?;
        let extract = edit.closure_from_variables(tuple, &[parameter])?;
        let extract = apply_waterlines(edit, extract.into(), &[bound.ty], next, warnings)?;
        let waterline = apply_bound(edit, body, bound, extract)?;
        let shape = Projection::Tuple {
            fields: selected,
            raw: false,
            nullable: false,
        };
        ValueRetention::Matching.insert(edit, join.owner, data, &shape, waterline, next)?;
        edit.place_before(body, waterline.port.operator, join.owner)?;
    }
    Ok(())
}
