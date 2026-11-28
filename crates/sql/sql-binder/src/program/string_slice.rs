//! Bind constant character and binary slice calls in the default Calcite profile.

use super::{ConstantOperand, graph, literal::Cell, source::SqlDisplay};
use crate::{Expr, Failure, unsupported};
use feldera_fold_probe::StringSlice;
use feldera_sql_rel_model::{
    expression_objects::ObjectPosition,
    literals::LiteralValue,
    operators::OperatorObject,
    program::Program,
    source::Origin,
    types::{IntegerWidth, SqlType, TypeKind},
};

#[derive(Clone, Copy)]
enum Length {
    Omitted,
    Null,
    Count(i32),
}

impl Length {
    fn from_operand(cell: Option<&Cell>) -> Result<Self, Failure> {
        match cell {
            Some(cell) => Ok(index(cell)?.map_or(Self::Null, Self::Count)),
            None => Ok(Self::Omitted),
        }
    }
}

fn index(cell: &Cell) -> Result<Option<i32>, Failure> {
    match (&cell.kind, &cell.value) {
        (
            TypeKind::Integer {
                width: IntegerWidth::I32,
                signed: true,
            },
            LiteralValue::Integer { value },
        ) => value
            .parse()
            .map(Some)
            .map_err(|_| unsupported("invalid INTEGER slice index")),
        (
            TypeKind::Integer {
                signed: true,
                width: IntegerWidth::I32,
            }
            | TypeKind::Null {},
            LiteralValue::Null {},
        ) => Ok(None),
        _ => Err(unsupported("slice index requires an integer literal")),
    }
}

pub(super) fn bind(
    program: &mut Program,
    name: &str,
    args: &[&Expr],
    fold: bool,
) -> Result<ConstantOperand, Failure> {
    if !fold {
        return Err(unsupported("string slice requires runtime evaluation"));
    }
    let cells = args
        .iter()
        .map(|e| Cell::parse(e))
        .collect::<Result<Vec<_>, _>>()?;
    let (input, start, length) = match cells.as_slice() {
        [input, start] => (input, start, None),
        [input, start, length] if matches!(name, "SUBSTR" | "SUBSTRING") => {
            (input, start, Some(length))
        }
        _ => {
            return Err(unsupported(
                "expected a string, index and optional substring length",
            ));
        }
    };
    let mut kind = input.kind.clone();
    match &mut kind {
        TypeKind::Character { fixed, .. } | TypeKind::Binary { fixed, .. } => *fixed = false,
        TypeKind::Null {} => {}
        _ => return Err(unsupported("expected a character or binary slice operand")),
    }
    if input.needs_non_bmp_wrapper() {
        return Err(unsupported(
            "supplementary string slices require the runtime character conversion",
        ));
    }
    if name == "SUBSTR" && matches!(start.kind, TypeKind::Null {}) {
        return Err(unsupported("SUBSTR requires a typed start index"));
    }
    let start = index(start)?;
    let length = Length::from_operand(length)?;
    let nullable = start.is_none()
        || matches!(length, Length::Null)
        || matches!(input.value, LiteralValue::Null {});
    let slice = |size| match start {
        Some(start) => bounds(name, size, start, length),
        None => Ok(None),
    };
    let length_error = || unsupported("negative SUBSTRING length requires a runtime call");
    let value = match &input.value {
        LiteralValue::Character { value } => slice(value.chars().count())?
            .map(|s| s.text(value).ok_or_else(length_error))
            .transpose()?
            .map(|value| LiteralValue::Character { value }),
        LiteralValue::Binary { value } => slice(value.len())?
            .map(|s| s.bytes(value).ok_or_else(length_error))
            .transpose()?
            .map(|value| LiteralValue::Binary { value }),
        LiteralValue::Null {} => None,
        _ => return Err(unsupported("expected a character or binary literal")),
    };
    let folded = !nullable || value.is_none();
    let result = SqlType {
        kind,
        nullable,
        origin: Origin::Absent {},
    };
    let ty = graph::insert_type(&mut program.table.type_table, result.clone());
    let root = if folded {
        graph::append_literal(program, ty, value.unwrap_or(LiteralValue::Null {}))
    } else {
        // SUBSTR can select the required input even when its length is NULL.
        // The projected type still has the CASE expression's nullability.
        let required = graph::insert_type(
            &mut program.table.type_table,
            SqlType {
                nullable: false,
                ..result
            },
        );
        let literal =
            graph::append_literal(program, required, value.expect("non-null CASE result"));
        graph::append_constant_call(
            program,
            OperatorObject::Builtin {
                registry: crate::frel::CALCITE.into(),
                registry_version: "1".into(),
                key: "SqlStdOperatorTable.CAST".into(),
            },
            vec![literal],
            ty,
            ObjectPosition::Absent {},
        )
    };
    let render = |quoted| {
        let operands = cells
            .iter()
            .map(|c| c.text.render(quoted))
            .collect::<Vec<_>>()
            .join(", ");
        format!("{name}({operands})")
    };
    let mut display = SqlDisplay::atom(render(true));
    display.diagnostic = if name == "SUBSTR" {
        format!("({})", render(false))
    } else {
        render(false)
    };
    Ok(ConstantOperand {
        root,
        ty,
        folded,
        reduction_changed: true,
        display,
    })
}

fn bounds(
    name: &str,
    size: usize,
    start: i32,
    length: Length,
) -> Result<Option<StringSlice>, Failure> {
    let mut start = start;
    let mut length = length;
    match name {
        "LEFT" => return Ok(Some(StringSlice::Left(i128::from(start)))),
        "RIGHT" => return Ok(Some(StringSlice::Right(i128::from(start)))),
        "SUBSTRING" => {}
        "SUBSTR" => {
            // The BigQuery convertlet's three-argument CASE returns the input
            // when start + (length(input) + 1) is less than one, even for a NULL length.
            let after = i32::try_from(size)
                .ok()
                .and_then(|s| s.checked_add(1))
                .ok_or_else(|| unsupported("SUBSTR length exceeds INTEGER"))?;
            if start < 0 || !matches!(length, Length::Omitted) {
                let adjusted = start.checked_add(after).ok_or_else(|| {
                    unsupported("SUBSTR index arithmetic requires a runtime call")
                })?;
                if adjusted < 1 && !matches!(length, Length::Omitted) {
                    return Ok(Some(StringSlice::Left(size as i128)));
                }
                if start < 0 {
                    start = adjusted;
                }
            }
            start = start.max(1);
            if let Length::Count(n) = length {
                length = Length::Count(n.max(0));
            }
        }
        _ => unreachable!("expected a slice function"),
    }
    let length = match length {
        Length::Omitted => None,
        Length::Null => return Ok(None),
        Length::Count(n) => Some(i128::from(n)),
    };
    Ok(Some(StringSlice::Substring {
        start: i128::from(start),
        length,
    }))
}
