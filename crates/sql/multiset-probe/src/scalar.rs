//! Evaluation of supported scalar expressions on constant rows.

use crate::model::{Arithmetic, ArithmeticKind, ConstantLiterals, ScalarLiteral, ScalarOperand};
use crate::{Error, Result};

pub(crate) struct EvaluatedScalar {
    pub value: ScalarLiteral,
    /// The existing literal, or the literal whose type and position are inherited.
    pub source: u64,
    pub computed: bool,
}

pub(crate) fn evaluate(
    literals: &ConstantLiterals,
    row: u64,
    operand: &ScalarOperand,
) -> Result<Option<EvaluatedScalar>> {
    let (source, value) = match operand {
        ScalarOperand::Field(index) => column(literals, row, *index)?,
        ScalarOperand::Arithmetic(expression) => return arithmetic(literals, row, expression),
        ScalarOperand::Literal(id) => (*id, lookup(literals, *id)?),
        ScalarOperand::Row => (row, lookup(literals, row)?),
    };
    Ok(Some(EvaluatedScalar {
        value,
        source,
        computed: false,
    }))
}

fn lookup(literals: &ConstantLiterals, id: u64) -> Result<ScalarLiteral> {
    literals
        .scalars
        .get(&id)
        .copied()
        .ok_or_else(|| Error("missing scalar constant literal".into()))
}

pub(crate) fn column(
    literals: &ConstantLiterals,
    row: u64,
    index: usize,
) -> Result<(u64, ScalarLiteral)> {
    let id = literals
        .tuples
        .get(&row)
        .and_then(|fields| fields.get(index))
        .ok_or_else(|| Error("constant projection needs a tuple field".into()))?;
    Ok((*id, lookup(literals, *id)?))
}

pub(crate) fn arithmetic(
    literals: &ConstantLiterals,
    row: u64,
    expression: &Arithmetic,
) -> Result<Option<EvaluatedScalar>> {
    let left = evaluate(literals, row, &expression.left)?;
    let right = evaluate(literals, row, &expression.right)?;
    let Some(left) = left else {
        if let Some(right) = right {
            let value = small_integer(&right)?;
            if value.is_none()
                || (matches!(expression.kind, ArithmeticKind::Multiply)
                    && value == Some(0)
                    && !expression.left_nullable)
            {
                return Ok(Some(right));
            }
        }
        return Ok(None);
    };
    let Some(a) = small_integer(&left)? else {
        return Ok(Some(left));
    };
    // Simplify can reuse a literal object instead of creating another node.
    match expression.kind {
        ArithmeticKind::Add if a == 0 => return Ok(right),
        ArithmeticKind::Multiply if a == 1 => return Ok(right),
        ArithmeticKind::Multiply if a == 0 && !expression.right_nullable => return Ok(Some(left)),
        _ => {}
    }
    let Some(right) = right else {
        return Ok(None);
    };
    if std::mem::discriminant(&left.value) != std::mem::discriminant(&right.value) {
        return Ok(None);
    }
    let Some(b) = small_integer(&right)? else {
        return Ok(Some(right));
    };
    let value = match expression.kind {
        ArithmeticKind::Add => a.checked_add(b),
        ArithmeticKind::Subtract => a.checked_sub(b),
        ArithmeticKind::Multiply => a.checked_mul(b),
    };
    let value = value.and_then(|value| match left.value {
        ScalarLiteral::Int8(_) => i8::try_from(value)
            .ok()
            .map(|value| ScalarLiteral::Int8(Some(value))),
        ScalarLiteral::Int32(_) => Some(ScalarLiteral::Int32(Some(value))),
        _ => unreachable!("arithmetic checks operand classes"),
    });
    Ok(value.map(|value| EvaluatedScalar {
        value,
        source: left.source,
        computed: true,
    }))
}

fn small_integer(scalar: &EvaluatedScalar) -> Result<Option<i32>> {
    match scalar.value {
        ScalarLiteral::Int8(value) => Ok(value.map(i32::from)),
        ScalarLiteral::Int32(value) => Ok(value),
        _ => Err(Error(
            "constant arithmetic requires signed 8- or 32-bit operands".into(),
        )),
    }
}
