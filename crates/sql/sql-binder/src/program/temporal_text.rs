//! Bind datetime parsing and formatting without evaluating the format string.

use super::{
    ConstantOperand, graph,
    literal::{Cell, temporal},
    source::{DeclarationTokens, SqlDisplay},
};
use crate::{Expr, Failure, unsupported};
use feldera_sql_rel_model::{
    expression_objects::ObjectPosition, literals::LiteralValue, operators::TemporalTextFunction,
    program::Program, relation_metadata::Coordinates, types::TypeKind,
};

pub(super) fn bind(
    program: &mut Program,
    function: &TemporalTextFunction,
    arguments: &[&Expr],
    tokens: &DeclarationTokens<'_>,
    position: Coordinates,
    fold: bool,
) -> Result<ConstantOperand, Failure> {
    let &[pattern_expr, value_expr] = arguments else {
        return Err(unsupported(
            "datetime text functions require a pattern and a value",
        ));
    };
    let pattern = Cell::parse(pattern_expr)?;
    let mut value = Cell::parse(value_expr)?;
    let wrapped = pattern.needs_non_bmp_wrapper() || value.needs_non_bmp_wrapper();
    let mut cast = None;
    let mut reduction_changed = false;
    if let Some((kind, name)) = function.format_input()
        && matches!(value.kind, TypeKind::Character { .. })
    {
        value.value = match &value.value {
            LiteralValue::Null {} => LiteralValue::Null {},
            LiteralValue::Character { value: text } => {
                // A failed sibling reduction keeps the DATE cast in the project.
                reduction_changed = matches!(kind, TypeKind::Date {});
                if reduction_changed && !fold {
                    return Err(unsupported("unreduced character-to-DATE cast"));
                }
                temporal::coerce_text(&kind, text)
                    .ok_or_else(|| unsupported("datetime text argument requires a runtime cast"))?
            }
            _ => return Err(unsupported("expected a character literal")),
        };
        value.kind = kind;
        cast = Some(name);
    }
    let (left, left_ty) = pattern.emit(program, tokens, pattern_expr)?;
    let (right, right_ty) = value.emit(program, tokens, value_expr)?;
    let types = &program.table.type_table.types;
    let result = function
        .result(&types[left_ty.0 as usize], &types[right_ty.0 as usize])
        .ok_or_else(|| unsupported("datetime text operands require coercion"))?;
    let ty = graph::insert_type(&mut program.table.type_table, result);
    let root = graph::append_constant_call(
        program,
        function.operator(),
        vec![left, right],
        ty,
        ObjectPosition::user(position),
    );
    let render = |quoted| {
        let name = if wrapped {
            let name = function.sql_name.to_ascii_lowercase();
            if quoted { format!("`{name}`") } else { name }
        } else {
            function.sql_name.to_owned()
        };
        let argument = value.text.render(quoted);
        let argument = match cast.filter(|_| !wrapped) {
            Some(target) => format!("CAST({argument} AS {target})"),
            None => argument.into_owned(),
        };
        let pattern = if cast.is_some() && !wrapped && matches!(pattern.kind, TypeKind::Null {}) {
            if quoted {
                "CAST(NULL AS VARCHAR CHARACTER SET `UTF-8`)".into()
            } else {
                "CAST(NULL AS VARCHAR)".into()
            }
        } else {
            pattern.text.render(quoted)
        };
        format!("{name}({pattern}, {argument})")
    };
    let mut display = SqlDisplay::atom(render(true));
    display.diagnostic = render(false);
    Ok(ConstantOperand {
        root,
        ty,
        folded: false,
        reduction_changed,
        display,
    })
}
