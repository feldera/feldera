//! Lower Feldera's datetime formatting and parsing calls over literal operands.

use super::literal::ProjectedLiteral;
use crate::{
    source_table::{expression_range, ColumnLayout, Ids},
    values::scalar::ScalarLiteral,
    Error,
};
use feldera_sql_ir::TypeKind as CircuitType;
use feldera_sql_rel_model::{
    expression_objects::ObjectKind,
    expressions::ExprId,
    literals::LiteralValue,
    operators::TemporalTextFunction,
    program::Program,
    types::{SqlType, TypeKind},
};
use serde_json::Value;

fn lower_argument(literal: &ProjectedLiteral, ids: &mut Ids, parse: bool) -> Result<Value, Error> {
    let value = literal.lower(ids)?;
    if parse && matches!(literal.ty.kind, TypeKind::Null {}) {
        let mut cast = ids.generated("DBSPCastExpression");
        cast["source"] = value;
        cast["type"] = ids.singleton(CircuitType::String, "STRING", true);
        cast["safe"] = "SqlUnsafe".into();
        return Ok(cast);
    }
    Ok(value)
}

pub(super) struct TemporalCall {
    function: &'static TemporalTextFunction,
    arguments: [ProjectedLiteral; 2],
    result: SqlType,
}

impl TemporalCall {
    pub(super) fn resolve(
        program: &Program,
        function: &'static TemporalTextFunction,
        args: &[ExprId],
        result: &SqlType,
    ) -> Result<Self, Error> {
        let &[pattern, value] = args else {
            return Err(Error("datetime text functions require two operands".into()));
        };
        let ty = |id: ExprId| {
            &program.table.type_table.types
                [program.expressions.expressions[id.0 as usize].ty.0 as usize]
        };
        if !function
            .result(ty(pattern), ty(value))
            .is_some_and(|inferred| {
                inferred.kind == result.kind && inferred.nullable == result.nullable
            })
        {
            return Err(Error("invalid datetime text function signature".into()));
        }
        let literal = |id| match ProjectedLiteral::wrapped(program, id)? {
            Some(value) => Ok(value),
            None => ProjectedLiteral::resolve(program, id),
        };
        Ok(Self {
            function,
            arguments: [literal(pattern)?, literal(value)?],
            result: result.clone(),
        })
    }

    pub(super) fn sql(&self) -> String {
        let [a, b] = &self.arguments;
        format!(
            "{}({}, {})",
            self.function.sql_name, a.value.sql, b.value.sql
        )
    }

    pub(super) fn digest(&self) -> String {
        let [a, b] = &self.arguments;
        format!(
            "{}({}, {})",
            self.function.sql_name.to_ascii_lowercase(),
            a.digest,
            b.digest
        )
    }

    pub(super) fn lower(
        &self,
        ids: &mut Ids,
        expression: &super::computed::Computed,
    ) -> Result<Value, Error> {
        let parse = self.function.parses_text();
        // ExpressionCompiler casts untyped NULL to VARCHAR before testing if
        // any argument of a strict PARSE function is a null literal.
        if parse
            && self
                .arguments
                .iter()
                .any(|arg| arg.ty.nullable && !matches!(arg.ty.kind, TypeKind::Null {}))
        {
            let literal = ScalarLiteral::resolve(&self.result, &LiteralValue::Null {})?;
            let mut ty = ColumnLayout::scalar(&self.result)?.node(ids);
            return literal.expression(ids, &self.result, &mut ty, None);
        }
        let [a, b] = &self.arguments;
        let suffix = |arg: &ProjectedLiteral| if arg.ty.nullable { "N" } else { "_" };
        let name = format!(
            "{}{}{}",
            self.function.sql_name.to_ascii_lowercase(),
            suffix(a),
            suffix(b)
        );
        let args = vec![
            lower_argument(a, ids, parse)?,
            lower_argument(b, ids, parse)?,
        ];
        Ok(expression.apply(ids, &name, args))
    }
}

pub(super) fn check_format(
    program: &Program,
    call: &feldera_sql_rel_model::expression_objects::ExpressionObject,
    context: &Value,
    messages: &mut Vec<Value>,
) {
    use feldera_sql_rel_model::{expression_objects::ObjectPosition, operators::OperatorObject};
    let ObjectKind::Call {
        operator,
        arguments,
    } = &call.kind
    else {
        return;
    };
    let graph = &program.expressions;
    let signature = &graph.operators[operator.0 as usize];
    let OperatorObject::Builtin {
        registry,
        registry_version,
        key,
    } = &graph.operator_objects[signature.object.0 as usize]
    else {
        return;
    };
    if registry != "feldera-s1-builtins"
        || registry_version != "1"
        || TemporalTextFunction::from_key(key).is_none()
    {
        return;
    }
    let &[pattern, value] = arguments.as_slice() else {
        return;
    };
    let argument = |id: ExprId| {
        let expr = &graph.expressions[id.0 as usize];
        let literal = match &expr.kind {
            ObjectKind::Literal { literal } => Some(&literal.value),
            _ => program
                .non_bmp_literal_input(id)
                .map(|(_, literal)| &literal.value),
        };
        (
            literal,
            &program.table.type_table.types[expr.ty.0 as usize].kind,
            &expr.position,
        )
    };
    let (fmt, kind, format_pos) = argument(pattern);
    let (data, _, value_pos) = argument(value);
    let (message, source) = match (kind, fmt) {
        (TypeKind::Null {}, _) => (
            "Format argument is expected to be a string, but the type is NULL",
            format_pos,
        ),
        (_, Some(LiteralValue::Null {})) => ("Format argument is NULL.", format_pos),
        (_, Some(LiteralValue::Character { value })) if !value.contains('%') => (
            "Format argument does not look like a format string.",
            format_pos,
        ),
        _ if matches!(data, Some(LiteralValue::Character { value }) if value.contains('%')) => {
            ("Are the two arguments swapped?", value_pos)
        }
        _ => return,
    };
    let source = if matches!(source, ObjectPosition::Absent {}) {
        &call.position
    } else {
        source
    };
    messages.push(serde_json::json!({
        "warning":true, "continuation":false, "type":"Suspicious argument", "message":message,
        "context":context, "range":expression_range(source)
    }));
}
