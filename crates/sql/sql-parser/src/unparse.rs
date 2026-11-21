//! SQL emission from represented syntax, without access to the original source.
//!
//! The vendored candidate formats nested ROW fields and MAP entries in Feldera
//! syntax. Function nullability needs declaration context: the parser stores
//! nullable types in Nullable and non-null types without that wrapper.

use crate::Statement;
use sqlparser_064::ast::{
    CreateFunction, CreateFunctionBody, DataType, FunctionReturnType, OperateFunctionArg,
    Statement as UpstreamStatement,
};

/// Render each parsed statement in the Feldera dialect, preserving its boundary.
pub fn emit_statements(statements: &[Statement]) -> Result<Vec<String>, String> {
    statements.iter().map(statement).collect()
}

fn statement(statement: &Statement) -> Result<String, String> {
    if let Statement::Upstream(node) = statement {
        if let UpstreamStatement::CreateFunction(function) = node.as_ref() {
            return function_declaration(function);
        }
    }
    Ok(statement.to_string())
}

fn nullable_type(ty: &DataType) -> String {
    match ty {
        DataType::Nullable(inner) => format!("{inner} NULL"),
        ty => format!("{ty} NOT NULL"),
    }
}

fn argument(argument: &OperateFunctionArg) -> Result<String, String> {
    let OperateFunctionArg {
        mode: None,
        name: Some(name),
        data_type,
        default_expr,
    } = argument
    else {
        return Err("function argument has no Feldera name or has a foreign mode".into());
    };
    let mut sql = format!("{name} {}", nullable_type(data_type));
    if let Some(default) = default_expr {
        sql.push_str(&format!(" DEFAULT {default}"));
    }
    Ok(sql)
}

fn function_declaration(function: &CreateFunction) -> Result<String, String> {
    // Do not silently discard upstream options if the adapter grows new forms.
    let CreateFunction {
        or_alter: false,
        or_replace,
        temporary: false,
        if_not_exists,
        name,
        args: Some(args),
        return_type: Some(FunctionReturnType::DataType(return_type)),
        function_body,
        behavior: None,
        called_on_null: None,
        parallel: None,
        security: None,
        set_params,
        using: None,
        language: None,
        determinism_specifier: None,
        options: None,
        remote_connection: None,
    } = function
    else {
        return Err("function declaration has fields outside Feldera SQL emission".into());
    };
    if !set_params.is_empty() {
        return Err("function SET clauses have no Feldera SQL emission".into());
    }
    let args = args.iter().map(argument).collect::<Result<Vec<_>, _>>()?;
    let mut sql = format!(
        "CREATE {}FUNCTION {}{name}({}) RETURNS {}",
        if *or_replace { "OR REPLACE " } else { "" },
        if *if_not_exists { "IF NOT EXISTS " } else { "" },
        args.join(", "),
        nullable_type(return_type)
    );
    match function_body {
        None => (),
        Some(CreateFunctionBody::AsBeforeOptions {
            body,
            link_symbol: None,
        }) => {
            sql.push_str(&format!(" AS {body}"));
        }
        _ => return Err("function body has no Feldera expression emission".into()),
    }
    Ok(sql)
}
