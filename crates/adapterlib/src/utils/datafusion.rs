use crate::errors::journal::ControllerError;
use anyhow::{anyhow, Error as AnyError};
use arrow::util::pretty::pretty_format_batches;
use datafusion::common::arrow::array::{AsArray, RecordBatch};
use datafusion::logical_expr::sqlparser::parser::ParserError;
use datafusion::prelude::{SQLOptions, SessionContext};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use feldera_types::program_schema::{ColumnType, Field, Relation, SqlType};

/// Execute a SQL query and collect all results in a vector of `RecordBatch`'s.
pub async fn execute_query_collect(
    datafusion: &SessionContext,
    query: &str,
) -> Result<Vec<RecordBatch>, AnyError> {
    let options = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false);

    let df = datafusion
        .sql_with_options(query, options)
        .await
        .map_err(|e| anyhow!("error compiling query '{query}': {e}"))?;

    df.collect()
        .await
        .map_err(|e| anyhow!("error executing query '{query}': {e}"))
}

/// Execute a SQL query that returns a result with exactly one row and column of type `string`.
pub async fn execute_singleton_query(
    datafusion: &SessionContext,
    query: &str,
) -> Result<String, AnyError> {
    let result = execute_query_collect(datafusion, query).await?;
    if result.len() != 1 {
        return Err(anyhow!(
            "internal error: query '{query}' returned {} batches; expected: 1",
            result.len()
        ));
    }

    if result[0].num_rows() != 1 {
        return Err(anyhow!(
            "internal error: query '{query}' returned {} rows; expected: 1",
            result[0].num_rows()
        ));
    }

    if result[0].num_columns() != 1 {
        return Err(anyhow!(
            "internal error: query '{query}' returned {} columns; expected: 1",
            result[0].num_columns()
        ));
    }

    Ok(result[0].column(0).as_string::<i32>().value(0).to_string())
}

/// Execute a SQL query that returns a result with exactly one row and column of type `string`.
pub async fn execute_query_text(
    datafusion: &SessionContext,
    query: &str,
) -> Result<String, AnyError> {
    let options = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false);

    let df = datafusion
        .sql_with_options(query, options)
        .await
        .map_err(|e| anyhow!("error compiling query '{query}': {e}"))?;

    Ok(pretty_format_batches(
        &df.collect()
            .await
            .map_err(|e| anyhow!("error executing query '{query}': {e}"))?,
    )
    .map_err(|e| anyhow!("error formatting result of query '{query}': {e}"))?
    .to_string())
}

/// Parse expression only to validate it.
pub fn validate_sql_expression(expr: &str) -> Result<(), ParserError> {
    let mut parser = Parser::new(&GenericDialect).try_with_sql(expr)?;
    parser.parse_expr()?;

    Ok(())
}

/// Convert a value of the timestamp column returned by a SQL query into a valid
/// SQL expression.
pub fn timestamp_to_sql_expression(column_type: &ColumnType, expr: &str) -> String {
    match column_type.typ {
        SqlType::Timestamp => format!("timestamp '{expr}'"),
        SqlType::Date => format!("date '{expr}'"),
        _ => expr.to_string(),
    }
}

/// Check that the `timestamp` field has one of supported types.
pub fn validate_timestamp_type(
    endpoint_name: &str,
    timestamp: &Field,
    docs: &str,
) -> Result<(), ControllerError> {
    if !timestamp.columntype.is_integral_type()
        && !matches!(
            &timestamp.columntype.typ,
            SqlType::Date | SqlType::Timestamp
        )
    {
        return Err(ControllerError::invalid_transport_configuration(
                    endpoint_name,
                    &format!(
                        "timestamp column '{}' has unsupported type {}; supported types for 'timestamp_column' are integer types, DATE, and TIMESTAMP; {docs}",
                        timestamp.name,
                        serde_json::to_string(&timestamp.columntype).unwrap()
                    ),
                ));
    }

    Ok(())
}

/// Validate 'timestamp_column'.
pub async fn validate_timestamp_column(
    endpoint_name: &str,
    timestamp_column: &str,
    datafusion: &SessionContext,
    schema: &Relation,
    docs: &str,
) -> Result<(), ControllerError> {
    // Lookup column in the schema.
    let Some(field) = schema.field(timestamp_column) else {
        return Err(ControllerError::invalid_transport_configuration(
            endpoint_name,
            &format!("timestamp column '{timestamp_column}' not found in table schema"),
        ));
    };

    // Field must have a supported type.
    validate_timestamp_type(endpoint_name, field, docs)?;

    // Column must have lateness.
    let Some(lateness) = &field.lateness else {
        return Err(ControllerError::invalid_transport_configuration(
            endpoint_name,
            &format!(
                "timestamp column '{timestamp_column}' does not have a LATENESS attribute; {docs}"
            ),
        ));
    };

    // Validate lateness expression.
    validate_sql_expression(lateness).map_err(|e|
                ControllerError::invalid_transport_configuration(
                    endpoint_name,
                    &format!("error parsing LATENESS attribute '{lateness}' of the timestamp column '{timestamp_column}': {e}; {docs}"),
                ),
            )?;

    // Lateness has to be >0. Zero would mean that we need to ingest data strictly in order. If we need to support this case in the future,
    // we could revert to our old (and very costly) strategy of issuing a single `select *` query with the 'ORDER BY timestamp_column' clause,
    // which requires storing and sorting the entire collection locally.
    let is_zero = execute_singleton_query(
        datafusion,
        &format!("select cast(({lateness} + {lateness}) = {lateness} as string)"),
    )
    .await
    .map_err(|e| ControllerError::invalid_transport_configuration(endpoint_name, &e.to_string()))?;

    if &is_zero == "true" {
        return Err(ControllerError::invalid_transport_configuration(endpoint_name, &format!("invalid LATENESS attribute '{lateness}' of the timestamp column '{timestamp_column}': LATENESS must be greater than zero; {docs}")));
    }

    Ok(())
}
