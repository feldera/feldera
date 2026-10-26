use super::error::BackoffError;
use crate::util::truncate_ellipse_middle;
use feldera_types::{
    program_schema::Relation,
    transport::postgres::{PostgresWriteMode, PostgresWriterConfig},
};
use itertools::Itertools;
use postgres::Statement;

/// Maximum length of a generated query echoed back in an error message.
///
/// A query names every column of the target table, so a wide table yields a
/// query too long to keep in the error list served by `/stats`.
const MAX_QUERY_LEN_IN_ERRMSG: usize = 2048;

#[derive(Debug, Default)]
struct RawQueries {
    insert: String,
    upsert: String,
    delete: String,
}

impl RawQueries {
    fn new(key_schema: &Relation, value_schema: &Relation, config: &PostgresWriterConfig) -> Self {
        let table = &config.table;
        let keys: Vec<String> = key_schema
            .fields
            .iter()
            .map(|f| f.name.sql_name())
            .collect();

        // List supplied columns explicitly so Postgres applies DEFAULTs to any
        // omitted column, instead of SELECT * forcing NULL into them (#6694).
        let mut insert_columns: Vec<String> = value_schema
            .fields
            .iter()
            .map(|f| f.name.sql_name())
            .chain(config.extra_columns.iter().map(|k| format!(r#""{k}""#)))
            .collect();

        // CDC rows also carry the op/ts metadata columns.
        if matches!(config.mode, PostgresWriteMode::Cdc) {
            insert_columns.push(format!(r#""{}""#, config.cdc_op_column));
            insert_columns.push(format!(r#""{}""#, config.cdc_ts_column));
        }
        let insert_columns = insert_columns.join(", ");

        let mut raw_queries = RawQueries::default();

        match config.mode {
            PostgresWriteMode::Cdc => {
                // In CDC mode, everything is an INSERT into the event log
                raw_queries.insert = format!(
                    r#"INSERT INTO "{table}" ({insert_columns}) SELECT {insert_columns} FROM jsonb_populate_recordset(NULL::"{table}", $1::jsonb)"#,
                );
                // For CDC mode, upsert and delete operations also use INSERT
                raw_queries.upsert = raw_queries.insert.clone();
                raw_queries.delete = raw_queries.insert.clone();
            }
            PostgresWriteMode::Materialized => {
                let on_conflict = if config.on_conflict_do_nothing {
                    " DO NOTHING".to_owned()
                } else {
                    let keys = keys.join(", ");

                    let columns: String = value_schema
                        .fields
                        .iter()
                        .map(|f| {
                            let f = f.name.sql_name();
                            format!(r#" {f} = EXCLUDED.{f} "#)
                        })
                        .chain(
                            config
                                .extra_columns
                                .iter()
                                .map(|k| format!(r#" "{k}" = EXCLUDED."{k}" "#)),
                        )
                        .join(", ");

                    format!(" ({keys}) DO UPDATE SET {columns}")
                };

                raw_queries.insert = format!(
                    r#"INSERT INTO "{table}" ({insert_columns}) SELECT {insert_columns} FROM jsonb_populate_recordset(NULL::"{table}", $1::jsonb) ON CONFLICT {on_conflict}"#,
                );
            }
        }

        // Only generate DELETE and UPDATE queries for normal mode
        if matches!(config.mode, PostgresWriteMode::Materialized) {
            {
                let (table_keys, d_keys): (Vec<_>, Vec<_>) = keys
                    .iter()
                    .map(|k| (format!(r#" "{table}".{k} "#), format!("d.{k}")))
                    .unzip();

                raw_queries.delete = format!(
                    r#"DELETE FROM "{table}" USING (SELECT {} FROM jsonb_populate_recordset(NULL::"{table}", $1::jsonb)) as d where ({}) = ({})"#,
                    keys.iter()
                        .map(|k| k.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    table_keys.join(", "),
                    d_keys.join(", "),
                );
            }

            {
                let table_alias = "t";
                let new_alias = "n";
                let columns = value_schema
                    .fields
                    .iter()
                    .map(|f| {
                        let f = f.name.sql_name();
                        format!("{f} = {new_alias}.{f}")
                    })
                    .chain(
                        config
                            .extra_columns
                            .iter()
                            .map(|k| format!(r#""{k}" = {new_alias}."{k}""#)),
                    )
                    .collect::<Vec<_>>()
                    .join(", ");

                let (table_fields, new_fields): (Vec<_>, Vec<_>) = keys
                    .iter()
                    .map(|f| (format!("{table_alias}.{f}"), format!("{new_alias}.{f}")))
                    .unzip();

                raw_queries.upsert = format!(
                    r#"UPDATE "{table}" AS {table_alias} SET {columns} FROM (SELECT * FROM jsonb_populate_recordset(NULL::"{table}", $1::jsonb)) AS {new_alias} WHERE ({}) = ({})"#,
                    table_fields.join(", "),
                    new_fields.join(", ")
                );
            }
        }

        raw_queries
    }
}

#[derive(Debug)]
pub(super) struct PreparedStatements {
    pub insert: Statement,
    pub upsert: Statement,
    pub delete: Statement,
}

impl PreparedStatements {
    pub fn new(
        key_schema: &Relation,
        value_schema: &Relation,
        config: &PostgresWriterConfig,
        client: &mut postgres::Client,
    ) -> Result<Self, BackoffError> {
        let raw_queries = RawQueries::new(key_schema, value_schema, config);

        let err_msg = "\nPlease ensure all field names that are quoted in PostgreSQL are quoted correctly in Feldera as well";

        let insert = client
            .prepare_typed(&raw_queries.insert, &[postgres::types::Type::VARCHAR])
            .map_err(|e| {
                BackoffError::from(e).context(format!(
                    "failed to prepare insert statement: `{}`: {err_msg}",
                    truncate_ellipse_middle(&raw_queries.insert, MAX_QUERY_LEN_IN_ERRMSG)
                ))
            })?;
        let upsert = client
            .prepare_typed(&raw_queries.upsert, &[postgres::types::Type::VARCHAR])
            .map_err(|e| {
                BackoffError::from(e).context(format!(
                    "failed to prepare update statement: `{}`: {err_msg}",
                    truncate_ellipse_middle(&raw_queries.upsert, MAX_QUERY_LEN_IN_ERRMSG)
                ))
            })?;
        let delete = client
            .prepare_typed(&raw_queries.delete, &[postgres::types::Type::VARCHAR])
            .map_err(|e| {
                BackoffError::from(e).context(format!(
                    "failed to prepare delete statement: `{}`: {err_msg}",
                    truncate_ellipse_middle(&raw_queries.delete, MAX_QUERY_LEN_IN_ERRMSG)
                ))
            })?;

        Ok(PreparedStatements {
            insert,
            upsert,
            delete,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_types::program_schema::{ColumnType, Field};
    use std::collections::BTreeMap;

    fn relation(name: &str, columns: &[&str]) -> Relation {
        Relation::new(
            name.into(),
            columns
                .iter()
                .map(|c| Field::new((*c).into(), ColumnType::varchar(true)))
                .collect(),
            false,
            BTreeMap::new(),
        )
    }

    fn writer_config(mode: PostgresWriteMode, extra_columns: &[&str]) -> PostgresWriterConfig {
        serde_json::from_value(serde_json::json!({
            "uri": "postgres://localhost",
            "table": "t",
            "mode": mode.to_string(),
            "extra_columns": extra_columns,
        }))
        .unwrap()
    }

    // INSERT lists supplied columns, never SELECT * (#6694).
    #[test]
    fn materialized_insert_lists_supplied_columns() {
        let key = relation("k", &["id"]);
        let value = relation("v", &["id", "name"]);
        let queries = RawQueries::new(
            &key,
            &value,
            &writer_config(PostgresWriteMode::Materialized, &["audit"]),
        );

        assert!(!queries.insert.contains("SELECT *"), "{}", queries.insert);
        assert!(queries.insert.contains(r#"(id, name, "audit")"#));
        assert!(queries.insert.contains(r#"SELECT id, name, "audit" FROM"#));
    }

    // Case-sensitive columns stay quoted.
    #[test]
    fn materialized_insert_quotes_case_sensitive_columns() {
        let key = relation("k", &[r#""Id""#]);
        let value = relation("v", &[r#""Id""#, r#""Name""#]);
        let queries = RawQueries::new(
            &key,
            &value,
            &writer_config(PostgresWriteMode::Materialized, &[]),
        );

        assert!(queries.insert.contains(r#"("Id", "Name")"#));
        assert!(queries.insert.contains(r#"SELECT "Id", "Name" FROM"#));
    }

    // CDC INSERT also lists the op/ts metadata columns.
    #[test]
    fn cdc_insert_lists_supplied_and_metadata_columns() {
        let key = relation("k", &["id"]);
        let value = relation("v", &["id", "name"]);
        let queries = RawQueries::new(&key, &value, &writer_config(PostgresWriteMode::Cdc, &[]));

        assert!(!queries.insert.contains("SELECT *"));
        assert!(
            queries
                .insert
                .contains(r#"(id, name, "__feldera_op", "__feldera_ts")"#)
        );
        assert!(
            queries
                .insert
                .contains(r#"SELECT id, name, "__feldera_op", "__feldera_ts" FROM"#)
        );
        assert_eq!(queries.insert, queries.upsert);
        assert_eq!(queries.insert, queries.delete);
    }
}
