use super::error::BackoffError;
use feldera_types::{program_schema::Relation, transport::postgres::PostgresWriterConfig};
use postgres::Statement;

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

        let mut raw_queries = RawQueries::default();

        {
            let on_conflict = if config.on_conflict_do_nothing {
                " DO NOTHING".to_owned()
            } else {
                let keys = keys
                    .iter()
                    .map(|k| k.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");

                let columns: String = value_schema
                    .fields
                    .iter()
                    .map(|f| {
                        let f = f.name.sql_name();
                        format!(r#" {f} = EXCLUDED.{f} "#)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");

                format!(" ({keys}) DO UPDATE SET {columns}")
            };

            raw_queries.insert = format!(
                r#"INSERT INTO "{table}" SELECT * FROM jsonb_populate_recordset(NULL::"{table}", $1::jsonb) ON CONFLICT {on_conflict}"#,
            );
        }

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
        let raw_queries = RawQueries::new(key_schema, value_schema, &config);

        let err_msg = "\nPlease ensure all field names that are quoted in PostgreSQL are quoted correctly in Feldera as well";

        let insert = client
            .prepare_typed(&raw_queries.insert, &[postgres::types::Type::VARCHAR])
            .map_err(|e| {
                BackoffError::from(e).context(format!(
                    "failed to prepare insert statement: `{}`: {err_msg}",
                    &raw_queries.insert
                ))
            })?;
        let upsert = client
            .prepare_typed(&raw_queries.upsert, &[postgres::types::Type::VARCHAR])
            .map_err(|e| {
                BackoffError::from(e).context(format!(
                    "failed to prepare update statement: `{}`: {err_msg}",
                    &raw_queries.upsert
                ))
            })?;
        let delete = client
            .prepare_typed(&raw_queries.delete, &[postgres::types::Type::VARCHAR])
            .map_err(|e| {
                BackoffError::from(e).context(format!(
                    "failed to prepare delete statement: `{}`: {err_msg}",
                    raw_queries.delete
                ))
            })?;

        Ok(PreparedStatements {
            insert,
            upsert,
            delete,
        })
    }
}
