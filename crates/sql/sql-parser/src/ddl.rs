//! Complete top-level DDL field policies for the pinned Feldera grammar.
//! Nested columns and constraints have their own policies below.

use sqlparser_064::ast::{
    ColumnDef, ColumnOption, CreateTable, CreateTableOptions, CreateView, Expr,
    ForeignKeyConstraint, HiveDistributionStyle, IndexColumn, ObjectNamePart, OrderByExpr,
    OrderByOptions, PrimaryKeyConstraint, TableConstraint, ViewColumnDef,
};

pub(super) fn table(table: &CreateTable) -> bool {
    let CreateTable {
        or_replace: _,
        temporary,
        unlogged,
        external,
        dynamic,
        global,
        if_not_exists: _,
        transient,
        volatile,
        iceberg,
        snapshot,
        name: _,
        columns: _,
        constraints: _,
        hive_distribution,
        hive_formats,
        table_options: _,
        file_format,
        location,
        query,
        sqlite_table_options,
        like,
        clone,
        version,
        comment,
        on_commit,
        on_cluster,
        primary_key,
        order_by,
        partition_by,
        cluster_by,
        clustered_by,
        inherits,
        partition_of,
        for_values,
        copy_grants,
        enable_schema_evolution,
        change_tracking,
        data_retention_time_in_days,
        max_data_extension_time_in_days,
        default_ddl_collation,
        with_aggregation_policy,
        with_row_access_policy,
        with_storage_lifecycle_policy,
        with_tags,
        external_volume,
        with_connection,
        base_location,
        catalog,
        catalog_sync,
        storage_serialization_policy,
        target_lag,
        warehouse,
        refresh_mode,
        initialize,
        require_user,
        diststyle,
        distkey,
        sortkey,
        backup,
        multiset,
        fallback,
        with_data,
    } = table;
    table_columns(table)
        && !temporary
        && !unlogged
        && !external
        && !dynamic
        && global.is_none()
        && !transient
        && !volatile
        && !iceberg
        && !snapshot
        && matches!(hive_distribution, HiveDistributionStyle::NONE)
        && hive_formats.is_none()
        && file_format.is_none()
        && location.is_none()
        && query.is_none()
        && !sqlite_table_options.leading_comma
        && sqlite_table_options.options.is_empty()
        && like.is_none()
        && clone.is_none()
        && version.is_none()
        && comment.is_none()
        && on_commit.is_none()
        && on_cluster.is_none()
        && primary_key.is_none()
        && order_by.is_none()
        && partition_by.is_none()
        && cluster_by.is_none()
        && clustered_by.is_none()
        && inherits.is_none()
        && partition_of.is_none()
        && for_values.is_none()
        && !copy_grants
        && enable_schema_evolution.is_none()
        && change_tracking.is_none()
        && data_retention_time_in_days.is_none()
        && max_data_extension_time_in_days.is_none()
        && default_ddl_collation.is_none()
        && with_aggregation_policy.is_none()
        && with_row_access_policy.is_none()
        && with_storage_lifecycle_policy.is_none()
        && with_tags.is_none()
        && external_volume.is_none()
        && with_connection.is_none()
        && base_location.is_none()
        && catalog.is_none()
        && catalog_sync.is_none()
        && storage_serialization_policy.is_none()
        && target_lag.is_none()
        && warehouse.is_none()
        && refresh_mode.is_none()
        && initialize.is_none()
        && !require_user
        && diststyle.is_none()
        && distkey.is_none()
        && sortkey.is_none()
        && backup.is_none()
        && multiset.is_none()
        && fallback.is_none()
        && with_data.is_none()
}

pub(super) fn view(view: &CreateView) -> bool {
    let CreateView {
        local,
        or_alter,
        or_replace: _,
        materialized,
        secure,
        name: _,
        name_before_not_exists,
        columns,
        query: _,
        options,
        cluster_by,
        comment,
        with_no_schema_binding,
        if_not_exists,
        temporary,
        copy_grants,
        to,
        params,
    } = view;
    !or_alter
        && !(local.is_some() && *materialized)
        && !secure
        && !name_before_not_exists
        && !with_no_schema_binding
        && !if_not_exists
        && !temporary
        && !copy_grants
        && to.is_none()
        && params.is_none()
        && cluster_by.is_empty()
        && comment.is_none()
        && properties(options)
        && columns.iter().all(
            |ViewColumnDef {
                 name: _,
                 data_type,
                 options,
             }| { data_type.is_none() && options.is_none() },
        )
}

fn properties(options: &CreateTableOptions) -> bool {
    // Key/value syntax is checked by attributes::properties.
    matches!(
        options,
        CreateTableOptions::None | CreateTableOptions::With(_)
    )
}

fn table_columns(table: &CreateTable) -> bool {
    properties(&table.table_options)
        && !(table.columns.is_empty() && table.constraints.is_empty())
        && table.columns.iter().all(column)
        && table.constraints.iter().all(|constraint| match constraint {
            TableConstraint::PrimaryKey(key) => primary_key(key, false),
            TableConstraint::ForeignKey(key) => foreign_key(key),
            _ => false,
        })
}

pub(super) fn inline_foreign_key(
    parser: &mut sqlparser_064::parser::Parser,
    start: sqlparser_064::tokenizer::Span,
) -> Result<ColumnOption, sqlparser_064::parser::ParserError> {
    use sqlparser_064::tokenizer::Token;
    let table = parser.parse_identifier()?;
    parser.expect_token(&Token::LParen)?;
    let column = parser.parse_identifier()?;
    let span = start.union(&parser.peek_token().span);
    parser.expect_token(&Token::RParen)?;
    for name in [&table, &column] {
        if let std::ops::ControlFlow::Break(error) = super::reserved::identifier(name) {
            return Err(error);
        }
    }
    Ok(ColumnOption::InlineForeignKey {
        span,
        table,
        column,
    })
}

fn column(column: &ColumnDef) -> bool {
    let ColumnDef {
        name: _,
        data_type: _,
        options,
    } = column;
    options.iter().enumerate().all(|(index, option)| {
        if option.name.is_some() {
            return false;
        }
        match &option.option {
            // NullableOptDefaultTrue precedes all ColumnAttribute occurrences;
            // a second NULL marker, or a marker after an attribute, is invalid.
            ColumnOption::Null | ColumnOption::NotNull => index == 0,
            ColumnOption::Default(_) | ColumnOption::InlineForeignKey { .. } => true,
            ColumnOption::PrimaryKey(key) => primary_key(key, true),
            ColumnOption::DialectSpecific(tokens) => matches!(tokens.first(),
                Some(sqlparser_064::tokenizer::Token::Word(word))
                    if word.quote_style.is_none() && word.value.eq_ignore_ascii_case("LATENESS")),
            _ => false,
        }
    })
}

fn primary_key(key: &PrimaryKeyConstraint, inline: bool) -> bool {
    let PrimaryKeyConstraint {
        name,
        index_name,
        index_type,
        columns,
        include,
        index_options,
        characteristics,
    } = key;
    name.is_none()
        && index_name.is_none()
        && index_type.is_none()
        && include.is_empty()
        && index_options.is_empty()
        && characteristics.is_none()
        && (if inline {
            columns.is_empty()
        } else {
            !columns.is_empty()
        })
        && columns.iter().all(
            |IndexColumn {
                 column,
                 operator_class,
             }| {
                let OrderByExpr {
                    expr,
                    options,
                    with_fill,
                } = column;
                matches!(expr, Expr::Identifier(_))
                    && *options == OrderByOptions::default()
                    && with_fill.is_none()
                    && operator_class.is_none()
            },
        )
}

fn foreign_key(key: &ForeignKeyConstraint) -> bool {
    let ForeignKeyConstraint {
        name,
        index_name,
        columns,
        foreign_table,
        referred_columns,
        on_delete,
        on_update,
        match_kind,
        characteristics,
    } = key;
    name.is_none()
        && index_name.is_none()
        && !columns.is_empty()
        && matches!(foreign_table.0.as_slice(), [ObjectNamePart::Identifier(_)])
        && !referred_columns.is_empty()
        && on_delete.is_none()
        && on_update.is_none()
        && match_kind.is_none()
        && characteristics.is_none()
}

#[cfg(test)]
mod tests {
    use super::super::parse;

    #[test]
    fn table_attributes_and_constraints_follow_the_feldera_grammar() {
        for sql in [
            "CREATE TABLE t(x INT UNIQUE)",
            "CREATE TABLE t(x INT CHECK(x>0))",
            "CREATE TABLE t(x INT REFERENCES s(x))",
            "CREATE TABLE t(x INT CONSTRAINT c PRIMARY KEY)",
            "CREATE TABLE t(x INT NOT NULL NULL)",
            "CREATE TABLE t(x INT DEFAULT 1 NOT NULL)",
            "CREATE TABLE t(x INT, PRIMARY KEY(x DESC))",
            "CREATE TABLE t(x INT, FOREIGN KEY(x) REFERENCES s)",
            "CREATE TABLE t(x INT, FOREIGN KEY(x) REFERENCES s(x) ON DELETE CASCADE)",
            "CREATE TABLE t(x INT) INHERITS(s)",
            "CREATE TABLE t(x INT) STRICT",
            "CREATE VIEW v(x COMMENT 'a') AS SELECT 1",
            "CREATE VIEW v CLUSTER BY (x) AS SELECT 1",
        ] {
            assert!(!parse(sql).is_ok(), "{sql}");
        }
        for sql in [
            "CREATE TABLE t(x INT NOT NULL PRIMARY KEY, y INT DEFAULT 1)",
            "CREATE TABLE t(x INT, PRIMARY KEY(x))",
            "CREATE TABLE t(x INT, FOREIGN KEY(x) REFERENCES s(x))",
            "CREATE TABLE t(x INT LATENESS 1) WITH ('x'='y')",
            "CREATE VIEW v(x) AS SELECT 1",
            "CREATE MATERIALIZED VIEW v AS SELECT 1",
            "CREATE OR REPLACE TABLE t(x INT)",
        ] {
            assert!(parse(sql).is_ok(), "{sql}");
        }
    }
}
