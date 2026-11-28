//! Explicit field policies for upstream query, DML and index nodes.
//!
//! Destructuring without `..` makes an upstream field addition require review.
//! This covers these structs, not every AST family or every expression option.

use sqlparser_064::ast::{
    CreateIndex, Delete, Expr, Fetch, FromTable, Insert, Merge, MergeAction, MergeClauseKind,
    MergeInsertKind, MergeUpdateKind, OrderByOptions, Query, Select, SelectFlavor, SetExpr, Spanned,
    Statement, TableFactor, TableObject, TableWithJoins, Update,
};
use sqlparser_064::parser::ParserError;
use std::ops::ControlFlow;

fn require(allowed: bool, node: &impl Spanned) -> ControlFlow<ParserError> {
    if allowed {
        ControlFlow::Continue(())
    } else {
        ControlFlow::Break(super::diagnostic::at(
            "AST option is outside the Feldera query/DML grammar",
            node.span().start,
        ))
    }
}

pub(super) fn query(query: &Query) -> ControlFlow<ParserError> {
    // Upstream can wrap DML in a query, for example WITH ... DELETE.
    // Calcite accepts DML only through its statement production.
    if let SetExpr::Insert(stmt) | SetExpr::Delete(stmt) | SetExpr::Merge(stmt) | SetExpr::Update(stmt) =
        query.body.as_ref()
    {
        return require(false, stmt);
    }
    let Query {
        with: _,
        body: _,
        order_by: _,
        limit_clause: _,
        fetch,
        locks,
        for_clause,
        settings,
        format_clause,
        pipe_operators,
    } = query;
    let fetch_allowed = fetch.as_ref().is_none_or(
        |Fetch {
             with_ties,
             percent,
             quantity: _,
         }| { !with_ties && !percent },
    );
    require(
        locks.is_empty()
            && for_clause.is_none()
            && settings.is_none()
            && format_clause.is_none()
            && pipe_operators.is_empty()
            && fetch_allowed,
        query,
    )
}

pub(super) fn select(select: &Select) -> ControlFlow<ParserError> {
    let Select {
        select_token: _,
        optimizer_hints: _,
        distinct: _,
        select_modifiers,
        top,
        top_before_distinct,
        projection: _,
        exclude,
        into,
        from: _,
        lateral_views,
        prewhere,
        selection: _,
        connect_by,
        group_by: _,
        cluster_by,
        distribute_by,
        sort_by,
        having: _,
        named_window,
        qualify,
        window_before_qualify,
        value_table_mode,
        flavor,
    } = select;
    require(
        select_modifiers.is_none()
            && top.is_none()
            && !top_before_distinct
            && exclude.is_none()
            && into.is_none()
            && lateral_views.is_empty()
            && prewhere.is_none()
            && connect_by.is_empty()
            && cluster_by.is_empty()
            && distribute_by.is_empty()
            && sort_by.is_empty()
            && value_table_mode.is_none()
            && *flavor == SelectFlavor::Standard
            && (named_window.is_empty() || qualify.is_none() || *window_before_qualify),
        select,
    )
}

fn named_table(table: &TableWithJoins) -> bool {
    table.joins.is_empty() && matches!(&table.relation, TableFactor::Table { args: None, .. })
}

fn insert(insert: &Insert) -> bool {
    let Insert {
        insert_token: _,
        optimizer_hints,
        or,
        ignore,
        into,
        table,
        table_alias,
        columns: _,
        by_name,
        overwrite,
        source,
        assignments,
        partitioned,
        after_columns,
        overriding,
        has_table_keyword,
        on,
        returning,
        output,
        replace_into,
        priority,
        insert_alias,
        settings,
        format_clause,
        multi_table_insert_type,
        multi_table_into_clauses,
        multi_table_when_clauses,
        multi_table_else_clause,
    } = insert;
    optimizer_hints.is_empty()
        && or.is_none()
        && !ignore
        && *into
        && matches!(
            table,
            TableObject::TableName(_) | TableObject::HintedTableName { .. }
        )
        && table_alias.is_none()
        && !by_name
        && !overwrite
        && source.is_some()
        && assignments.is_empty()
        && partitioned.is_none()
        && after_columns.is_empty()
        && overriding.is_none()
        && !has_table_keyword
        && on.is_none()
        && returning.is_none()
        && output.is_none()
        && !replace_into
        && priority.is_none()
        && insert_alias.is_none()
        && settings.is_none()
        && format_clause.is_none()
        && multi_table_insert_type.is_none()
        && multi_table_into_clauses.is_empty()
        && multi_table_when_clauses.is_empty()
        && multi_table_else_clause.is_none()
}

fn update(update: &Update) -> bool {
    let Update {
        update_token: _,
        optimizer_hints,
        table,
        on_cluster,
        assignments: _,
        from,
        selection: _,
        returning,
        output,
        or,
        order_by,
        limit,
        settings,
    } = update;
    optimizer_hints.is_empty()
        && named_table(table)
        && on_cluster.is_none()
        && from.is_none()
        && returning.is_none()
        && output.is_none()
        && or.is_none()
        && order_by.is_empty()
        && limit.is_none()
        && settings.is_none()
}

fn delete(delete: &Delete) -> bool {
    let Delete {
        delete_token: _,
        optimizer_hints,
        tables,
        from,
        on_cluster,
        using,
        selection: _,
        returning,
        output,
        order_by,
        limit,
        settings,
    } = delete;
    optimizer_hints.is_empty()
        && tables.is_empty()
        && matches!(from, FromTable::WithFromKeyword(tables)
            if matches!(tables.as_slice(), [table] if named_table(table)))
        && on_cluster.is_none()
        && using.is_none()
        && returning.is_none()
        && output.is_none()
        && order_by.is_empty()
        && limit.is_none()
        && settings.is_none()
}

pub(super) fn statement(statement: &Statement) -> ControlFlow<ParserError> {
    let allowed = match statement {
        Statement::Insert(node) => insert(node),
        Statement::Update(node) => update(node),
        Statement::Delete(node) => delete(node),
        Statement::CreateIndex(node) => create_index(node),
        Statement::Merge(node) => merge(node),
        Statement::CreateTable(node) => super::ddl::table(node),
        Statement::CreateView(node) => super::ddl::view(node),
        _ => true,
    };
    require(allowed, statement)
}

fn create_index(index: &CreateIndex) -> bool {
    // Feldera's SqlCreateIndex accepts only a name, relation and simple column
    // identifiers. Every upstream option is explicit so additions need review.
    let CreateIndex {
        or_replace: _,
        name,
        table_name: _,
        using,
        columns,
        unique,
        concurrently,
        r#async,
        if_not_exists,
        include,
        nulls_distinct,
        with,
        predicate,
        index_options,
        alter_options,
    } = index;
    name.is_some()
        && using.is_none()
        && !unique
        && !concurrently
        && !r#async
        && !if_not_exists
        && include.is_empty()
        && nulls_distinct.is_none()
        && with.is_empty()
        && predicate.is_none()
        && index_options.is_empty()
        && alter_options.is_empty()
        && !columns.is_empty()
        && columns.iter().all(|column| {
            matches!(column.column.expr, Expr::Identifier(_))
                && column.column.options == OrderByOptions::default()
                && column.column.with_fill.is_none()
                && column.operator_class.is_none()
        })
}

fn merge(merge: &Merge) -> bool {
    let Merge {
        merge_token: _,
        optimizer_hints,
        into,
        table: _,
        source: _,
        on: _,
        clauses,
        output,
    } = merge;
    if !optimizer_hints.is_empty() || !into || output.is_some() || clauses.is_empty() {
        return false;
    }
    let mut matched = false;
    let mut not_matched = false;
    clauses.iter().all(|clause| {
        if clause.predicate.is_some() {
            return false;
        }
        match (&clause.clause_kind, &clause.action) {
            (MergeClauseKind::Matched, MergeAction::Update(update)) => {
                !std::mem::replace(&mut matched, true)
                    && !not_matched
                    && matches!(update.kind, MergeUpdateKind::Set(_))
                    && update.update_predicate.is_none()
                    && update.delete_predicate.is_none()
            }
            (MergeClauseKind::NotMatched, MergeAction::Insert(insert)) => {
                !std::mem::replace(&mut not_matched, true)
                    && matches!(insert.kind, MergeInsertKind::Values(_))
                    && insert.insert_predicate.is_none()
            }
            _ => false,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::super::parse;

    #[test]
    fn index_columns_are_identifiers_without_sort_or_index_options() {
        for sql in [
            "CREATE INDEX i ON t(x DESC)",
            "CREATE INDEX i ON t(x NULLS FIRST)",
            "CREATE INDEX i ON t((x+1))",
            "CREATE INDEX i ON t(t.x)",
            "CREATE UNIQUE INDEX i ON t(x)",
            "CREATE INDEX ON t(x)",
            "CREATE INDEX i ON t(x) INCLUDE(y)",
        ] {
            assert!(!parse(sql).is_ok(), "{sql}");
        }
        assert!(parse("CREATE INDEX i ON t(x, \"y\")").is_ok());
    }

    #[test]
    fn merge_has_at_most_one_update_followed_by_one_insert() {
        let base = "MERGE INTO t USING s ON t.x=s.x";
        for suffix in [
            "WHEN MATCHED THEN DELETE",
            "WHEN MATCHED AND TRUE THEN UPDATE SET x=1",
            "WHEN NOT MATCHED BY TARGET THEN INSERT(x) VALUES(1)",
            "WHEN MATCHED THEN UPDATE SET x=1 WHEN MATCHED THEN UPDATE SET x=2",
            "WHEN NOT MATCHED THEN INSERT(x) VALUES(1) WHEN MATCHED THEN UPDATE SET x=2",
        ] {
            let sql = format!("{base} {suffix}");
            assert!(!parse(&sql).is_ok(), "{sql}");
        }
        for suffix in [
            "WHEN MATCHED THEN UPDATE SET x=1",
            "WHEN NOT MATCHED THEN INSERT(x) VALUES(1)",
            "WHEN MATCHED THEN UPDATE SET x=1 WHEN NOT MATCHED THEN INSERT(x) VALUES(1)",
        ] {
            let sql = format!("{base} {suffix}");
            assert!(parse(&sql).is_ok(), "{sql}");
        }
    }

    #[test]
    fn foreign_query_options_cannot_hide_inside_valid_subqueries() {
        for sql in [
            "SELECT * FROM t FOR UPDATE",
            "SELECT * FROM t FOR SHARE",
            "SELECT * FROM t FOR XML AUTO",
            "SELECT TOP 1 * FROM t",
            "SELECT x INTO q FROM t",
            "SELECT * FROM t CLUSTER BY x",
            "SELECT * FROM t DISTRIBUTE BY x",
            "SELECT * FROM t SORT BY x",
            "SELECT * FROM t CONNECT BY PRIOR x=y",
            "SELECT * FROM t LATERAL VIEW explode(x) q AS a",
            "SELECT * FROM t QUALIFY TRUE WINDOW w AS ()",
            "SELECT * FROM t FETCH FIRST 1 ROWS WITH TIES",
            "SELECT * FROM t FETCH FIRST 10 PERCENT ROWS ONLY",
        ] {
            for sql in [sql.to_owned(), format!("SELECT * FROM ({sql}) q")] {
                assert!(!parse(&sql).is_ok(), "{sql}");
            }
        }
    }

    #[test]
    fn foreign_dml_options_are_rejected_while_regular_forms_remain_valid() {
        for sql in [
            "INSERT OR REPLACE INTO t VALUES (1)",
            "INSERT INTO TABLE t VALUES (1)",
            "INSERT INTO t BY NAME SELECT 1",
            "INSERT INTO t VALUES (1) RETURNING x",
            "INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING",
            "INSERT INTO t PARTITION (x=1) VALUES (1)",
            "INSERT INTO t OVERRIDING SYSTEM VALUE VALUES (1)",
            "UPDATE t SET x=1 FROM s WHERE t.x=s.x",
            "UPDATE t SET x=1 RETURNING x",
            "UPDATE t JOIN s ON t.x=s.x SET t.x=1",
            "DELETE FROM t RETURNING x",
            "DELETE FROM t ORDER BY x LIMIT 1",
            "DELETE t FROM t JOIN s ON t.x=s.x",
            "DELETE FROM t USING s WHERE t.x=s.x",
        ] {
            assert!(!parse(sql).is_ok(), "{sql}");
        }
        for sql in [
            "SELECT DISTINCT ON (x) x FROM t",
            "SELECT SQL_NO_CACHE x FROM t",
            "SELECT * FROM t WINDOW w AS () QUALIFY TRUE",
            "SELECT x FROM t GROUP BY ALL",
            "SELECT * FROM t LIMIT 2 OFFSET 1",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET x=1 WHERE x=0",
            "DELETE FROM t WHERE x=0",
            "INSERT INTO t /*+ h */ VALUES (1)",
            "UPDATE t /*+ h */ SET x=1",
        ] {
            assert!(parse(sql).is_ok(), "{sql}");
        }
    }
}
