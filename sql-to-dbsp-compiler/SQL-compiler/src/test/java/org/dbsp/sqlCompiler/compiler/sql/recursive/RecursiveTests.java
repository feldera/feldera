package org.dbsp.sqlCompiler.compiler.sql.recursive;

import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.compiler.backend.MerkleOuter;
import org.dbsp.sqlCompiler.compiler.frontend.statements.DeclareViewStatement;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.util.Logger;
import org.junit.Assert;
import org.junit.Test;

/** Tests with recursive queries */
public class RecursiveTests extends BaseSQLTests {
    @Test
    public void issue2977() {
        this.statementsFailingInCompilation("""
            DECLARE RECURSIVE VIEW V(x integer, y VARCHAR);
            CREATE VIEW V AS SELECT y, x FROM V;""", """
                does not match the declared type v(x INTEGER, y VARCHAR):
                    1|DECLARE RECURSIVE VIEW V(x integer, y VARCHAR);
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                    2|CREATE VIEW V AS SELECT y, x FROM V;""");
    }

    @Test
    public void issue2979() {
        String sql = """
                -- Given a cell value as a formula (e.g., =A0+B0), and a context with cell values
                -- referenced in the formula, returns the computed value of the cell
                create function cell_value(cell varchar(64), mentions_ids bigint array, mentions_values varchar(64) array) returns varchar(64);
                
                -- Given a cell value e.g., =A0+B0, returns an array of cell ids that were mentioned in the formula
                create function mentions(cell varchar(64)) returns bigint array;
                
                -- Forward declaration of spreadsheet view
                declare recursive view spreadsheet_view (
                    id bigint not null,
                    raw_value varchar(64) not null,
                    background integer not null,
                    mentions_ids bigint array,
                    mentions_values varchar(64) array,
                    computed_value varchar(64)
                );
                
                -- Raw spreadsheet cell data coming from backend/user, updates
                -- are inserted as new entries with newer timestamps
                create table spreadsheet_data (
                    id bigint not null,
                    ip varchar(45) not null,
                    ts timestamp not null,
                    raw_value varchar(64) not null,
                    background integer not null
                ) with (
                      'materialized' = 'true',
                      'connectors' = '[{
                        "transport": {
                            "name": "datagen",
                            "config": {
                                "workers": 1,
                                "plan": [{
                                    "rate": 1,
                                    "limit": 2,
                                    "fields": {
                                        "id": { "range": [0, 1000] },
                                        "ip": { "strategy": "uniform", "range": [0, 5] },
                                        "raw_value": { "values": ["=B0", "0"] },
                                        "background": { "strategy": "uniform", "range": [0, 1000] }
                                    }
                                }]
                            }
                        }
                    }]'
                      );
                
                -- Get the latest cell value for the spreadsheet.
                -- (By finding the one with the highest `ts` for a given `id`)
                create view latest_cells as with
                    max_ts_per_cell as (
                        select
                            id,
                            max(ts) as max_ts
                        from
                            spreadsheet_data
                        group by
                            id
                    )
                select
                    s.id,
                    s.raw_value,
                    s.background,
                    -- The append with null is silly but crucial to ensure that the
                    -- cross join in `latest_cells_with_mention` returns all cells
                    -- not just those that reference another cell
                    ARRAY_APPEND(mentions(s.raw_value), null) as mentioned_cell_ids
                from
                    spreadsheet_data s
                        join max_ts_per_cell mt on s.id = mt.id and s.ts = mt.max_ts;
                
                -- List all mentioned ids per latest cell
                create view latest_cells_with_mentions as
                select
                    s.id,
                    s.raw_value,
                    s.background,
                    m.mentioned_id
                from
                    latest_cells s, unnest(s.mentioned_cell_ids) as m(mentioned_id);
                
                -- Like latest_cells_with_mentions, but enrich it with values of mentioned cells
                create local view mentions_with_values as
                select
                    m.id,
                    m.raw_value,
                    m.background,
                    m.mentioned_id,
                    sc.computed_value as mentioned_value
                from
                    latest_cells_with_mentions m
                left join
                    spreadsheet_view sc on m.mentioned_id = sc.id;
                
                -- We aggregate mentioned values and ids back into arrays
                create local view mentions_aggregated as
                select
                    id,
                    raw_value,
                    background,
                    ARRAY_AGG(mentioned_id) as mentions_ids,
                    ARRAY_AGG(mentioned_value) as mentions_values
                from
                    mentions_with_values
                group by
                    id,
                    raw_value,
                    background;
                
                -- Calculate the final spreadsheet by executing the UDF for the formula
                create materialized view spreadsheet_view as
                select
                    id,
                    raw_value,
                    background,
                    mentions_ids,
                    mentions_values,
                    cell_value(raw_value, mentions_ids, mentions_values) AS computed_value
                from
                    mentions_aggregated;
                
                -- Figure out which IPs currently reached their API limit
                create materialized view api_limit_reached as
                select
                    ip,
                    count(*) as request_count
                from
                    spreadsheet_data
                where
                    ts >= NOW() - INTERVAL 60 MINUTES
                group by
                    ip
                having
                    count(*) > 200;
                
                -- Compute statistics
                create materialized view spreadsheet_statistics as
                with filled_total as (
                    select
                        count(distinct id) as filled_total
                    from
                        spreadsheet_data
                ),
                filled_this_hour as (
                    select
                        count(*) as filled_this_hour
                    from
                        spreadsheet_data
                    where
                        ts >= NOW() - INTERVAL 1 HOUR
                ),
                filled_today as (
                    select
                        count(*) as filled_today
                    from
                        spreadsheet_data
                    where
                        ts >= NOW() - INTERVAL 1 DAY
                ),
                filled_this_week as (
                    select
                        count(*) as filled_this_week
                    from
                        spreadsheet_data
                    where
                        ts >= NOW() - INTERVAL 1 WEEK
                ),
                currently_active_users as (
                    select
                        count(distinct ip) as currently_active_users
                    from
                        spreadsheet_data
                    where
                        ts >= NOW() - INTERVAL 5 MINUTE
                )
                select
                    (select filled_total from filled_total) as filled_total,
                    (select filled_this_hour from filled_this_hour) as filled_this_hour,
                    (select filled_today from filled_today) as filled_today,
                    (select filled_this_week from filled_this_week) as filled_this_week,
                    (select currently_active_users from currently_active_users) as currently_active_users;""";
        var cc = this.getCC(sql);
        CircuitVisitor visitor = new CircuitVisitor(cc.compiler) {
            @Override
            public void postorder(DBSPSinkOperator operator) {
                assert !operator.viewName.name().endsWith(DeclareViewStatement.declSuffix);
            }
        };
        cc.visit(visitor);
    }

    @Test
    public void issue2978() {
        String sql = """
                -- Forward declaration of spreadsheet view
                DECLARE RECURSIVE view spreadsheet_view (
                    id bigint not null,
                    raw_value varchar(64) not null,
                    background integer not null,
                    computed_value varchar(64) not null
                );
                
                -- Forward declaration of mentions_with_values view
                DECLARE RECURSIVE view mentions_with_values (
                    id BIGINT NOT NULL,
                    raw_value VARCHAR(64) NOT NULL,
                    background INTEGER NOT NULL
                );
                
                -- Raw spreadsheet cell data coming from backend/user, updates
                -- are inserted as new entries with newer timestamps
                create table spreadsheet_data (
                    id bigint not null,
                    ip varchar(45) not null,
                    ts timestamp not null,
                    raw_value varchar(64) not null,
                    background integer not null
                );
                
                -- Get the latest cell value for the spreadsheet.
                -- (By finding the one with the highest `ts` for a given `id`)
                create view latest_cells as with
                    max_ts_per_cell as (
                        select
                            id,
                            max(ts) as max_ts
                        from
                            spreadsheet_data
                        group by
                            id
                    )
                select
                    s.id,
                    s.raw_value,
                    s.background
                from
                    spreadsheet_data s
                        join max_ts_per_cell mt on s.id = mt.id and s.ts = mt.max_ts;
                
                -- List all mentioned ids per latest cell
                create view latest_cells_with_mentions as
                select
                    s.id,
                    s.raw_value,
                    s.background
                from
                    latest_cells s;
                
                -- Like latest_cells_with_mentions, but enrich it with values of mentioned cells
                create view mentions_with_values as
                select
                    m.id,
                    m.raw_value,
                    m.background
                from
                    latest_cells_with_mentions m;
                
                -- We aggregate mentioned values and ids back into arrays
                create view mentions_aggregated as
                select
                    id,
                    raw_value,
                    background
                from
                    mentions_with_values
                group by
                    id,
                    raw_value,
                    background;
                
                -- Calculate the final spreadsheet by executing the UDF for the formula
                create materialized view spreadsheet_view as
                select
                    id,
                    raw_value,
                    background,
                    raw_value AS computed_value
                from
                    mentions_aggregated;""";
        this.getCCS(sql);
    }

    @Test
    public void issue2980() {
        String sql = """
                -- Given a cell value as a formula (e.g., =A0+B0), and a context with cell values
                -- referenced in the formula, returns the computed value of the cell
                create function cell_value(cell varchar(64), mentions_ids bigint array, mentions_values varchar(64) array)
                returns varchar(64) not null as CAST('' AS VARCHAR(64));
                
                -- Given a cell value e.g., =A0+B0, returns an array of cell ids that were mentioned in the formula
                create function mentions(cell varchar(64)) returns bigint array not null as CAST(array(null) AS BIGINT ARRAY);
                
                -- Forward declaration of spreadsheet view
                DECLARE RECURSIVE view spreadsheet_view (
                    id bigint not null,
                    raw_value varchar(64) not null,
                    background integer not null,
                    mentions_ids bigint array,
                    mentions_values varchar(64) array,
                    computed_value varchar(64) not null
                );
                
                -- Forward declaration of mentions_with_values view
                DECLARE RECURSIVE view mentions_with_values (
                    id BIGINT NOT NULL,
                    raw_value VARCHAR(64) NOT NULL,
                    background INTEGER NOT NULL,
                    mentioned_id BIGINT,
                    mentioned_value VARCHAR(64)
                );
                
                -- Raw spreadsheet cell data coming from backend/user, updates
                -- are inserted as new entries with newer timestamps
                create table spreadsheet_data (
                    id bigint not null,
                    ip varchar(45) not null,
                    ts timestamp not null,
                    raw_value varchar(64) not null,
                    background integer not null
                ) with ( 'materialized' = 'true' );
                
                -- Get the latest cell value for the spreadsheet.
                -- (By finding the one with the highest `ts` for a given `id`)
                create view latest_cells as with
                    max_ts_per_cell as (
                        select
                            id,
                            max(ts) as max_ts
                        from
                            spreadsheet_data
                        group by
                            id
                    )
                select
                    s.id,
                    s.raw_value,
                    s.background,
                    -- The append with null is silly but crucial to ensure that the
                    -- cross join in `latest_cells_with_mention` returns all cells
                    -- not just those that reference another cell
                    ARRAY_APPEND(mentions(s.raw_value), null) as mentioned_cell_ids
                from
                    spreadsheet_data s
                        join max_ts_per_cell mt on s.id = mt.id and s.ts = mt.max_ts;
                
                -- List all mentioned ids per latest cell
                create view latest_cells_with_mentions as
                select
                    s.id,
                    s.raw_value,
                    s.background,
                    m.mentioned_id
                from
                    latest_cells s, unnest(s.mentioned_cell_ids) as m(mentioned_id);
                
                -- Like latest_cells_with_mentions, but enrich it with values of mentioned cells
                create view mentions_with_values as
                select
                    m.id,
                    m.raw_value,
                    m.background,
                    m.mentioned_id,
                    sc.computed_value as mentioned_value
                from
                    latest_cells_with_mentions m
                left join
                    spreadsheet_view sc on m.mentioned_id = sc.id;
                
                -- We aggregate mentioned values and ids back into arrays
                create local view mentions_aggregated as
                select
                    id,
                    raw_value,
                    background,
                    ARRAY_AGG(mentioned_id) as mentions_ids,
                    ARRAY_AGG(mentioned_value) as mentions_values
                from
                    mentions_with_values
                group by
                    id,
                    raw_value,
                    background;
                
                -- Calculate the final spreadsheet by executing the UDF for the formula
                create materialized view spreadsheet_view as
                select
                    id,
                    raw_value,
                    background,
                    mentions_ids,
                    mentions_values,
                    cell_value(raw_value, mentions_ids, mentions_values) AS computed_value
                from
                    mentions_aggregated;""";
        this.getCCS(sql);
    }

    @Test
    public void testRecursive() {
        String sql = """
                DECLARE RECURSIVE VIEW V(v INT);
                CREATE VIEW V AS SELECT v FROM V UNION SELECT 1;""";
        var ccs = this.getCCS(sql);
        ccs.step("", """
                 v | weight
                ------------
                 1 | 1""");
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int recursive = 0;
            @Override
            public void postorder(DBSPNestedOperator operator) {
                this.recursive++;
            }
            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.recursive);
            }
        };
        ccs.visit(visitor);
    }

    @Test
    public void testRecursive2() {
        // Non-incremental test
        String sql = """
                DECLARE RECURSIVE VIEW V(v INT);
                CREATE TABLE T(v INT);
                CREATE VIEW V AS SELECT v FROM V UNION SELECT * FROM T;""";
        var ccs = this.getCCS(sql);
        ccs.step("", """
                 v | weight
                ------------""");
        ccs.step("INSERT INTO T VALUES(1)", """
                 v | weight
                ------------
                 1 | 1""");
        ccs.step("INSERT INTO T VALUES(2)", """
                 v | weight
                ------------
                 2 | 1""");
    }

    @Test
    public void testRecursiveInMiddle() {
        // Circuit with a recursive component sandwiched between two
        // non-recursive components.
        String sql = """
                CREATE TABLE T(v INT);
                CREATE LOCAL VIEW X AS SELECT v/2 FROM T;
                DECLARE RECURSIVE VIEW V(v INT);
                CREATE LOCAL VIEW V AS SELECT v FROM V UNION SELECT * FROM X;
                CREATE VIEW O AS SELECT v+1 FROM V;""";
        var ccs = this.getCCS(sql);
        ccs.step("", """
                 v | weight
                ------------""");
        ccs.step("INSERT INTO T VALUES(1)", """
                 v | weight
                ------------
                 1 | 1""");
        ccs.step("INSERT INTO T VALUES(2)", """
                 v | weight
                ------------
                 2 | 1""");
    }

    @Test
    public void illegalRecursiveTests() {
        String sql = """
                CREATE TABLE E(x int, y int);
                DECLARE RECURSIVE VIEW CLOSURE(x int, y int);
                CREATE VIEW STEP AS
                SELECT E.x, CLOSURE.y FROM
                E JOIN CLOSURE ON e.y = CLOSURE.x;
                CREATE MATERIALIZED VIEW CLOSURE AS (SELECT * FROM E) UNION (SELECT * FROM STEP);
                """;
        this.statementsFailingInCompilation(sql,
                "View 'step' must be declared either as LOCAL or as RECURSIVE");

        sql = """
                CREATE TABLE E(x int, y int);
                DECLARE RECURSIVE VIEW CLOSURE(x int, y int);
                CREATE MATERIALIZED VIEW CLOSURE AS (SELECT * FROM E) UNION
                    (SELECT lag(x) OVER (PARTITION BY x ORDER BY y), x FROM CLOSURE);
                """;
        this.statementsFailingInCompilation(sql,
                "Unsupported operation 'LAG' in recursive code");
    }

    @Test
    public void errorTests() {
        // Declared type does not match
        String sql = """
                DECLARE RECURSIVE VIEW V(v INT);
                CREATE VIEW V AS SELECT CAST(V.v AS VARCHAR) FROM V UNION SELECT '1';""";
        this.statementsFailingInCompilation(sql, "does not match the declared type");

        // Declared column name does not match
        sql = """
                DECLARE RECURSIVE VIEW V(v INT);
                CREATE VIEW V (v0) AS SELECT DISTINCT v FROM V UNION SELECT 1;""";
        this.statementsFailingInCompilation(sql, "does not match the declared type");

        // Declared recursive view not used anywhere
        sql = "DECLARE RECURSIVE VIEW V(v INT);";
        this.shouldWarn(sql, "Unused view declaration");

        // Recursive view is not recursive
        sql = """
                DECLARE RECURSIVE VIEW V(v INT NOT NULL);
                CREATE VIEW V AS SELECT 1 AS v;""";
        this.shouldWarn(sql, "is declared recursive, but is not used in any recursive computation");
    }

    @Test
    public void unsupportedRecursive() {
        String sql = """
                DECLARE RECURSIVE VIEW V(v BIGINT NOT NULL);
                CREATE TABLE T(v BIGINT);
                CREATE LOCAL VIEW W AS SELECT * FROM V UNION SELECT * FROM T;
                CREATE VIEW V AS SELECT COUNT(*) OVER (ORDER BY v) v FROM W;""";
        this.statementsFailingInCompilation(sql, "Unsupported operation 'OVER' in recursive code");
    }
}
