package org.dbsp.sqlCompiler.compiler.sql.recursive;

import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.junit.Assert;
import org.junit.Test;

/** Tests with recursive queries and incremental compilation */
public class IncrementalRecursiveTests extends BaseSQLTests {
    @Override
    public DBSPCompiler testCompiler() {
        CompilerOptions options = this.testOptions(true, true);
        return new DBSPCompiler(options);
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
    public void issue3317() {
        this.getCC("""
                -- Given a cell value as a formula (e.g., =A0+B0), and a context with cell values
                -- referenced in the formula, returns the computed value of the cell
                create function cell_value(cell varchar(64), mentions_ids bigint array, mentions_values varchar(64) array) returns varchar(64);
                
                -- Given a cell value e.g., =A0+B0, returns an array of cell ids that were mentioned in the formula
                create function mentions(cell varchar(64)) returns bigint array;
                
                -- Forward declaration of spreadsheet view
                declare recursive view spreadsheet_view (
                                                        id bigint not null,
                                                        background integer not null,
                                                        raw_value varchar(64) not null,
                                                        computed_value varchar(64),
                                                        cnt integer not null
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
                    sv.computed_value as mentioned_value,
                    coalesce(sv.cnt, 0) + 1 as cnt
                from
                    latest_cells_with_mentions m
                        left join
                    spreadsheet_view sv on m.mentioned_id = sv.id;
                
                -- We aggregate mentioned values and ids back into arrays
                create local view mentions_aggregated as
                select
                    id,
                    raw_value,
                    background,
                    ARRAY_AGG(mentioned_id) as mentions_ids,
                    ARRAY_AGG(mentioned_value) as mentions_values,
                    max(cnt) as max_cnt
                from
                    mentions_with_values
                group by
                    id,
                    raw_value,
                    background,
                    cnt;
                
                -- Calculate the final spreadsheet by executing the UDF for the formula, apply recursively up to a depth of 10
                create materialized view spreadsheet_compute_values as
                select
                    id,
                    background,
                    raw_value,
                    cell_value(raw_value, mentions_ids, mentions_values) AS computed_value,
                    max_cnt
                from
                    mentions_aggregated
                where max_cnt < 10;
                
                -- Take the final value of the recursive iteration
                create materialized view spreadsheet_view as
                select
                    id,
                    background,
                    raw_value,
                    cell_value(raw_value, mentions_ids, mentions_values) AS computed_value,
                    coalesce(max_cnt, 0) as cnt
                from
                    mentions_aggregated
                where max_cnt < 10;
                
                -- Figure out which IPs currently reached their API limit
                create materialized view api_limit_reached as
                select
                    ip
                from
                    spreadsheet_data
                where
                    ts >= NOW() - INTERVAL 60 MINUTES
                group by
                    ip
                having
                    count(*) > 10000;
                
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
                    (select currently_active_users from currently_active_users) as currently_active_users;""");
    }

    @Test
    public void transitiveClosure() {
        String sql = """
                CREATE TABLE EDGES(x int, y int);
                DECLARE RECURSIVE VIEW CLOSURE(x int, y int);
                CREATE LOCAL VIEW STEP AS
                SELECT EDGES.x, CLOSURE.y FROM
                EDGES JOIN CLOSURE ON EDGES.y = CLOSURE.x;
                CREATE MATERIALIZED VIEW CLOSURE AS (SELECT * FROM EDGES) UNION (SELECT * FROM STEP);
                """;
        var ccs = this.getCCS(sql);
        ccs.step("", """
                 x | y | weight
                ----------------""");
        ccs.step("INSERT INTO EDGES VALUES(0, 1);", """
                 x | y | weight
                ----------------
                 0 | 1 | 1""");
        ccs.step("INSERT INTO EDGES VALUES(1, 2);", """
                x | y | weight
                ---------------
                1 | 2 | 1
                0 | 2 | 1""");
        ccs.step("REMOVE FROM EDGES VALUES(0, 1);", """
                x | y | weight
                ---------------
                0 | 1 | -1
                0 | 2 | -1""");
    }

    @Test
    public void factorial() {
        // Three tests adapted from https://www.geeksforgeeks.org/postgresql-create-recursive-views/
        String sql = """
                DECLARE RECURSIVE VIEW fact(n int, factorial int);
                CREATE VIEW fact AS
                   SELECT 1 as n, 5 as factorial
                   UNION ALL SELECT n+1, factorial*n FROM fact WHERE n < 5;""";
        var ccs = this.getCCS(sql);
        ccs.step("", """
                 n | factorial | weight
                ---+--------------------
                 1 |         5 | 1
                 2 |         5 | 1
                 3 |        10 | 1
                 4 |        30 | 1
                 5 |       120 | 1""");
    }

    @Test
    public void testSequence() {
        String sql = """
                DECLARE RECURSIVE VIEW tens(n int);
                CREATE VIEW tens AS
                    SELECT 1 as n
                 UNION ALL
                   SELECT n+1 FROM tens WHERE n < 10;""";
        var ccs = this.getCCS(sql);
        ccs.step("", """
                 n  | weight
                -------------
                  1 | 1
                  2 | 1
                  3 | 1
                  4 | 1
                  5 | 1
                  6 | 1
                  7 | 1
                  8 | 1
                  9 | 1
                 10 | 1""");
    }

    @Test
    public void testHierarchy() {
        String sql = """
                CREATE TABLE emp (
                  emp_id INT NOT NULL PRIMARY KEY,
                  emp_name VARCHAR,
                  manager_id INT
                );
                
                DECLARE RECURSIVE VIEW subordinates(emp_id int NOT NULL, manager_id int, emp_name varchar, level int);
                CREATE VIEW subordinates AS
                  SELECT emp_id, manager_id, emp_name, 0 AS level
                  FROM emp
                  WHERE manager_id IS NULL
                  UNION ALL
                  SELECT e.emp_id, e.manager_id, e.emp_name, s.level + 1
                  FROM emp e
                  INNER JOIN subordinates s ON s.emp_id = e.manager_id;""";
        var ccs = this.getCCS(sql);
        ccs.step("""
                INSERT INTO emp
                VALUES
                (1, 'Onkar', NULL),
                (2, 'Isaac', 1),
                (3, 'Jack', 1),
                (4, 'Aditya', 1),
                (5, 'Albert', 1),
                (6, 'Alex', 2),
                (7, 'Brian', 2),
                (8, 'Harry', 3),
                (9, 'Paul', 3),
                (10, 'Kunal', 4),
                (11, 'Pranav', 5)""", """
                 emp_id | manager_id | emp_name | level | weight
                -------------------------------------------------
                      1 |            | Onkar|         0 | 1
                      2 |          1 | Isaac|         1 | 1
                      3 |          1 | Jack|          1 | 1
                      4 |          1 | Aditya|        1 | 1
                      5 |          1 | Albert|        1 | 1
                      6 |          2 | Alex|          2 | 1
                      7 |          2 | Brian|         2 | 1
                      8 |          3 | Harry|         2 | 1
                      9 |          3 | Paul|          2 | 1
                     10 |          4 | Kunal|         2 | 1
                     11 |          5 | Pranav|        2 | 1""");
    }
}
