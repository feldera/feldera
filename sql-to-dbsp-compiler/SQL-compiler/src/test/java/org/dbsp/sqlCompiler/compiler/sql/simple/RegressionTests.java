package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.IInputOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustVisitor;
import org.dbsp.sqlCompiler.compiler.backend.rust.multi.ProjectDeclarations;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.util.IndentStreamBuilder;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class RegressionTests extends SqlIoTest {
    @Test
    public void issue3913() {
        this.getCC("CREATE VIEW V AS SELECT SUBSTR(TIMESTAMP '2024-01-01 10:00:00', 10)");
    }

    @Test
    public void issue3653() {
        this.statementsFailingInCompilation("CREATE TYPE t AS (related t ARRAY);",
                "error: Compilation error: At line 1, column 27: Unknown identifier 't'");
    }

    @Test
    public void issue3418() {
        this.compileRustTestCase("""
                create table t (r ROW(j VARCHAR));
                create materialized view v as select parse_json(t.r.j) from t;""");
    }

    @Test
    public void testInternal179() {
        this.getCC("""
                CREATE TABLE h(p REAL NOT NULL);
                CREATE TABLE q(r REAL NOT NULL);
                CREATE VIEW V AS SELECT * FROM h LEFT JOIN q ON ROUND(r) = ROUND(p);
                """);
    }

    @Test
    public void issue3517() {
        this.getCCS("""
                create table t_problematic
                    ( r0 ROW
                        ( j00 VARCHAR
                        )
                    , r1 ROW
                        ( r10 ROW
                            ( j100 VARCHAR
                            )
                        , j11 VARCHAR
                        )
                    );
                create view v0 as select parse_json(t_problematic.r1.r10.j100) from t_problematic;
                create view v1 as select parse_json(t_problematic.r1.j11) from t_problematic;
                """);
    }

    @Test
    public void internalIssue174() {
        this.compileRustTestCase("""
                CREATE FUNCTION a2m(input VARIANT ARRAY) RETURNS VARIANT NOT NULL AS VariantNull();
                
                CREATE TYPE iff AS (x BIGINT);
                CREATE TYPE fr AS (
                    fields iff ARRAY,
                    sf VARIANT ARRAY
                );
                CREATE TYPE fs AS (records fr ARRAY);
                
                CREATE TABLE flows (sets fs array);
                
                CREATE VIEW fp AS
                SELECT data
                FROM flows, UNNEST(flows.sets) as t (data);
                
                CREATE VIEW V AS
                SELECT a2m(r.fields) as fields
                FROM fp, UNNEST(fp.data) as r;""");
    }

    @Test
    public void issue3364() {
        this.statementsFailingInCompilation("""
                create type typ1 as (f int);
                create table t1 (x1 typ1);
                create table t2 (x1 typ1);
                create view v1 as select t1.* from t1 join t2
                on t1.x1 = t2.x1;""", "Not yet implemented: Join on struct types");
    }

    @Test
    public void missingFrom() {
        this.compileRustTestCase("""
                CREATE TYPE i as(sets variant array);
                CREATE TABLE f(
                    ts TIMESTAMP NOT NULL LATENESS INTERVAL 5 SECONDS,
                    i I);
                
                CREATE VIEW V AS
                SELECT fs
                FROM f,
                     UNNEST(f.i.sets) as t(fs);""");
    }

    @Test
    public void issue3183() {
        var ccs = this.getCCS("""
                CREATE TABLE row_tbl(id INT, c1 INT, c2 VARCHAR, c3 VARCHAR);
                CREATE MATERIALIZED VIEW row_count_col_where AS SELECT
                COUNT(ROW(c1, c2, c3)) FILTER(WHERE c1 < c2) AS c1
                FROM row_tbl;""");
        ccs.step("INSERT INTO row_tbl VALUES(0, 4, NULL, 'adios')",
                """
                  c | weight
                 ------------
                  0 | 1""");
    }

    @Test
    public void issue2595() {
        var ccs = this.getCCS("""
                CREATE VIEW V AS SELECT CAST(PARSE_JSON('[1, null]') AS INT ARRAY)""");
        ccs.step("", """
                  array | weight
                 ----------------
                  {1, NULL} | 1""");
    }

    @Test
    public void issue3259() {
        this.statementsFailingInCompilation("create table t0(c0 decimal(0, 0));",
                "DECIMAL precision 0 must be between 1 and 38");
    }

    @Test
    public void issue3247() {
        this.getCCS("""
                CREATE TABLE t3(c0 CHAR(21));
                CREATE TABLE t9(c0 VARCHAR);
                CREATE VIEW v6 AS (SELECT * FROM t3 NATURAL JOIN t9);""");
    }

    @Test
    public void recursionTest() {
        this.getCCS("""
                CREATE TABLE B(id VARCHAR);
                CREATE VIEW V1 AS SELECT id, 'x' as y FROM B;
                CREATE VIEW V2 AS SELECT id, 'xx' as y FROM B;
                CREATE VIEW U AS SELECT * FROM V1 UNION ALL SELECT * FROM V2;
                
                DECLARE RECURSIVE VIEW e(id TEXT, y VARCHAR(2) NOT NULL);
                CREATE VIEW e AS
                SELECT * FROM U;""");
    }

    @Test
    public void issue3256() {
        this.compileRustTestCase("""
                CREATE TABLE t0(c0 int);
                CREATE VIEW v6 AS (
                    SELECT 1 FROM t0 WHERE
                    FALSE
                    NOT BETWEEN
                    (
                        (
                            '7983-8-6 4:8:10'::TIMESTAMP IS DISTINCT FROM '6895-2-30 16:1:47'::TIMESTAMP
                        ) NOT BETWEEN (
                            ARRAY_CONTAINS(array[1.531442425, 1.542531961E9, 5.09355347E8], 1.0164673495)
                        ) AND true
                    ) AND true
                );""");
        this.compileRustTestCase("""
                CREATE TABLE t0(c0 BOOLEAN);
                CREATE VIEW v5 AS
                (SELECT 1 FROM t0 WHERE (ARRAYS_OVERLAP(array[1, 1, 3], array[1]))
                BETWEEN (ARRAY_CONTAINS(array[1.7727161785], 1.0)) AND ((CAST('5280-11-23 14:7:53' AS TIMESTAMP))
                BETWEEN (CAST('7227-11-13 15:49:21' AS TIMESTAMP)) AND (CAST('8869-5-1 10:45:55' AS TIMESTAMP))));""");
    }

    @Test
    public void issue3257() {
        this.compileRustTestCase(
                "CREATE VIEW v7 AS " +
                        "(SELECT ARRAY_TO_STRING(array[CAST('1:17:23' AS TIME), CAST('3:9:29' AS TIME)], ''));");
    }

    @Test
    public void issue3258() {
        this.compileRustTestCase("""
                CREATE TABLE t0(c0 int);
                CREATE VIEW v6 AS (SELECT COUNT(1) FROM t0 WHERE
                ARRAYS_OVERLAP(array[array[CAST('1173-8-15 5:11:20' AS TIMESTAMP)]],
                array[array[CAST('5973-8-27 8:0:9' AS TIMESTAMP)]]));""");
    }

    @Test
    public void issue3114() {
        this.compileRustTestCase("""
                CREATE TABLE T(x integer);
                LATENESS V.x 1;
                CREATE VIEW V
                    AS SELECT
                        x,
                        MAP['x', x] AS map,
                        ARRAY[1, x] as a,
                        ROW(1, x) as r
                    FROM T;""");
    }

    @Test
    public void internalTest177() {
        this.getCCS("""
                CREATE TABLE T(x BIGINT);
                CREATE FUNCTION F(seconds BIGINT)
                RETURNS TIMESTAMP AS
                TIMESTAMPADD(SECOND, SECONDS, DATE '1970-01-01');
                CREATE VIEW V AS SELECT x FROM T WHERE x > 1 OR F(x) > NOW();""");
    }

    @Test
    public void issue3164() {
        this.compileRustTestCase("""
                CREATE TABLE T(x integer LATENESS 1);
                CREATE VIEW V
                    AS SELECT
                        x,
                        CAST(x as VARCHAR)
                    FROM T;""");
    }

    @Test
    public void latenessType() {
        this.statementsFailingInCompilation("""
                CREATE TABLE T(x integer);
                LATENESS V.x INTERVAL '10' SECONDS;
                CREATE VIEW V AS SELECT
                   x,
                   MAP['x', x] AS map
                FROM T;""", "Cannot apply '-' to arguments of type '<INTEGER> - <INTERVAL SECOND>'");
    }

    @Test
    public void issue3159() {
        this.compileRustTestCase("""
                CREATE TABLE t2(c0 TINYINT);
                CREATE VIEW v2_optimized AS (SELECT POWER(5, t2.c0) FROM t2);
                """);
    }

    @Test
    public void issue3158() {
        this.compileRustTestCase("""
                CREATE TABLE t3(c0 SMALLINT);
                CREATE VIEW v0 AS (SELECT CHR(t3.c0) FROM t3);""");
    }

    @Test
    public void issue3109() {
        var cc = this.getCC("""
                CREATE TYPE other_type AS (s string);
                
                CREATE TYPE my_type AS (
                    other other_type ARRAY,
                    s string
                );
                
                CREATE TABLE t (content my_type ARRAY);
                
                create view v as (
                    select content_flat.*
                    from t, unnest(t.content) as content_flat
                );""");
        CircuitVisitor visitor = new CircuitVisitor(cc.compiler) {
            @Override
            public void postorder(DBSPMapOperator operator) {
                throw new RuntimeException("Map operator should have been eliminated");
            }
        };
        cc.visit(visitor);
    }

    @Test
    public void issue3086() {
        this.getCCS("""
                CREATE TABLE map_tbl(c1 MAP<VARCHAR, INT>, c2 MAP<VARCHAR, INT>);
                CREATE MATERIALIZED VIEW v AS SELECT
                ARG_MAX(c1, c2) AS arg_max,
                ARG_MIN(c1, c2) AS arg_min
                FROM map_tbl;""");
    }

    @Test
    public void issue3083() {
        var ccs = this.getCCS("""
                CREATE TABLE timestamp_tbl(c1 TIMESTAMP, c2 TIMESTAMP);
                CREATE LOCAL VIEW atbl_interval_months AS SELECT
                (c1 - c2)MONTH AS c1_minus_c2,
                (c2 - c1)MONTH AS c2_minus_c1
                FROM timestamp_tbl;
                
                CREATE VIEW atbl_interval_months_res AS SELECT
                (c1_minus_c2) = -(c2_minus_c1) AS eq
                FROM atbl_interval_months;""");
        ccs.step("""
                INSERT INTO timestamp_tbl VALUES('2019-12-05 08:27:00', '2014-11-05 12:45:00');
                INSERT INTO timestamp_tbl VALUES('2020-06-21 14:00:00', '2023-02-26 18:00:00');""",
                """ 
                 eq   | weight
                ------------------
                 true | 1
                 true | 1""");
    }

    @Test
    public void issue3814b() {
        var ccs = this.getCCS("""
                CREATE TYPE person AS (
                    id INTEGER,
                    name TEXT);
                
                CREATE TABLE example_table(data person ARRAY);
                
                CREATE VIEW V AS SELECT element.id
                FROM example_table,
                UNNEST(data) AS element;""");
        ccs.step("""
                INSERT INTO example_table (data) VALUES
                (ARRAY[ROW(1, 'Alice')::person, ROW(2, 'Bob')::person, NULL, ROW(NULL, 'Charlie')::person]),
                (ARRAY[ROW(3, 'David')::person, ROW(NULL, NULL)::person]);""", """
                 id | weight
                -------------
                 1  | 1
                 2  | 1
                    | 3
                 3  | 1""");
    }

    @Test
    public void issue3095() {
        var cc = this.getCC("""
                CREATE FUNCTION udf(input INT) RETURNS INT;
                CREATE TABLE T (p int);
                
                CREATE LOCAL VIEW V0 AS
                SELECT udf(p) as f FROM T;
                
                CREATE VIEW V1 AS
                SELECT f+1, f+2 FROM V0;""");
        int[] functionCalls = new int[] { 0 };
        InnerVisitor visitor = new InnerVisitor(cc.compiler) {
            @Override
            public void postorder(DBSPApplyExpression node) {
                ++functionCalls[0];
            }
        };
        cc.visit(visitor.getCircuitVisitor(false));
        Assert.assertEquals(1, functionCalls[0]);
    }

    @Test
    public void issue3070() {
        this.compileRustTestCase("""
                CREATE TABLE t0(c0 INT);
                CREATE MATERIALIZED VIEW v20_optimized AS (SELECT ASCII(CHR(t0.c0)) FROM t0);""");
    }

    @Test
    public void issue3071() {
        this.getCC("""
                CREATE TABLE t0(c0 char) with ('materialized' = 'true');
                CREATE MATERIALIZED VIEW v100_optimized AS (SELECT * FROM t0 WHERE (t0.c0) NOT BETWEEN ('Y') AND ('䤈'));""");
        this.getCC("""
                CREATE MATERIALIZED VIEW v73_optimized AS SELECT 'a'>'헊';""");
    }

    @Test
    public void issue3140() {
        this.compileRustTestCase("""
                CREATE TABLE variant_tbl(c1 INT ARRAY);
                CREATE MATERIALIZED VIEW atbl_variant AS SELECT
                CAST(c1 AS VARIANT) AS c1 FROM variant_tbl;""");
    }

    @Test
    public void issue3609() {
        this.getCCS("""
              CREATE TABLE T(x INT ARRAY);
              CREATE VIEW V AS select array_join(array_distinct(x), ',') FROM T;""");
    }

    @Test
    public void issue3072() {
        this.compileRustTestCase("""
                CREATE TABLE t0(c0 VARCHAR);
                CREATE VIEW v271_optimized AS
                SELECT COUNT(RLIKE(t0.c0, '0.5123590946084831')) FROM t0;""");
        this.compileRustTestCase("""
                CREATE TABLE t0(c0 DOUBLE, c1 INT, c2 BIGINT);
                CREATE MATERIALIZED VIEW v293_optimized AS SELECT COUNT(RLIKE(SUBSTRING('', t0.c1), '')) FROM t0;""");
    }

    @Test
    public void issue3073() {
        this.compileRustTestCase("""
                CREATE MATERIALIZED VIEW v10_optimized AS
                SELECT SOME(('2059-10-5 5:3:37') NOT BETWEEN ('2171-1-14 22:23:8') AND ((-7::TINYINT)::TIMESTAMP));""");
    }

    @Test
    public void issue3076() {
        this.statementsFailingInCompilation("CREATE VIEW v AS SELECT SUM(NULL);",
                "Argument of aggregate has NULL type");
        this.compileRustTestCase("CREATE VIEW V AS SELECT SUM(CAST(NULL AS INTEGER));");
    }

    @Test
    public void issue3063() {
        this.getCCS("""
                CREATE TABLE array_tbl(c1 INT ARRAY, c2 INT ARRAY);
                CREATE MATERIALIZED VIEW v AS SELECT
                ARG_MIN(c1, c2) AS arg_min,
                ARG_MAX(c1, c2) AS arg_max
                FROM array_tbl;""");
    }

    @Test
    public void issue3035() {
        this.compileRustTestCase("""
                CREATE TABLE t0(c0 INT) with ('materialized' = 'true');
                CREATE TABLE t4(c0 DOUBLE) with ('materialized' = 'true');
                CREATE VIEW v11_optimized AS (SELECT COUNT(*) FROM t0, t4 WHERE IS_INF(ROUND(t4.c0, t0.c0)));""");
    }

    @Test
    public void issue3038() {
        this.compileRustTestCase("""
                CREATE TABLE t0(c0 VARCHAR) with ('materialized' = 'true');
                CREATE VIEW v3_optimized AS (SELECT COUNT(t0.c0) FROM t0 WHERE ((RLIKE(t0.c0, t0.c0))<(LOWER(t0.c0))));""");
    }

    @Test
    public void issue3039() {
        this.compileRustTestCase(
                "CREATE VIEW v1_optimized AS SELECT MIN(MOD(POWER(-1553189232, 1981020635), 1981020635))");
    }

    @Test
    public void issue3040() {
        this.compileRustTestCase("""
                CREATE TABLE t5(c0 BOOLEAN, c1 INT, c2 INT) with ('materialized' = 'true');
                CREATE MATERIALIZED VIEW v40_optimized AS (SELECT SUM(t5.c2) FROM t5 WHERE (('X')<(CHR(t5.c2))));""");
    }

    @Test
    public void issue3041() {
        this.statementsFailingInCompilation("CREATE MATERIALIZED VIEW v199 AS SELECT TRIM();",
                "Error parsing SQL: Encountered \")\" at line 1");
    }

    @Test
    public void issue3042() {
        this.compileRustTestCase("""
                CREATE TABLE t3(c0 DOUBLE) with ('materialized' = 'true');
                CREATE TABLE t4(c0 VARCHAR, c1 INT, c2 VARCHAR, c3 BOOLEAN, c4 BOOLEAN) with ('materialized' = 'true');
                CREATE VIEW v29_optimized AS (SELECT AVG(TRUNCATE((t3.c0::DOUBLE), t4.c1)) FROM t4, t3);""");
    }

    @Test
    public void issue3043() {
        this.compileRustTestCase("""
                CREATE TABLE t1(c0 VARCHAR, c1 DOUBLE, c2 DOUBLE) with ('materialized' = 'true');
                CREATE TABLE t4(c0 BOOLEAN) with ('materialized' = 'true');
                CREATE VIEW v22_optimized AS (SELECT * FROM t4, t1 WHERE ((t4.c0)IS NOT DISTINCT FROM(IS_INF(t1.c1))));""");
    }

    @Test
    public void issue3361() {
        // Validated on Postgres, which however produces higher precision results.
        var ccs = this.getCCS("""
                CREATE TABLE dt(id INT, c1 DECIMAL(6,2), c2 DECIMAL(6,2) NOT NULL);
                
                CREATE VIEW V AS SELECT
                STDDEV_SAMP(c1) AS c1,
                STDDEV_SAMP(c2) AS c2
                FROM dt;""");
        ccs.step("""
                INSERT INTO dt VALUES
                   (0, 1111.52, 2231.90),
                   (0, NULL, 3802.71),
                   (1, 5681.08, 7689.88),
                   (1, 5681.08, 7335.88);""", """
                      c1 |      c2 | weight
                ----------------------------
                 2638.23 | 2677.47 | 1""");
    }

    @Test
    public void issue3461() {
        var ccs = this.getCCS("""
                CREATE TABLE timestamp_tbl(
                id INT,
                c1 TIMESTAMP NOT NULL,
                c2 TIMESTAMP);
                
                CREATE LOCAL VIEW ats_minus_ts AS
                SELECT id, (c1-c2)DAY TO SECOND AS dts
                FROM timestamp_tbl;
                
                CREATE LOCAL VIEW ts_minus_ts_res AS
                SELECT id, CAST((dts) AS VARCHAR) AS dts_res
                FROM ats_minus_ts;
                
                CREATE MATERIALIZED VIEW ts_minus_tssinterval AS
                SELECT
                c1 - CAST(v1.dts_res AS INTERVAL DAY TO SECOND)  AS dts
                FROM ts_minus_ts_res v1
                JOIN timestamp_tbl v2 ON v1.id = v2.id;""");
        ccs.step("INSERT INTO timestamp_tbl VALUES(2, '1959-06-21 11:32:00', '1948-12-02 09:15:00');",
                """
                  dts | weight
                 --------------
                 1948-12-02 09:15:00 | 1""");
    }

    @Test
    public void issue3030() {
        this.compileRustTestCase("""
                CREATE TABLE timestamp_tbl(c1 TIMESTAMP, c2 TIMESTAMP);
                
                CREATE LOCAL VIEW atbl_interval AS SELECT
                (c1 - c2) MONTH AS c1_minus_c2
                FROM timestamp_tbl;
                
                CREATE LOCAL VIEW interval_minus_interval AS SELECT
                ((c1_minus_c2) - INTERVAL '2' YEAR) AS c1
                FROM atbl_interval;
                
                CREATE MATERIALIZED VIEW interval_minus_interval_seconds AS SELECT
                EXTRACT(MONTH FROM c1) AS f_c1
                FROM interval_minus_interval;""");
    }

    @Test
    public void issue3210() {
        // validated on Postgres
        var ccs = this.getCCS("""
                create table t1(arr STRING ARRAY);
                create table t2 (id string);
                
                CREATE VIEW v AS
                WITH ids AS (
                  SELECT array_element.id
                  FROM t1, UNNEST(t1.arr) AS array_element(id)
                )
                SELECT * FROM ids
                WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = ids.id);""");
        ccs.step("""
                INSERT INTO t1 VALUES(array['a']), (array['b', 'd']);
                INSERT INTO t2 VALUES('a'), ('b'), ('c'), ('e');""", """
                 id | weight
                -------------
                 a| 1
                 b| 1""");
    }

    @Test
    public void issue457() {
        String sql = """
                create table t (id int, x int);
                create view v as
                select max(t.x) OVER (PARTITION BY t.id)
                from t;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue3031() {
        this.compileRustTestCase("""
                CREATE TABLE time_tbl(c1 TIME, c2 TIME);
                
                CREATE LOCAL VIEW atbl_interval AS SELECT
                (c1 - c2) SECOND AS c1_minus_c2
                FROM time_tbl;
                
                CREATE MATERIALIZED VIEW interval_negation AS SELECT
                '18:30:00':: TIME + (-c1_minus_c2) AS c1
                FROM atbl_interval;""");
    }

    @Test
    public void issue2943() {
        this.compileRustTestCase("""
                CREATE TABLE x(c1 TIMESTAMP, c2 TIMESTAMP);
                CREATE VIEW v AS SELECT (c1 - c2) SECONDS FROM x;""");
    }

    @Test
    public void recursionCrash() {
        // View defined but not used was producing a compiler crash
        String sql = "DECLARE RECURSIVE VIEW ba (id BIGINT);";
        this.getCCS(sql);
    }

    @Test
    public void issue2651() {
        this.compileRustTestCase("""
                CREATE TYPE INT32 AS INTEGER;
                CREATE TYPE AI AS INT ARRAY;
                CREATE TABLE T(x INT32, y AI);""");
    }

    @Test
    public void issueLateral() {
        this.compileRustTestCase("""
                CREATE TABLE t2 (
                  a VARCHAR,
                  ts INT,
                  x BIGINT
                );
                
                CREATE VIEW v AS
                WITH t1(a, ts) AS (VALUES('a', 1))
                SELECT * FROM t1
                LEFT JOIN LATERAL (
                    SELECT x
                    FROM t2
                    WHERE t2.a = t1.a
                      AND t2.ts <= t1.ts
                    LIMIT 1
                ) ON true
                LEFT JOIN LATERAL (
                    SELECT x
                    FROM t2
                    WHERE t2.a = t1.a
                ) ON true;""");
    }

    @Test
    public void issue2639() {
        String sql = """
                CREATE TABLE t (
                    bin BINARY
                ) with ('materialized' = 'true');
                
                CREATE FUNCTION nbin2nbin(i BINARY NOT NULL) RETURNS BINARY NOT NULL AS i;
                
                CREATE MATERIALIZED VIEW v AS
                SELECT
                    nbin2nbin(bin)
                FROM t;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue2942() {
        var ccs = this.getCCS("CREATE VIEW v1(c0) AS SELECT '9,\uE8C7voz[*'");
        ccs.step("", """
         c0 | weight
        -------------
         9,voz[*|1""");
    }

    @Test
    public void issue2642() {
        String sql = """
                create table t (
                    v VARIANT
                );
                create view v as select COALESCE(v, VARIANTNULL()) from t;
                """;
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue2640() {
        String sql = """
                CREATE TABLE t (
                    bin VARBINARY
                ) with ('materialized' = 'true');
                CREATE FUNCTION nbin2nbin2(i VARBINARY NOT NULL) RETURNS VARBINARY NOT NULL AS CAST(x'ABCD' as VARBINARY);
                CREATE MATERIALIZED VIEW v AS
                SELECT
                    nbin2nbin2(bin)
                FROM t;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void testOuterDuplicate() {
        // Validated on Postgres
        String sql = """
                CREATE TABLE t1(x int, y int);
                CREATE TABLE t2(z int, w int);
                CREATE VIEW W AS SELECT * FROM t1 LEFT JOIN t2 on x = z;""";
        var ccs = this.getCCS(sql);
        ccs.step("""
                INSERT INTO t1 VALUES(1, 0), (1, 0), (2, 0), (3, 0);
                INSERT INTO t2 VALUES(1, 1), (2, 2);
                """,
                """
                  x | y | z | w | weight
                --------------------------
                  1 | 0 | 1 | 1 | 2
                  2 | 0 | 2 | 2 | 1
                  3 | 0 |   |   | 1""");
    }

    @Test
    public void issue2641() {
        String sql = """
                create table t (
                    d DATE
                );
                create view v as select COALESCE(d, '2023-01-01') from t;
                """;
        this.compileRustTestCase(sql);
    }

    @Test
    public void existingFunction() {
        String sql = """
                CREATE FUNCTION regexp_replace(s VARCHAR, p VARCHAR, pos INTEGER)
                RETURNS VARCHAR NOT NULL AS CAST('foo' as VARCHAR);""";
        this.statementsFailingInCompilation(sql,
                "A function named 'regexp_replace' is already predefined");
    }

    @Test
    public void testUnnestWithOrdinality() {
        String sql = """
                CREATE TABLE data
                (CITIES VARCHAR ARRAY, COUNTRY VARCHAR);
                CREATE VIEW v as
                SELECT city, country
                FROM data, UNNEST(cities) WITH ORDINALITY AS city;""";
        this.getCCS(sql);
    }

    @Test
    public void testUnnest2() {
        String sql = """
                CREATE TYPE ResourceSpans AS (
                    r int,
                    scopeSpans int ARRAY
                );
                CREATE TABLE t (z INT, resourceSpans ResourceSpans ARRAY);
                
                CREATE MATERIALIZED VIEW resource_spans AS
                SELECT resourceSpans.scopeSpans
                FROM t, UNNEST(resourceSpans) WITH ORDINALITY as resourceSpans;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void testUnnest() {
        String sql = """
                CREATE TABLE tx (outputs VARIANT ARRAY);
                
                CREATE MATERIALIZED VIEW outs0 AS (
                    select output['output_type'] as json_output from tx, UNNEST(outputs) AS t (output)
                );
                
                CREATE MATERIALIZED VIEW outs1 AS (
                    select PARSE_JSON(CAST(output AS STRING)) from tx, UNNEST(outputs) AS t (output)
                );
                
                CREATE MATERIALIZED VIEW outs2 AS (
                    select (CAST(output AS VARIANT))['output_type'] from tx, UNNEST(outputs) AS t (output)
                );
                """;
        this.getCCS(sql);
    }

    @Test
    public void issue3006() {
        this.statementsFailingInCompilation("""
                CREATE TABLE user(id BIGINT);
                CREATE TABLE t(user_id BIGINT);
                
                CREATE MATERIALIZED VIEW v AS
                SELECT t.user_id
                FROM user, t
                WHERE user.id = t.user_id;""",
                "Table 'user' has the same name as a predefined function");
    }

    @Test
    public void issue2638() {
        String sql = """
                CREATE TABLE t (
                    m MAP<VARCHAR, VARCHAR>
                ) with ('materialized' = 'true');
                
                CREATE FUNCTION map2map(i MAP<VARCHAR, VARCHAR>) RETURNS MAP<VARCHAR, VARCHAR>;
                CREATE FUNCTION nmap2nmap(i MAP<VARCHAR, VARCHAR> NOT NULL) RETURNS MAP<VARCHAR, VARCHAR> NOT NULL;
                
                CREATE MATERIALIZED VIEW v AS
                SELECT
                    map2map(m),
                    nmap2nmap(m)
                FROM t;""";
        // This is not executed, since the udfs have no definitions
        var cc = this.getCC(sql);
        // Test that code generation does not crash
        ToRustVisitor.toRustString(cc.compiler, new IndentStreamBuilder(),
                cc.getCircuit(), new ProjectDeclarations());
    }

    @Test
    public void tableAfterView() {
        // Test that tables created after views
        // are inserted in Rust before views
        String sql = """
               CREATE TABLE t(id int);
               CREATE MATERIALIZED VIEW test AS
               SELECT * FROM t;
               CREATE TABLE s (id int);
               """;
        this.compileRustTestCase(sql);
    }

    @Test
    public void vecStringCast() {
        String sql = "CREATE VIEW V AS SELECT CAST(ARRAY [1,2,3] AS VARCHAR)";
        this.statementsFailingInCompilation(sql,
                "Cast function cannot convert value of type INTEGER NOT NULL ARRAY NOT NULL to type VARCHAR");
    }

    @Test
    public void issue2539() {
        String sql = """
                CREATE TABLE t(c1 INT, c2 INT);
                CREATE VIEW v AS SELECT
                ARRAY_AGG(c1) FILTER(WHERE (c1+c2)>3)
                FROM t;""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        ccs.step("INSERT INTO t VALUES (2, 3), (5, 6), (2, 1);",
                """
                         result | weight
                        ------------------
                         {2,5} | 1""");
    }

    @Test
    public void issue2649() {
        String sql = """
                CREATE TABLE T(id INT, c1 INT, c2 INT);
                CREATE VIEW V AS SELECT
                ARG_MIN(c1, c1) FILTER(WHERE c2 > 2) AS f_c1,
                ARG_MAX(c2, c2) FILTER(WHERE id = 1) AS f_c2
                FROM T;""";
        var ccs = this.getCCS(sql);
        ccs.step("INSERT INTO T VALUES (0, 5, 8), (1, 4, 2), (0, NULL, 3), (1, NULL, 5);",
                """
                 f_c1 | f_c2 | weight
                ----------------------
                    5 |    5 |     1""");
    }

    @Test
    public void issue3165() {
        this.q("""
             SELECT ASCII('8') >= ABS(1.1806236821);
              r
             ---
              true""");
    }

    @Test
    public void issue3180() {
        this.getCCS("CREATE VIEW V AS SELECT ROW(1, 'x') = ROW('x', 1);");
    }

    @Test
    public void issue2316() {
        String sql = """
                CREATE TABLE sum(c1 TINYINT);
                CREATE VIEW sum_view AS SELECT SUM(c1) AS c1 FROM sum;
                """;
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(sql);
        CompilerCircuitStream ccs = this.getCCSFailing(compiler, "Error converting 128 to TINYINT");
        ccs.step("INSERT INTO sum VALUES (127), (1);",
                " result" +
                        "---------");
    }

    @Test
    public void issue3235() {
        this.getCCS("""
                create table users (
                    id bigint not null primary key,
                    is_banned bool,
                    country string,
                    name string
                );
                
                create table groups (
                    id bigint not null primary key,
                    name string
                );
                
                create table members (
                    user_id  bigint not null,
                    group_id bigint not null
                );
                
                create table files (
                    id bigint not null primary key,
                    name string,
                    parent_id bigint,
                    is_public bool,
                    folder bool
                );
                
                create table group_file_owner (
                    group_id bigint not null,
                    file_id bigint not null
                );
                
                create table group_file_viewer (
                    group_id bigint not null,
                    file_id bigint not null
                );
                
                declare recursive view group_can_read (
                    group_id bigint not null,
                    file_id bigint not null
                );
                
                declare recursive view group_can_write (
                    group_id bigint not null,
                    file_id bigint not null
                );
                
                create materialized view group_can_write as
                select group_id, file_id from group_file_owner
                UNION ALL
                select
                    group_can_write.group_id,
                    files.id as file_id
                from
                    group_can_write join files on group_can_write.file_id = files.parent_id;
                
                create materialized view group_can_read as
                select group_id, file_id from group_file_viewer
                UNION ALL
                select group_id, file_id from group_can_write
                UNION ALL
                select
                    group_can_read.group_id,
                    files.id as file_id
                from
                    group_can_read join files on group_can_read.file_id = files.parent_id;
                
                create view user_can_write as
                select
                    members.user_id,
                    group_can_write.file_id
                from
                    members join group_can_write on members.group_id = group_can_write.group_id;""");
    }

    @Test
    public void testVariantCast() {
        String sql = """
                CREATE TABLE variant_table(val VARIANT);
                CREATE VIEW typed_view AS SELECT
                    CAST(val['scores'] AS DECIMAL ARRAY) as scores
                FROM variant_table;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void timestamp() {
        String sql = """
                CREATE FUNCTION MAKE_TIMESTAMP(SECONDS BIGINT) RETURNS TIMESTAMP AS
                TIMESTAMPADD(SECOND, SECONDS, DATE '1970-01-01');

                CREATE TABLE T(c1 BIGINT);
                CREATE VIEW sum_view AS SELECT MAKE_TIMESTAMP(c1) FROM T;
                """;
        CompilerCircuitStream ccs = this.getCCS(sql);
        ccs.step("INSERT INTO T VALUES (10000000);",
                """
                         result | weight
                        -------------------
                         1970-04-26 17:46:40 | 1""");
    }

    @Test
    public void testFpCast() {
        String sql = """
                CREATE TABLE TAB2 (COL0 INTEGER, COL1 INTEGER, COL2 INTEGER);
                CREATE VIEW V100 AS
                SELECT 99 * - COR0.COL0
                FROM TAB2 AS COR0
                WHERE NOT - COR0.COL2 * + CAST(+ COR0.COL0 AS REAL) >= + (- COR0.COL2)
                """;
        this.compileRustTestCase(sql);
    }

    @Test
    public void testTPCHQ5Simple() {
        String sql = """
                CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,
                                        L_PARTKEY     INTEGER NOT NULL,
                                        L_SUPPKEY     INTEGER NOT NULL,
                                        L_LINENUMBER  INTEGER NOT NULL,
                                        L_QUANTITY    DECIMAL(15,2) NOT NULL,
                                        L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                                        L_DISCOUNT    DECIMAL(15,2) NOT NULL,
                                        L_TAX         DECIMAL(15,2) NOT NULL,
                                        L_RETURNFLAG  CHAR(1) NOT NULL,
                                        L_LINESTATUS  CHAR(1) NOT NULL,
                                        L_SHIPDATE    DATE NOT NULL,
                                        L_COMMITDATE  DATE NOT NULL,
                                        L_RECEIPTDATE DATE NOT NULL,
                                        L_SHIPINSTRUCT CHAR(25) NOT NULL,
                                        L_SHIPMODE     CHAR(10) NOT NULL,
                                        L_COMMENT      VARCHAR(44) NOT NULL);

                create view q5 as
                select
                    sum(l_extendedprice * (1 - l_discount)) as revenue
                from
                    lineitem
                """;
        this.compileRustTestCase(sql);
    }

    @Test
    public void decimalMult() {
        String sql = """
                CREATE TABLE T(C DECIMAL(16, 2));
                CREATE VIEW V AS SELECT 100.20 * T.C FROM T;""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        ccs.step("INSERT INTO T VALUES (100.0)",
                """
                         value  | weight
                        ----------------
                          10020 | 1""");
    }

    @Test
    public void issue2438() {
        String sql = """
                CREATE TABLE t(
                   id INT, c1 REAL,
                   c2 REAL NOT NULL);
                CREATE VIEW v AS SELECT
                   AVG(c1) FILTER (WHERE c2 > -3802.271) AS f_c1,
                   AVG(c2) FILTER (WHERE c2 > -3802.271) AS f_c2
                FROM t;""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        ccs.step("""
                INSERT INTO t VALUES
                (0, -1111.5672,  2231.790),
                (0, NULL, -3802.271),
                (1, 57681.08, 71689.8057),
                (1, 57681.08, 87335.89658)""",
                """
                 c1       | c2      | weight
                -----------------------------
                 38083.53 | 53752.5 | 1""");
    }

    @Test
    public void issue2350() {
        String sql = """
                CREATE TABLE arg_min(c1 TINYINT NOT NULL);
                CREATE VIEW arg_min_view AS SELECT
                ARG_MIN(c1, c1) AS c1
                FROM arg_min""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        ccs.step("""
                INSERT INTO arg_min VALUES (2), (3), (5)""",
                """
                 value | weight
                ----------------
                  2    | 1""");
    }

    @Test
    public void issue2261() {
        String sql = """
                CREATE TABLE stddev_groupby(id INT, c2 TINYINT NOT NULL);
                CREATE VIEW stddev_view AS SELECT
                STDDEV_SAMP(c2) AS c2
                FROM stddev_groupby
                GROUP BY id;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void testLag() {
        String sql = """
                CREATE TABLE foo (
                    id INT NOT NULL,
                    card_id INT,
                    ttime INT
                );

                CREATE VIEW bar AS
                select
                    id,
                    ttime,
                    lag(ttime, 1) OVER (PARTITION BY card_id ORDER BY ttime) as lag1,
                    lag(ttime, 2) OVER (PARTITION BY card_id ORDER BY ttime) as lag2
                from foo;""";

        // Validated on postgres
        CompilerCircuitStream ccs = this.getCCS(sql);
        ccs.step("""
                INSERT INTO foo VALUES(2, 2, 10);
                INSERT INTO foo VALUES(2, 2, 10);
                INSERT INTO foo VALUES(30, 2, 12);
                INSERT INTO foo VALUES(50, 2, 13);""",
                """
                 id | ttime | lag1 | lag2 | weight
                ------------------------------------
                 2  | 10   |      |        | 1
                 2  | 10   | 10   |        | 1
                 30 | 12   | 10   | 10     | 1
                 50 | 13   | 12   | 10     | 1""");
    }

    @Test
    public void issue2333() {
        this.q("""
                SELECT TIMESTAMP '1970-01-01 00:00:00' - INTERVAL 1 HOURS;
                 result
                --------
                 1969-12-31 23:00:00""");
    }

    @Test
    public void issue2315() {
        String sql = """
                CREATE TABLE T(id INT, c6 INT NOT NULL);

                CREATE VIEW stddev_view AS
                SELECT id, STDDEV_SAMP(c6) AS c6
                FROM T
                GROUP BY id;""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        ccs.step("""
                       INSERT INTO T VALUES(0, 6);
                       INSERT INTO T VALUES(1, 3);""",
                """
                        id | c6 | weight
                       -----------------
                         0 |    | 1
                         1 |    | 1""");
    }

    @Test
    public void issue2090() {
        String sql = """
                CREATE TABLE example (
                    id INT
                ) WITH (
                    'connector' = 'value1'
                                  'value2'
                );""";
        this.statementsFailingInCompilation(sql, "Expected a simple string");
    }

    @Test
    public void issue2201() {
        String sql = """
                create table customer_address(
                    ca_zip char(10)
                );

                CREATE VIEW V AS SELECT substr(ca_zip,1,5)
                FROM customer_address;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue1189() {
        String sql = """
                create table EVENT_DURATION_V(duration bigint, event_type_id bigint);
                create table EVENTTYPE_T(id bigint, name string);

                CREATE VIEW SHORTEST_ALARMS_TYPE_V AS
                SELECT duration
                ,      event_type_id
                ,      ett.name
                FROM   (SELECT duration
                        ,      event_type_id
                        ,      ROW_NUMBER() OVER (PARTITION BY event_type_id
                                                  ORDER BY duration ASC) AS rnum
                        FROM   EVENT_DURATION_V) a
                        JOIN EVENTTYPE_T ett ON a.event_type_id = ett.id
                WHERE   rnum <= 1
                ;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue2017() {
        String sql = """
                CREATE TABLE customer (
                    c_id INT NOT NULL,
                    c_d_id INT NOT NULL,
                    c_w_id INT NOT NULL,
                    c_first VARCHAR(16),
                    c_middle CHAR(2),
                    c_last VARCHAR(16),
                    c_street_1 VARCHAR(20),
                    c_street_2 VARCHAR(20),
                    c_city VARCHAR(20),
                    c_state CHAR(2),
                    c_zip CHAR(9),
                    c_phone CHAR(16),
                    c_since TIMESTAMP,
                    c_credit CHAR(2),
                    c_credit_lim DECIMAL(12,2),
                    c_discount DECIMAL(4,4),
                    c_balance DECIMAL(12,2),
                    c_ytd_payment DECIMAL(12,2),
                    c_payment_cnt INT,
                    c_delivery_cnt INT,
                    c_data VARCHAR(500),
                    PRIMARY KEY (c_w_id, c_d_id, c_id),
                    FOREIGN KEY (c_w_id, c_d_id) REFERENCES district(d_w_id, d_id)
                );

                CREATE TABLE transaction_parameters (
                    txn_id INT NOT NULL PRIMARY KEY,
                    w_id INT,
                    d_id INT,
                    c_id INT,
                    c_w_id INT,
                    c_d_id INT,
                    c_last VARCHAR(20), -- TODO check
                    h_amount DECIMAL(5,2),
                    h_date TIMESTAMP,
                    datetime_ TIMESTAMP
                );

                CREATE VIEW cust_max AS
                SELECT c.c_first, c.c_middle, c.c_id,
                    c.c_street_1, c.c_street_2, c.c_city, c.c_state, c.c_zip,
                    c.c_phone, c.c_credit, c.c_credit_lim,
                    c.c_discount, c.c_balance, c.c_since
                FROM customer AS c,
                     transaction_parameters AS t
                WHERE c.c_last = t.c_last
                  AND c.c_d_id = t.c_d_id
                  AND c.c_w_id = t.c_w_id
                  AND c_first = (select max(c_first) from customer LIMIT 1)
                LIMIT 1;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue2094() {
        String sql = "CREATE FUNCTION F0() RETURNS INT NOT NULL AS 0;";
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue2095() {
        String sql = """
        CREATE FUNCTION F1() RETURNS INT NOT NULL AS 0;
        CREATE FUNCTION G() RETURNS INT NOT NULL AS F1();""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void testErrorPosition() {
        // Test that errors for functions report correct source position
        String sql = """
                CREATE FUNCTION ANCHOR_TIMESTAMP() RETURNS TIMESTAMP NOT NULL
                  AS TIMESTAMP '2024-01-01 00:00:00';

                CREATE FUNCTION ROUND_TIMESTAMP(ts TIMESTAMP, billing_interval_days INT) RETURNS TIMESTAMP
                  AS TRUNC(DATEDIFF(DAYS, ts, ANCHOR_TIMESTAMP())) + ANCHOR_TIMESTAMP();
                """;
        this.statementsFailingInCompilation(sql, """
                    5|  AS TRUNC(DATEDIFF(DAYS, ts, ANCHOR_TIMESTAMP())) + ANCHOR_TIMESTAMP();
                           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                """);
    }

    @Test
    public void issue1868() {
        String sql = """
                CREATE TABLE example_a (
                    id INT NOT NULL
                );

                CREATE TABLE example_b (
                    id INT NOT NULL
                );

                CREATE VIEW example_c AS (
                    SELECT COALESCE(example_a.id, 0) - COALESCE(example_b.id, 0)
                    FROM example_a
                         FULL JOIN example_b
                         ON example_a.id = example_b.id
                );""";
        this.compileRustTestCase(sql);
    }

    // Test for https://github.com/feldera/feldera/issues/1151
    @Test
    public void issue1151() {
        String sql = "CREATE TABLE event_t ( id BIGINT NOT NULL PRIMARY KEY, local_event_dt DATE )";
        this.compileRustTestCase(sql);
    }

    @Test
    public void testFilterPull() {
        // Example used in a blog post
        String sql = """
                CREATE TABLE transaction (
                   cc_num int
                );

                CREATE TABLE users (
                   cc_num int,
                   id bigint,
                   age int
                );

                CREATE VIEW transaction_with_user AS
                SELECT
                    transaction.*,
                    users.id as user_id,
                    users.age
                FROM
                    transaction JOIN users
                ON users.cc_num = transaction.cc_num
                WHERE
                    users.age >= 21;""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(sql);
        DBSPCircuit circuit = getCircuit(compiler);
        CircuitVisitor visitor = new CircuitVisitor(compiler) {
            int filterJoin = 0;

            @Override
            public void postorder(DBSPJoinFilterMapOperator operator) {
                this.filterJoin++;
            }

            @Override
            public void endVisit() {
                // If the filter for age is not pulled above the join, it
                // will produce a JoinFilterMap operator.
                Assert.assertEquals(0, this.filterJoin);
            }
        };
        visitor.apply(circuit);
    }

    @Test
    public void issue3326() {
        this.compileRustTestCase("""
                CREATE TYPE KeyValue AS (
                    key VARCHAR,
                    value VARIANT
                );
                
                CREATE TYPE Event AS (
                    timeUnixNano VARCHAR,
                    name VARCHAR,
                    attributes KeyValue ARRAY
                );
                
                CREATE TYPE Span AS (
                    traceId VARCHAR,
                    spanId VARCHAR,
                    traceState VARCHAR,
                    parentSpanId VARCHAR,
                    flags BIGINT,
                    name VARCHAR,
                    kind INT,
                    startTimeUnixNano VARCHAR,
                    endTimeUnixNano VARCHAR,
                    attributes KeyValue ARRAY,
                    events Event ARRAY
                );
                
                CREATE TYPE Scope AS (
                    name VARCHAR,
                    version VARCHAR,
                    attributes KeyValue ARRAY
                );
                
                CREATE TYPE ScopeSpans AS (
                    scope Scope,
                    spans Span ARRAY
                );
                
                CREATE TYPE Resource AS (
                    attributes KeyValue ARRAY
                );
                
                CREATE TYPE ResourceSpans AS (
                    resource Resource,
                    scopeSpans ScopeSpans ARRAY
                );
                
                CREATE TYPE Metric AS (
                    name VARCHAR,
                    description VARCHAR,
                    unit VARCHAR,
                    data VARIANT,
                    metadata KeyValue ARRAY
                );
                
                CREATE TYPE ScopeMetrics AS (
                    scope Scope,
                    metrics Metric ARRAY
                );
                
                CREATE TYPE ResourceMetrics AS (
                    resource Resource,
                    scopeMetrics ScopeMetrics ARRAY
                );
                
                CREATE TYPE LogRecords AS (
                    attributes KeyValue ARRAY,
                    timeUnixNano VARCHAR,
                    observedTimeUnixNano VARCHAR,
                    severityNumber INT,
                    severityText VARCHAR,
                    flags INT4,
                    traceId VARCHAR,
                    spanId VARCHAR,
                    eventName VARCHAR,
                    body VARIANT
                );
                
                CREATE TYPE ScopeLogs AS (
                    scope Scope,
                    logRecords LogRecords ARRAY
                );
                
                CREATE TYPE ResourceLogs AS (
                    resource Resource,
                    scopeLogs ScopeLogs ARRAY
                );
                
                CREATE TABLE otel_traces (
                    resourceSpans ResourceSpans ARRAY
                ) WITH ('append_only' = 'true');
                
                CREATE TABLE otel_logs (
                    resourceLogs ResourceLogs ARRAY
                ) WITH ('append_only' = 'true');
                
                CREATE TABLE otel_metrics (
                    resourceMetrics ResourceMetrics ARRAY
                ) WITH ('append_only' = 'true');
                
                CREATE MATERIALIZED VIEW metrics AS
                SELECT
                    resourceMetrics.*
                FROM
                    otel_metrics, UNNEST(resourceMetrics) as resourceMetrics;
                
                CREATE MATERIALIZED VIEW traces AS
                SELECT
                    resourceSpans.*
                FROM otel_traces, UNNEST(resourceSpans) as resourceSpans;
                
                CREATE MATERIALIZED VIEW logs AS
                SELECT
                    resourceLogs.*
                FROM otel_logs, UNNEST(resourceLogs) as resourceLogs;
                
                CREATE VIEW scope_spans AS
                SELECT
                    scopeSpan.scope['name'] as scopeName,
                    scopeSpan.spans
                FROM
                    traces, UNNEST(scopeSpans) AS scopeSpan;""");
    }

    @Test
    public void issue1898() {
        String sql = """
                create table t(
                    id bigint,
                    part bigint
                );

                create view v as
                SELECT
                    id,
                    COUNT(DISTINCT id) FILTER (WHERE id > 100) OVER window_100 AS agg
                FROM
                    t
                WINDOW
                    window_100 AS (PARTITION BY part ORDER BY id RANGE BETWEEN 100 PRECEDING AND CURRENT ROW);""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.throwOnError = false;
        compiler.submitStatementsForCompilation(sql);
        TestUtil.assertMessagesContain(compiler, "OVER must be applied to aggregate function");
    }

    @Test
    public void issue2027() {
        // validated with Postgres 15
        // Generates DBSPPartitionedRollingAggregateOperator
        String sql = """
                CREATE TABLE T (
                   id INT,
                   amt INT,
                   ts TIMESTAMP
                );

                CREATE VIEW V AS SELECT
                    id,
                    amt,
                    SUM(amt) OVER window1 AS s1,
                    SUM(amt) OVER window2 AS s2,
                    SUM(amt) OVER window3 AS s3,
                    SUM(amt) OVER window4 AS s4
                FROM T WINDOW
                window1 AS (PARTITION BY id ORDER BY EXTRACT(HOUR FROM ts) RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING),
                window2 AS (PARTITION BY id ORDER BY ts RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND INTERVAL 1 MINUTE FOLLOWING),
                window3 AS (PARTITION BY id ORDER BY CAST(ts AS DATE) RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND INTERVAL 1 MINUTE FOLLOWING),
                window4 AS (PARTITION BY id ORDER BY CAST(ts AS TIME) RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND INTERVAL 1 MINUTE FOLLOWING);""";
           CompilerCircuitStream ccs = this.getCCS(sql);
        ccs.step("""
                        INSERT INTO T VALUES(0, 1, '2024-01-01 00:00:00');
                        INSERT INTO T VALUES(1, 2, '2024-01-01 00:00:00');
                        INSERT INTO T VALUES(0, 3, '2024-01-01 00:00:01');
                        INSERT INTO T VALUES(0, 4, '2024-01-01 00:00:01');
                        INSERT INTO T VALUES(0, 5, '2024-01-01 00:10:00');
                        INSERT INTO T VALUES(0, 6, '2024-01-01 00:11:00');
                        INSERT INTO T VALUES(0, 7, '2024-01-01 00:13:00');""",
                """
                        id | amt | s1 | s2 | s3 | s4 | weight
                       ---------------------------------------
                        0  | 1   | 26 | 8  | 26 | 8  | 1
                        0  | 3   | 26 | 8  | 26 | 8  | 1
                        0  | 4   | 26 | 8  | 26 | 8  | 1
                        0  | 5   | 26 | 19 | 26 | 19 | 1
                        0  | 6   | 26 | 19 | 26 | 19 | 1
                        0  | 7   | 26 | 26 | 26 | 26 | 1
                        1  | 2   | 2  | 2  | 2  | 2  | 1"""
        );
    }

    @Test
    public void issue2027negative() {
        this.statementsFailingInCompilation("""
                CREATE TABLE t (
                   id INT,
                   amt INT,
                   ts TIMESTAMP
                );

                CREATE VIEW V AS SELECT
                    SUM(amt) OVER window1 AS s1
                FROM t WINDOW
                window1 AS (PARTITION BY id ORDER BY ts RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND INTERVAL 1 YEAR FOLLOWING);""",
                "Can you rephrase the query using an interval");
    }

    @Test
    public void issue2027negative1() {
        this.statementsFailingInCompilation("""
                CREATE TABLE t (
                   id INT,
                   amt INT,
                   ts TIMESTAMP
                );

                CREATE VIEW V AS SELECT
                    SUM(amt) OVER window1 AS s1
                FROM t WINDOW
                window1 AS (PARTITION BY id ORDER BY ts RANGE BETWEEN INTERVAL -1 HOUR PRECEDING AND CURRENT ROW);""",
                "Window bounds must be positive");
    }

    @Test
    public void issue1768() {
        String sql = """
                CREATE TABLE transaction (
                    trans_date_time TIMESTAMP NOT NULL LATENESS INTERVAL 1 DAY,
                    cc_num BIGINT NOT NULL,
                    merchant STRING,
                    category STRING,
                    amt FLOAT64,
                    trans_num STRING,
                    unix_time INTEGER NOT NULL,
                    merch_lat FLOAT64 NOT NULL,
                    merch_long FLOAT64 NOT NULL,
                    is_fraud INTEGER
                );

                CREATE TABLE demographics (
                    cc_num BIGINT NOT NULL,
                    first STRING,
                    gender STRING,
                    street STRING,
                    city STRING,
                    state STRING,
                    zip INTEGER,
                    lat FLOAT64,
                    long FLOAT64,
                    city_pop INTEGER,
                    job STRING,
                    dob STRING
                );

                CREATE VIEW V AS SELECT
                    transaction.cc_num,
                    CASE
                      WHEN dayofweek(trans_date_time) IN(6, 7) THEN true
                      ELSE false
                    END AS is_weekend,
                    CASE
                      WHEN hour(trans_date_time) <= 6 THEN true
                      ELSE false
                    END AS is_night,
                    category,
                    AVG(amt) OVER window_1_day AS avg_spend_pd,
                    AVG(amt) OVER window_7_day AS avg_spend_pw,
                    AVG(amt) OVER window_30_day AS avg_spend_pm,
                    COUNT(*) OVER window_1_day AS trans_freq_24,
                      amt, state, job, unix_time, city_pop, is_fraud
                  FROM transaction
                  JOIN demographics
                  ON transaction.cc_num = demographics.cc_num
                  WINDOW
                    window_1_day AS (PARTITION BY transaction.cc_num ORDER BY unix_time RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW),
                    window_7_day AS (PARTITION BY transaction.cc_num ORDER BY unix_time RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW),
                    window_30_day AS (PARTITION BY transaction.cc_num ORDER BY unix_time RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW);""";

        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.incrementalize = true;
        compiler.submitStatementsForCompilation(sql);
        DBSPCircuit circuit = getCircuit(compiler);
        CircuitVisitor visitor = new CircuitVisitor(compiler) {
            int mapIndex = 0;

            @Override
            public void postorder(DBSPMapIndexOperator operator) {
                this.mapIndex++;
            }

            @Override
            public void endVisit() {
                // We expect 7 MapIndex operators instead of 11 if CSE works
                Assert.assertEquals(7, this.mapIndex);
            }
        };
        visitor.apply(circuit);
    }

    @Test
    public void issue3128() {
        this.compileRustTestCase("""
                CREATE TABLE t2(c2 VARCHAR) with ('materialized' = 'true');
                CREATE MATERIALIZED VIEW v106_optimized AS (SELECT SUBSTRING(t2.c2, 1.36683) FROM t2);""");
    }

    @Test
    public void missingCast() {
        String sql = """
                create table TRANSACTION (unix_time BIGINT LATENESS 0);
                """;
        this.compileRustTestCase(sql);
    }

    @Test
    public void testDiv() {
        this.qs("""
            SELEct 95.0/100;
             r
            ----
             0.95
            (1 row)

            SELEct 95/100.0;
             r
            ----
             0.95
            (1 row)""");
    }

    @Test @Ignore("Calcite decorrelator fails")
    public void issue1956() {
        String sql = """
                CREATE TABLE auctions (
                  id INT NOT NULL PRIMARY KEY,
                  seller INT,
                  item TEXT
                );

                CREATE TABLE bids (
                  id INT NOT NULL PRIMARY KEY,
                  buyer INT,
                  auction_id INT,
                  amount INT
                );

                CREATE VIEW V AS SELECT id, (SELECT array_agg(buyer) FROM (
                  SELECT buyer FROM bids WHERE auction_id = auctions.id
                  ORDER BY buyer LIMIT 10
                )) FROM auctions;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue1957() {
        String sql = """
                CREATE TABLE warehouse (
                   id INT NOT NULL PRIMARY KEY,
                   parentId INT
                );

                CREATE VIEW V AS SELECT
                  id,
                  (SELECT ARRAY_AGG(id) FROM (
                    SELECT id FROM warehouse WHERE parentId = warehouse.id
                    ORDER BY id LIMIT 10
                  )) AS first_children
                FROM warehouse;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue1793() {
        String sql = """
                CREATE TABLE transaction_demographics (
                    trans_date_time TIMESTAMP,
                    cc_num BIGINT NOT NULL,
                    category STRING,
                    amt FLOAT64,
                    unix_time INTEGER NOT NULL LATENESS 86400,
                    first STRING,
                    state STRING,
                    job STRING,
                    city_pop INTEGER,
                    is_fraud BOOLEAN
                );

                CREATE VIEW V AS SELECT
                    cc_num,
                    CASE
                      WHEN dayofweek(trans_date_time) IN(6, 7) THEN true
                      ELSE false
                    END AS is_weekend,
                    CASE
                      WHEN hour(trans_date_time) <= 6 THEN true
                      ELSE false
                    END AS is_night,
                    category,
                    AVG(amt) OVER window_1_day AS avg_spend_pd,
                    AVG(amt) OVER window_7_day AS avg_spend_pw,
                    AVG(amt) OVER window_30_day AS avg_spend_pm,
                    COUNT(*) OVER window_1_day AS trans_freq_24,
                      amt, state, job, unix_time, city_pop, is_fraud
                  FROM transaction_demographics
                  WINDOW
                    window_1_day AS (PARTITION BY cc_num ORDER BY unix_time RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW),
                    window_7_day AS (PARTITION BY cc_num ORDER BY unix_time RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW),
                    window_30_day AS (PARTITION BY cc_num ORDER BY unix_time RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW);""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.incrementalize = true;
        compiler.submitStatementsForCompilation(sql);
        DBSPCircuit circuit = getCircuit(compiler);
        CircuitVisitor visitor = new CircuitVisitor(compiler) {
            int count = 0;

            @Override
            public void postorder(DBSPPartitionedRollingAggregateWithWaterlineOperator operator) {
                this.count++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(3, this.count);
            }
        };
        visitor.apply(circuit);
        this.compileRustTestCase(sql);
    }

    @Test
    public void testPanic() {
        this.q("""
                SELECT CAST(CAST(array[1] AS VARIANT) AS VARCHAR);
                 r
                ---
                NULL""");
    }

    @Test
    public void issue3313() {
        this.compileRustTestCase("""
                CREATE FUNCTION RF(R REAL) RETURNS REAL AS ROUND(R, 6);
                CREATE TABLE real_tbl(c1 REAL);""");
    }

    @Test
    public void issue3196() {
        var ccs = this.getCCS("""
                CREATE TABLE row_tbl(
                id INT,
                c1 INT NOT NULL,
                c2 VARCHAR,
                c3 VARCHAR);
                
                CREATE MATERIALIZED VIEW row_count_col_gby AS SELECT
                id, COUNT(DISTINCT ROW(c1, c2, c3)) AS c1
                FROM row_tbl
                GROUP BY id;""");
        ccs.step("""
                INSERT INTO row_tbl VALUES
                (0, 4, NULL, 'adios'),
                (0, 3, 'ola', 'ciao'),
                (1, 7, 'hi', 'hiya'),
                (1, 2, 'elo', 'ciao'),
                (1, 2, 'elo', 'ciao');""", """
                 id | count | weight
                ---------------------
                 0  |     2 | 1
                 1  |     2 | 1""");
    }

    @Test
    public void testSlt() {
        this.q("""
                SELECT CASE WHEN 1 NOT IN ( NULL, COUNT(*) ) THEN 1 END;
                 r
                ---
                NULL""");
    }

    @Test
    public void issue3199() {
        this.statementsFailingInCompilation("""
                CREATE TABLE t1(c0 VARBINARY);
                CREATE TABLE t2(c0 DECIMAL(19, 9));
                CREATE VIEW V AS (SELECT * FROM t1 NATURAL JOIN t2);""",
                "Column 'c0' matched using NATURAL keyword or USING clause has incompatible types");
        this.getCCS("""
                CREATE TABLE t1(c0 INTEGER);
                CREATE TABLE t2(c0 DECIMAL(19, 9));
                CREATE VIEW V AS (SELECT * FROM t1 NATURAL JOIN t2);""");
    }

    @Test
    public void issue3294() {
        this.compileRustTestCase("""
                CREATE TABLE not_materialized(id bigint not null);
                CREATE TABLE "TaBle1"(id bigint not null) with ('materialized' = 'true');
                
                CREATE TABLE t1 (
                    id INT NOT NULL,
                    dt DATE NOT NULL,
                    uid UUID NOT NULL
                );
                CREATE TABLE t2 (
                    id INT NOT NULL,
                    st VARCHAR NOT NULL
                );
                CREATE VIEW joined AS ( SELECT t1.dt AS c1, t2.st AS c2, t1.uid as c3 FROM t1, t2 WHERE t1.id = t2.id );
                CREATE VIEW view_of_not_materialized AS ( SELECT * FROM not_materialized );""");
    }

    @Test
    public void issue3589() {
        // validated on Postgres
        var ccs = this.getCCS("""
                CREATE TABLE tab0(pk INTEGER, col0 INTEGER, col1 REAL, col2 TEXT, col3 INTEGER);
                CREATE VIEW V AS SELECT pk FROM tab0 WHERE (col3 < 73 AND col3 IN (SELECT col0 FROM tab0 WHERE col0 = 3)) OR col1 > 8.64""");
        ccs.step("""
                INSERT INTO tab0 VALUES(0,91,79.43,'dlvog',97);
                INSERT INTO tab0 VALUES(1,57,89.18,'yzohb',80);
                INSERT INTO tab0 VALUES(2,73,81.93,'aqyub',40);
                INSERT INTO tab0 VALUES(3,61,82.61,'daxlc',42);
                INSERT INTO tab0 VALUES(4,76,101.91,'xhzcz',2);
                INSERT INTO tab0 VALUES(5,39,11.85,'zshnl',81);
                INSERT INTO tab0 VALUES(6,75,52.42,'zgvdj',49);
                INSERT INTO tab0 VALUES(7,53,53.21,'wglqx',15);
                INSERT INTO tab0 VALUES(8,67,2.2,'uaaon',98);
                INSERT INTO tab0 VALUES(9,48,62.50,'noqzf',50);""", """
                 c | weight
                -------------
                 0 | 1
                 1 | 1
                 2 | 1
                 3 | 1
                 4 | 1
                 5 | 1
                 6 | 1
                 7 | 1
                 9 | 1""");
    }

    @Test
    public void issue2391() {
        var ccs = this.getCCS("""
                CREATE TABLE double_tbl(c1 DOUBLE, c2 DOUBLE NOT NULL);
                CREATE VIEW v AS SELECT
                CAST((c1) AS VARCHAR) AS c1,
                CAST((c2) AS VARCHAR) AS c2
                FROM double_tbl;""");
        ccs.step("""
                INSERT INTO double_tbl values
                   (-34567891.312, 98765432.12),
                   (8765432.147, -2344579.923);""", """
                 c1            |            c2 | weight
                ----------------------------------------
                 -34567891.312| 98765432.12| 1
                 8765432.147| -2344579.923| 1""");
    }

    @Test
    public void issue3093() {
        this.compileRustTestCase("""
                CREATE TABLE t1(c0 REAL);
                CREATE VIEW v0 AS
                SELECT t1.c0 + 0.42331, t1.c0 * 0.42331, .42331 * t1.c0 FROM t1;""");
    }

    @Test
    public void issue3094() {
        this.compileRustTestCase("""
                CREATE TABLE t1(c0 REAL, c1 VARCHAR, c2 CHAR);
                CREATE VIEW v2_optimized AS SELECT MIN(t1.c1) FROM t1 WHERE (1 IS NOT DISTINCT FROM 2);""");
    }

    @Test
    public void issue3547() {
        this.queryFailingInCompilation(
                "SELECT * FROM UNNEST(ARRAY [1, 2, 3, 4, 5], ARRAY[3, 2, 1]);",
                "UNNEST with multiple vectors");
    }

    @Test
    public void issue2736() {
        var ccs = this.getCCS("""
                create table latest_cells(
                  id integer,
                  mentioned_cell_ids integer array
                );
                
                create local view l as
                select
                    s.id,
                    m.mentioned_id
                from
                    latest_cells s
                left join unnest(s.mentioned_cell_ids) as m(mentioned_id) on true;
                
                create local view l1 as
                select
                    s.id,
                    m.mentioned_id
                from
                    latest_cells s
                join unnest(s.mentioned_cell_ids) as m(mentioned_id) on true;
                
                create view e as (SELECT * FROM l) EXCEPT (SELECT * FROM l1);""");
        ccs.step("INSERT INTO latest_cells VALUES(1, ARRAY[1,2,3]), (2, ARRAY[2,3,4]);",
                """
                  id | mentioned_id
                 -------------------""");
    }

    @Test
    public void projectionBug() {
        String sql = """
                CREATE TABLE flows(ts TIMESTAMP NOT NULL LATENESS INTERVAL 5 SECONDS)
                WITH ('append_only' = 'true');
                
                CREATE LOCAL VIEW tumble AS SELECT
                            MIN(ts) AS ts_min,
                            MAX(ts) AS ts_max,
                            window_start,
                            window_end
                    FROM TABLE(
                        TUMBLE(
                            TABLE flows,
                            DESCRIPTOR(ts),
                            INTERVAL '1' MINUTES
                        )
                    )
                    GROUP BY window_start, window_end;

                CREATE VIEW final
                    WITH ('emit_final' = 'window_end')
                    AS SELECT window_start, window_end
                    FROM tumble;""";
        this.getCCS(sql);
    }

    @Test
    public void testChain() {
        var ccs = this.getCCS("""
                CREATE TABLE T(x INT);
                CREATE LOCAL VIEW V0 AS SELECT * FROM T WHERE x < 10;
                CREATE LOCAL VIEW V1 AS SELECT x + 1 AS x FROM V0;
                CREATE LOCAL VIEW V2 AS SELECT * FROM V1 WHERE x > 0;
                CREATE VIEW V3 AS SELECT x / 2 AS x FROM V2;
                """);
        // Map and Filter operators should have been collapsed
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            @Override
            public void postorder(DBSPMapOperator operator) {
                Assert.fail();
            }

            @Override
            public void postorder(DBSPFilterOperator operator) {
                Assert.fail();
            }
        };
        ccs.visit(visitor);
    }

    @Test
    public void issue3278() {
        this.compileRustTestCase("""
                CREATE TABLE t1(c0 varchar);
                CREATE TABLE t2(c0 varchar);
                CREATE VIEW v2 AS (SELECT 1 FROM t1 JOIN t2 ON (1 IS NOT DISTINCT FROM NULL));""");
    }

    @Test
    public void testGroupbyOrdinal() {
        this.getCCS("""
                CREATE TABLE X (xx int);
                CREATE VIEW Y
                AS select xx from X
                group by 1""");
    }

    @Test
    public void testPullUp() {
        var ccs = this.getCCS("""
                   CREATE TABLE X(x INT);
                   CREATE TABLE Y(x INT, y INT);
                   CREATE VIEW V AS SELECT X.x, Y.y FROM X JOIN Y ON X.x = Y.x WHERE Y.y = 23;""");
        var circuit = ccs.getCircuit();
        IInputOperator y = circuit.getInput(new ProgramIdentifier("Y", false));
        // Check that 23 is pulled right after the Y input table, before the join.
        for (DBSPOperator op: circuit.allOperators) {
            if (!op.inputs.isEmpty() && op.inputs.get(0).node() == y) {
                Assert.assertTrue(op.is(DBSPFlatMapIndexOperator.class));
                var clo = op.to(DBSPFlatMapIndexOperator.class).getClosureFunction();
                final boolean[] found = {false};
                InnerVisitor visitor = new InnerVisitor(ccs.compiler) {
                    public void postorder(DBSPI32Literal lit) {
                        found[0] = true;
                        Assert.assertEquals((Integer)23, lit.value);
                    }
                };
                clo.accept(visitor);
                Assert.assertTrue(found[0]);
            }
        }
    }

    @Test
    public void testUuid() {
        this.statementsFailingInCompilation("""
                        DECLARE RECURSIVE VIEW V(u UUID);
                        CREATE VIEW V AS SELECT 1 AS u;""",
                "does not match the declared type v(u UUID)");
    }

    @Test
    public void issue3744() {
        this.getCCS("""
                CREATE TABLE row_of_map_tbl(
                  c1 ROW(m1_int MAP<VARCHAR, INT> NOT NULL));
                
                CREATE MATERIALIZED VIEW v AS SELECT
                c1[1]['1'] AS c1_one
                FROM row_of_map_tbl;""");
    }

    @Test
    public void cseTest() {
        this.getCCS("""
                CREATE TABLE binary_tbl(
                id INT,
                c1 BINARY(4),
                c2 BINARY(4) NULL);
                
                CREATE VIEW binary_array_where AS SELECT
                ARRAY_AGG(c1) FILTER(WHERE c1 < c2) AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c1 < c2) AS f_c2
                FROM binary_tbl;""");
    }

    @Test
    public void issue3777() {
        this.statementsFailingInCompilation(
                "CREATE VIEW baz AS select DATEADD(DAY, -360, DATE '2020-01-01')",
                """
                Not yet implemented: Function 'DATEADD' not yet implemented
                This is tracked by issue https://github.com/feldera/feldera/issues/1265
                Perhaps you can use DATE_ADD or addition between a date and an interval?
                    1|CREATE VIEW baz AS select DATEADD(DAY, -360, DATE '2020-01-01')
                                                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^""");
    }

    @Test
    public void issue3769() {
        this.getCCS("""
                CREATE TABLE telemetry_pipeline_statistics (
                    account_id UUID NOT NULL,
                    pipeline_id_hash BYTEA NOT NULL,
                    event_timestamp TIMESTAMP NOT NULL,
                    pipeline_started_at TIMESTAMP NOT NULL,
                    runtime_elapsed_msecs INTEGER NOT NULL
                );
                
                CREATE VIEW pipeline_runtime_summary AS
                WITH ranked_events AS (
                    SELECT
                        account_id,
                        pipeline_id_hash,
                        pipeline_started_at,
                        runtime_elapsed_msecs,
                        event_timestamp,
                        ROW_NUMBER() OVER (
                            PARTITION BY account_id, pipeline_id_hash, pipeline_started_at\s
                            ORDER BY event_timestamp DESC
                        ) AS event_rank
                    FROM telemetry_pipeline_statistics
                )
                SELECT
                    account_id,
                    pipeline_id_hash,
                    SUM(runtime_elapsed_msecs) AS total_runtime_msecs,
                    COUNT(*) AS number_of_pipeline_runs
                FROM ranked_events
                WHERE event_rank = 1
                GROUP BY account_id, pipeline_id_hash;
                
                CREATE VIEW pipeline_runtime_by_day AS
                WITH ranked_events AS (
                    SELECT
                        account_id,
                        pipeline_id_hash,
                        pipeline_started_at,
                        event_timestamp,
                        DATE_TRUNC(event_timestamp, day) AS event_day,
                        runtime_elapsed_msecs,
                        ROW_NUMBER() OVER (
                            PARTITION BY account_id, pipeline_id_hash, pipeline_started_at\s
                            ORDER BY event_timestamp DESC
                        ) AS event_rank,
                        runtime_elapsed_msecs - COALESCE(
                            LAG(runtime_elapsed_msecs) OVER (
                                PARTITION BY account_id, pipeline_id_hash, pipeline_started_at\s
                                ORDER BY event_timestamp
                            ),
                            0
                        ) AS daily_runtime_increment
                    FROM telemetry_pipeline_statistics
                ),
                daily_increments AS (
                    SELECT
                        account_id,
                        pipeline_id_hash,
                        event_day,
                        daily_runtime_increment
                    FROM ranked_events
                    WHERE event_rank = 1
                )
                SELECT
                    event_day,
                    SUM(daily_runtime_increment) AS daily_runtime_msecs
                FROM daily_increments
                GROUP BY event_day
                ORDER BY event_day;""");
    }

    @Test
    public void issue3770() {
        this.statementsFailingInCompilation("""
                CREATE TABLE row_of_map_tbl(
                c1 ROW(m1_int MAP<VARCHAR, INT> NOT NULL, m2_int MAP<VARCHAR, INT> NULL));
                
                CREATE MATERIALIZED VIEW row_of_map_idx_outbound AS SELECT
                c1[3] AS c13_val
                FROM row_of_map_tbl;""", "ROW type does not have a field with index 3; legal range is 1 to 2");
    }

    @Test
    public void testAggregateLimit() {
        var cc = this.getCC("""
               CREATE TABLE T(id INT);
               CREATE TABLE S(s INT, id INT);
               CREATE VIEW V0 AS SELECT coalesce(
                      (select sum(s) from S where S.id = T.id limit 1), 0) FROM T;""");
        CircuitVisitor visitor = new CircuitVisitor(cc.compiler) {
            @Override
            public void postorder(DBSPIndexedTopKOperator node) {
                Assert.fail("Should not have TopK operators");
            }
        };
        cc.visit(visitor);
    }

    @Test
    public void issue3778() {
        this.statementsFailingInCompilation("""
                CREATE TABLE foo (
                    bar NUMERIC,
                    baz STRING
                );
                
                CREATE VIEW biz AS (
                    SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY bar) AS bar_median
                    FROM foo
                    GROUP BY baz
                );""", "Aggregate function not yet implemented");
    }

    @Test
    public void issue3059() {
        this.statementsFailingInCompilation("CREATE FUNCTION F(x INT) RETURNS BIGINT AS YEAR(NOW()) + x;",
                "Non-deterministic UDF");
    }

    @Test
    public void issue4562() {
        this.q("""
                SELECT POSITION('い' in 'かわいい');
                 pos
                -----
                 3""");
    }

    @Test
    public void issue4937() {
        this.qf("SELECT PARSE_TIMESTAMP('%Y-%m-%d', '2020-01-01')",
                "Invalid format in PARSE_TIMESTAMP: '%Y-%m-%d'");
    }
}
