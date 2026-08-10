package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CteToLocalViews;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ParsedStatement;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.SqlToRelCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

/** Tests for the conversion of common table expressions into local views. */
public class CteToLocalViewsTests extends SqlIoTest {
    /** Compile with the "CTE to local view" rewrite forced. */
    CompilerCircuitStream ccsNoCte(String sql) {
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.cteViews = true;
        compiler.submitStatementsForCompilation(sql);
        return this.getCCS(compiler);
    }

    @Test
    public void testRewriteShape() throws SqlParseException {
        DBSPCompiler compiler = this.testCompiler();
        SqlToRelCompiler sqlToRel = compiler.sqlToRelCompiler;
        List<ParsedStatement> statements = sqlToRel.parseStatements("""
                CREATE TABLE data(x INT);
                CREATE VIEW v AS
                WITH data AS (SELECT x + 1 AS x FROM data),
                     doubled(y) AS (SELECT x * 2 FROM data)
                SELECT data.x, d.y FROM data, doubled AS d;""");
        Assert.assertEquals(2, statements.size());
        sqlToRel.compile(statements.get(0), compiler.sources);

        List<ParsedStatement> parts = sqlToRel.hoistCtes(statements.get(1));
        Assert.assertNotNull(parts);
        Assert.assertEquals(3, parts.size());

        // The CTE shadows table 'data' only for the rest of the query;
        // the CTE's own body still reads the real table.
        Assert.assertEquals("""
                CREATE LOCAL VIEW v-cte-data AS
                SELECT (data.x + 1) AS x
                FROM schema.data AS data""",
                CalciteObject.create(parts.get(0)).toString());
        // The second CTE reads the first one; its column list carries over.
        Assert.assertEquals("""
                CREATE LOCAL VIEW v-cte-doubled (y) AS
                SELECT (data.x * 2)
                FROM v-cte-data AS data""",
                CalciteObject.create(parts.get(1)).toString());
        // The CTE names survive as aliases, so column references still resolve.
        Assert.assertEquals("""
                CREATE VIEW v AS
                SELECT data.x, d.y
                FROM v-cte-data AS data,
                v-cte-doubled AS d""",
                CalciteObject.create(parts.get(2)).toString());
    }

    @Test
    public void testChainedCtesShadowingTable() {
        // CTE 'data' shadows the table 'data'; 'doubled' reads the CTE.
        // Expected output validated on Postgres.
        var ccs = this.ccsNoCte("""
                CREATE TABLE data(x INT);
                CREATE VIEW v AS
                WITH data AS (SELECT x + 1 AS x FROM data),
                     doubled(y) AS (SELECT x * 2 FROM data)
                SELECT data.x, d.y FROM data JOIN doubled AS d ON d.y = 2 * data.x;""");
        ccs.stepWeightOne("INSERT INTO data VALUES(1), (2);", """
                 x | y
                --------
                 2 | 4
                 3 | 6""");
    }

    @Test
    public void testNestedWith() {
        // The nested WITH stays inline; its body references the hoisted CTE.
        // Expected output validated on Postgres.
        var ccs = this.ccsNoCte("""
                CREATE TABLE t(x INT);
                CREATE VIEW v AS
                WITH a AS (SELECT x FROM t)
                SELECT * FROM (WITH b AS (SELECT x + 1 AS x FROM a) SELECT * FROM b) AS sub;""");
        ccs.stepWeightOne("INSERT INTO t VALUES(1), (2);", """
                 x
                ---
                 2
                 3""");
    }

    @Test
    public void testCteUsedTwice() {
        // Expected output validated on Postgres.
        var ccs = this.ccsNoCte("""
                CREATE TABLE t(id INT, v INT);
                CREATE VIEW v AS
                WITH e AS (SELECT id, v FROM t)
                SELECT e.id, e.v, agg.s
                FROM e JOIN (SELECT id, SUM(v) AS s FROM e GROUP BY id) AS agg
                ON e.id = agg.id;""");
        ccs.stepWeightOne("INSERT INTO t VALUES(1, 10), (1, 20), (2, 30);", """
                 id | v  | s
                --------------
                 1  | 10 | 30
                 1  | 20 | 30
                 2  | 30 | 30""");
    }

    @Test
    public void testAutomaticFallback() {
        // A correlated subquery whose body contains an UNNEST cannot be
        // decorrelated with the CTE inlined; the compiler will retry
        // with the CTE as a local view.
        // Expected output validated on Postgres.
        var ccs = this.getCCS("""
                CREATE TABLE t(id INT, arr INT ARRAY);
                CREATE VIEW v AS
                WITH e AS (SELECT t.id, u.v FROM t, UNNEST(t.arr) AS u(v))
                SELECT e.id, e.v,
                       (SELECT COUNT(*) FROM e AS e2 WHERE e2.id = e.id) AS cnt
                FROM e;""");
        ccs.stepWeightOne("INSERT INTO t VALUES(1, ARRAY[10, 20]), (2, ARRAY[30]);", """
                 id | v  | cnt
                ---------------
                 1  | 10 | 2
                 1  | 20 | 2
                 2  | 30 | 1""");
    }

    @Test
    public void testRetryRequested() throws SqlParseException {
        // The query of testAutomaticFallback cannot be compiled with the
        // CTE inlined: compileCreateView must request the CTE rewrite.
        DBSPCompiler compiler = this.testCompiler();
        SqlToRelCompiler sqlToRel = compiler.sqlToRelCompiler;
        List<ParsedStatement> statements = sqlToRel.parseStatements("""
                CREATE TABLE t(id INT, arr INT ARRAY);
                CREATE VIEW v AS
                WITH e AS (SELECT t.id, u.v FROM t, UNNEST(t.arr) AS u(v))
                SELECT e.id, e.v,
                       (SELECT COUNT(*) FROM e AS e2 WHERE e2.id = e.id) AS cnt
                FROM e;""");
        sqlToRel.compile(statements.get(0), compiler.sources);
        try {
            sqlToRel.compileCreateView(statements.get(1), new HashMap<>(), compiler.sources, true);
            Assert.fail("Expected a CTE rewrite request");
        } catch (CteToLocalViews.Retry ignored) {}
    }

    @Test
    public void testRecursiveCteRejected() {
        // Recursion must be expressed with DECLARE RECURSIVE VIEW.
        this.statementsFailingInCompilation("""
                CREATE TABLE t(x INT);
                CREATE VIEW v AS
                WITH RECURSIVE r(x) AS (SELECT x FROM t UNION ALL SELECT x + 1 FROM r WHERE x < 10)
                SELECT * FROM r;""",
                "use DECLARE RECURSIVE VIEW instead");
    }
}
