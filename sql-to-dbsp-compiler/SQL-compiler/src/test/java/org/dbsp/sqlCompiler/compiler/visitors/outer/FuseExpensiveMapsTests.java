package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TableData;
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLazyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link FuseExpensiveMaps} */
public class FuseExpensiveMapsTests extends SqlIoTest {
    /** Counts the calls to a function in the whole circuit. */
    static class CountCalls extends InnerVisitor {
        final String namePrefix;
        int count = 0;

        public CountCalls(DBSPCompiler compiler, String namePrefix) {
            super(compiler);
            this.namePrefix = namePrefix;
        }

        @Override
        public VisitDecision preorder(DBSPType type) {
            return VisitDecision.STOP;
        }

        @Override
        public void postorder(DBSPApplyExpression node) {
            String name = node.getFunctionName();
            if (name != null && name.startsWith(this.namePrefix))
                this.count++;
        }
    }

    static final String TABLE_AND_FUNCTION = """
            CREATE TABLE data (
              id VARCHAR NOT NULL,
              sid VARCHAR NOT NULL
            );
            CREATE FUNCTION expensive(x VARCHAR NOT NULL) RETURNS VARCHAR NOT NULL AS
              UPPER(x) || '!';
            """;

    /** Two views over the same table calling the same expensive function */
    @Test
    public void testCrossViewFusion() {
        String sql = TABLE_AND_FUNCTION + """
                CREATE VIEW V1 AS SELECT id, expensive(sid) AS e FROM data;
                CREATE VIEW V2 AS SELECT expensive(sid) AS e, sid FROM data;""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(sql);
        var ccs = this.getCCS(compiler);

        CountCalls counter = new CountCalls(compiler, "expensive");
        ccs.visit(counter.getCircuitVisitor(false));
        Assert.assertEquals(1, counter.count);

        Change input = ccs.toChange("INSERT INTO data VALUES('a', 'x');");
        Change output = new Change(
                new TableData("V1", new DBSPZSetExpression(new DBSPTupleExpression(
                        new DBSPStringLiteral("a"), new DBSPStringLiteral("X!")))),
                new TableData("V2", new DBSPZSetExpression(new DBSPTupleExpression(
                        new DBSPStringLiteral("X!"), new DBSPStringLiteral("x")))));
        ccs.addPair(input, output);
    }

    /** expensive is top-level and nested in the two calls */
    @Test
    public void testUnionSharing() {
        String sql = TABLE_AND_FUNCTION + """
                CREATE VIEW both AS
                SELECT id, expensive(sid) AS e FROM data
                UNION ALL
                SELECT sid, expensive(sid) || '?' AS e FROM data;""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(sql);
        var ccs = this.getCCS(compiler).withStringTrim();

        CountCalls counter = new CountCalls(compiler, "expensive");
        ccs.visit(counter.getCircuitVisitor(false));
        Assert.assertEquals(1, counter.count);

        ccs.stepWeightOne("INSERT INTO data VALUES('a', 'x');",
                """
                         id | e
                        ---------
                         a | X!
                         x | X!?""");
    }

    /** Counts {@link DBSPLazyExpression}s in the circuit.  InnerCSE creates one
     * for an expensive expression that a single function computes twice, so a
     * fused circuit must have none. */
    static class CountLazy extends InnerVisitor {
        int count = 0;

        public CountLazy(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public VisitDecision preorder(DBSPType type) {
            return VisitDecision.STOP;
        }

        @Override
        public void postorder(DBSPLazyExpression node) {
            this.count++;
        }
    }

    @Test
    public void testWithinSingleMap() {
        String sql = TABLE_AND_FUNCTION + """
                CREATE VIEW dup AS
                SELECT id, expensive(sid) AS a, expensive(sid) AS b FROM data;""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(sql);
        var ccs = this.getCCS(compiler).withStringTrim();

        CountCalls counter = new CountCalls(compiler, "expensive");
        ccs.visit(counter.getCircuitVisitor(false));
        Assert.assertEquals(1, counter.count);

        // If fusion worked there is no CSE-ed call
        CountLazy lazy = new CountLazy(compiler);
        ccs.visit(lazy.getCircuitVisitor(false));
        Assert.assertEquals(0, lazy.count);

        ccs.stepWeightOne("INSERT INTO data VALUES('a', 'x');", """
                         id | a | b
                        ------------
                         a | X! | X!""");
    }

    /** Two views projecting expensive computations of the same VARIANT column. */
    @Test
    public void testVariantSharing() {
        String sql = """
                CREATE TABLE data (
                  id VARCHAR NOT NULL PRIMARY KEY,
                  properties VARIANT,
                  sid VARCHAR
                );
                CREATE FUNCTION to_ts(d VARCHAR) RETURNS TIMESTAMP AS
                  PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', d);
                CREATE VIEW created AS
                SELECT id, TO_TS(SAFE_CAST(properties['created'] AS VARCHAR)) AS c
                FROM data;
                CREATE VIEW created2 AS
                SELECT TO_TS(SAFE_CAST(properties['created'] AS VARCHAR)) AS c, sid
                FROM data;""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(sql);
        var ccs = this.getCCS(compiler);

        CountCalls counter = new CountCalls(compiler, "to_ts");
        ccs.visit(counter.getCircuitVisitor(false));
        Assert.assertEquals(1, counter.count);
    }
}
