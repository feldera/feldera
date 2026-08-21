package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CanonicalForm;
import org.dbsp.sqlCompiler.compiler.visitors.outer.temporal.ReorderTemporalFilters;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.BiFunction;

/** Tests for {@link ReorderTemporalFilters}. */
public class ReorderTemporalFiltersTests extends SqlIoTest {
    DBSPExpression now() {
        return new DBSPApplyExpression("now", DBSPTypeTimestamp.INSTANCE);
    }

    /** The String representation of a closure's body in canonical form. */
    String print(DBSPCompiler compiler, DBSPClosureExpression closure) {
        return new CanonicalForm(compiler).apply(closure)
                .to(DBSPClosureExpression.class).body.toString();
    }

    record Rewrite(String before, String after) {}

    /** Applies a rewrite `policy` to the conjuncts of the condition `build` produces over a variable
     * with type Tup2(INT, TIMESTAMP).  Returns the condition before and after the policy application. */
    Rewrite reorder(ReorderTemporalFilters.FilterOrderPolicy policy,
                    BiFunction<ExpressionBuilder, DBSPVariablePath, DBSPExpression> build) {
        DBSPCompiler compiler = this.testCompiler();
        ExpressionBuilder b = new ExpressionBuilder();
        DBSPType rowType = b.tup(b.i32(), DBSPTypeTimestamp.INSTANCE);

        DBSPVariablePath original = b.refVar(rowType);
        String before = this.print(compiler, build.apply(b, original).closure(original));

        DBSPVariablePath row = b.refVar(rowType);
        DBSPExpression expression = build.apply(b, row);
        DBSPExpression reordered = ReorderTemporalFilters.reorder(
                expression, policy,conjunct -> conjunct.toString().contains("now()"));
        String after = this.print(compiler, reordered.closure(row));
        return new Rewrite(before, after);
    }

    /** @return (*v).0 > 2 AND (*v).1 > now() */
    DBSPExpression plainThenTemporal(ExpressionBuilder b, DBSPVariablePath v) {
        return b.binary(DBSPOpcode.AND,
                b.binary(DBSPOpcode.GT, b.field(v, 0), b.lit(2)),
                b.binary(DBSPOpcode.GT, b.field(v, 1), this.now()));
    }

    @Test
    public void policyMovesTheTemporalConjunct() {
        // Move temporal filter first
        Rewrite first = this.reorder(ReorderTemporalFilters.FilterOrderPolicy.FIRST, this::plainThenTemporal);
        Assert.assertEquals("((((*p0).0) > 2) && (((*p0).1) > now()))", first.before());
        Assert.assertEquals("((((*p0).1) > now()) && (((*p0).0) > 2))", first.after());

        // Move last: the temporal conjunct is already last
        Rewrite last = this.reorder(ReorderTemporalFilters.FilterOrderPolicy.LAST, this::plainThenTemporal);
        Assert.assertEquals("((((*p0).0) > 2) && (((*p0).1) > now()))", last.before());
        Assert.assertEquals(last.before(), last.after());

        // Does not modify predicate
        Rewrite keep = this.reorder(ReorderTemporalFilters.FilterOrderPolicy.KEEP, this::plainThenTemporal);
        Assert.assertEquals("((((*p0).0) > 2) && (((*p0).1) > now()))", keep.before());
        Assert.assertEquals(keep.before(), keep.after());
    }

    @Test
    public void lastMovesTheTemporalConjunctToTheEnd() {
        BiFunction<ExpressionBuilder, DBSPVariablePath, DBSPExpression> builder = (b, row) ->
                b.binary(DBSPOpcode.AND,
                        b.binary(DBSPOpcode.GT, b.field(row, 1), this.now()),
                        b.binary(DBSPOpcode.GT, b.field(row, 0), b.lit(2)));
        Rewrite last = this.reorder(ReorderTemporalFilters.FilterOrderPolicy.LAST, builder);
        Assert.assertEquals("((((*p0).1) > now()) && (((*p0).0) > 2))", last.before());
        Assert.assertEquals("((((*p0).0) > 2) && (((*p0).1) > now()))", last.after());
    }

    @Test
    public void relativeOrderIsPreserved() {
        BiFunction<ExpressionBuilder, DBSPVariablePath, DBSPExpression> builder =
                (b, row) ->
                        b.binary(DBSPOpcode.AND,
                                b.binary(DBSPOpcode.AND,
                                        b.binary(DBSPOpcode.AND,
                                                b.binary(DBSPOpcode.GT, b.field(row, 0), b.lit(2)),
                                                b.binary(DBSPOpcode.GT, b.field(row, 1), this.now())),
                                        b.binary(DBSPOpcode.LT, b.field(row, 1), this.now())),
                                b.binary(DBSPOpcode.LT, b.field(row, 0), b.lit(10)));
        Rewrite first = this.reorder(ReorderTemporalFilters.FilterOrderPolicy.FIRST, builder);
        // The order of temporal filters and non-temporal filters with respect to each other is preserved
        Assert.assertEquals(
                "((((((*p0).0) > 2) && (((*p0).1) > now())) && (((*p0).1) < now())) && (((*p0).0) < 10))",
                first.before());
        Assert.assertEquals(
                "(((((*p0).1) > now()) && (((*p0).1) < now())) && ((((*p0).0) > 2) && (((*p0).0) < 10)))",
                first.after());
    }

    /** Two views comparing the same column against NOW(), with the temporal conjunct
     * written after a non-temporal one. */
    static String twoViews(int threshold) {
        return "SET feldera_window_sharing_threshold = " + threshold + ";\n" + """
                CREATE TABLE T(a INT, b INT, ts TIMESTAMP);
                CREATE VIEW V1 AS SELECT b FROM T WHERE a > 2 AND ts >= NOW() - INTERVAL 100 YEARS;
                CREATE VIEW V2 AS SELECT b FROM T WHERE a < 10 AND ts >= NOW() - INTERVAL 50 YEARS;""";
    }

    @Test
    public void thresholdDecidesWhetherWindowsShare() {
        // 2 is under the threshold: do not share
        WindowInputStats alone = WindowInputStats.windows(this.getCC(twoViews(2)));
        Assert.assertEquals(2, alone.leftInputIds.size());
        Assert.assertEquals(2, alone.distinctInputCount());

        // 2 is over the threshold: share
        WindowInputStats shared = WindowInputStats.windows(this.getCC(twoViews(1)));
        Assert.assertEquals(2, shared.leftInputIds.size());
        Assert.assertEquals(1, shared.distinctInputCount());
    }

    @Test
    public void resultsSurviveTheMove() {
        String sql = """
                SET feldera_window_sharing_threshold = 1;
                CREATE TABLE T(a INT, b INT, ts TIMESTAMP);
                CREATE LOCAL VIEW V1 AS SELECT b FROM T
                WHERE a > 2 AND ts BETWEEN NOW() - INTERVAL 1 MONTHS AND NOW();
                CREATE LOCAL VIEW V2 AS SELECT b FROM T
                WHERE a < 10 AND ts >= NOW() - INTERVAL 2 MONTHS;
                CREATE VIEW V AS
                SELECT b, 1 AS q FROM V1
                UNION ALL
                SELECT b, 2 AS q FROM V2;""";
        var ccs = this.getCCS(sql);
        // Every row is inside both windows, so each query is decided by its other filter alone.
        ccs.stepWeightOne("""
                        INSERT INTO T VALUES (1, 10, '2024-12-01 00:00:00'),
                                             (5, 20, '2024-12-02 00:00:00'),
                                             (20, 30, '2024-12-03 00:00:00');
                        INSERT INTO now VALUES ('2024-12-12 00:00:00');""",
                """
                          b | q
                        --------
                         20 | 1
                         30 | 1
                         10 | 2
                         20 | 2""");
    }
}
