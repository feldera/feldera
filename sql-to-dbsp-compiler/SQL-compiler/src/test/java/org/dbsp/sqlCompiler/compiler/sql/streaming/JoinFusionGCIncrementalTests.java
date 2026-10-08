package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.sql.streaming.JoinGCIncrementalTests.LateInputs;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuit;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.CountGCOperators;
import org.dbsp.sqlCompiler.compiler.sql.tools.DifferentialTester;
import org.dbsp.sqlCompiler.compiler.sql.tools.TablePair;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** GC of a join that the compiler fuses with the operator that follows it: with a filter into a
 * join filter-map, or with the index of an aggregate into a join index.  Each fused join has the GC
 * of the same join in {@link JoinGCIncrementalTests}, whose inputs and steps these tests use. */
public class JoinFusionGCIncrementalTests extends StreamingTestBase {
    /** A join of inputs L and R on t, followed by an operator that the compiler fuses with it.
     * @param name   Name of the shape.
     * @param join   Kind of the fused join, as {@link CountGCOperators#joins} names it.
     * @param query  The query; {@code L} and {@code R} are replaced by the names of the inputs.  Its
     *               columns are those of {@link JoinGCIncrementalTests#COLUMNS} after j. */
    record FusedShape(String name, String join, String query) {
        String over(String left, String right) {
            return this.query
                    .replaceAll("\\bL l\\b", left + " l")
                    .replaceAll("\\bR r\\b", right + " r");
        }
    }

    /** The columns of a join, and of an aggregate over a join grouped by l.t. */
    static final String JOIN_COLUMNS = "SELECT l.k AS lk, l.t AS lt, l.v AS lv, r.k AS rk, r.t AS rt, r.v AS rv ";
    static final String GROUP_COLUMNS = "SELECT CAST(NULL AS INT) AS lk, l.t AS lt, COUNT(*) AS lv, " +
            "CAST(NULL AS INT) AS rk, CAST(NULL AS INT) AS rt, SUM(r.v) AS rv ";

    /** A filter on the columns of both inputs of an inner join becomes the residual condition of
     * the join; a filter on the right columns of a left join stays after it. */
    static final List<FusedShape> SHAPES = List.of(
            new FusedShape("inner_where", "JoinFilterMap",
                    JOIN_COLUMNS + "FROM L l JOIN R r ON l.t = r.t WHERE l.v + r.v > 12"),
            new FusedShape("inner_group", "JoinIndex",
                    GROUP_COLUMNS + "FROM L l JOIN R r ON l.t = r.t GROUP BY l.t"),
            new FusedShape("left_where", "LeftJoinFilterMap",
                    JOIN_COLUMNS + "FROM L l LEFT JOIN R r ON l.t = r.t WHERE r.v IS NULL OR r.v > 12"),
            new FusedShape("left_group", "LeftJoinIndex",
                    GROUP_COLUMNS + "FROM L l LEFT JOIN R r ON l.t = r.t GROUP BY l.t"));

    /** The GC operators of each shape, for each choice of the inputs with LATENESS: K is a
     * RetainKeys operator.  The joins retain their inputs as inner_t and left_t of
     * {@link JoinGCIncrementalTests} do, and the aggregates add a RetainKeys where the join's
     * output l.t has a waterline: only when both inputs have LATENESS. */
    static final String EXPECTED_GC = """
            shape            | L        | R        | LR       | LR_LARGE | LARGE_LR
            inner_where      | K        | K        | KK       | KK       | KK
            inner_group      | K        | K        | KKK      | KKK      | KKK
            left_where       | -        | K        | KK       | KK       | KK
            left_group       | -        | K        | KKK      | KKK      | KKK
            """;

    /** Each shape is fused, and has GC exactly where the waterlines allow it. */
    @Test
    public void gcOperators() {
        StringBuilder header = new StringBuilder(String.format("%-16s", "shape"));
        for (LateInputs lateInputs : LateInputs.values())
            header.append(String.format(" | %-8s", lateInputs));
        StringBuilder actual = new StringBuilder(header.toString().stripTrailing()).append("\n");
        for (FusedShape shape : SHAPES) {
            StringBuilder line = new StringBuilder(String.format("%-16s", shape.name));
            for (LateInputs lateInputs : LateInputs.values()) {
                String sql = JoinGCIncrementalTests.createInput("L", lateInputs.left) +
                        JoinGCIncrementalTests.createInput("R", lateInputs.right) +
                        "CREATE VIEW V AS " + shape.over("L", "R") + ";";
                CompilerCircuit cc = this.getCC(sql);
                CountGCOperators operators = new CountGCOperators(cc.compiler);
                cc.visit(operators);
                Assert.assertEquals(shape.name + " " + lateInputs, List.of(shape.join), operators.joins);
                line.append(String.format(" | %-8s", operators.kinds()));
            }
            // A text block has no trailing spaces
            actual.append(line.toString().stripTrailing()).append("\n");
        }
        Assert.assertEquals(EXPECTED_GC, actual.toString());
    }

    /* The generated program for the shape where starts with
     * CREATE TABLE LATE_LEFT_L (k INT NOT NULL, t INT LATENESS 10, v INT NOT NULL);
     * ...
     * CREATE LOCAL VIEW LATE_ALL AS
     * SELECT 'where_L' AS j, CAST(lk AS VARCHAR) AS lk, ..., CAST(rv AS VARCHAR) AS rv FROM (
     *   SELECT l.k AS lk, ..., r.v AS rv FROM LATE_LEFT_L l LEFT JOIN LATE_RIGHT_L r ON l.t = r.t
     *   WHERE r.v IS NULL OR r.v > 12) v
     * UNION ALL ...;
     */
    static String differentialProgram(List<TablePair<?>> pairs, FusedShape shape) {
        StringBuilder sql = new StringBuilder();
        for (TablePair<?> pair : pairs) {
            sql.append(pair.create("LATE_", true));
            sql.append(pair.create("PLAIN_", false));
        }
        List<String> columns = JoinGCIncrementalTests.COLUMNS;
        for (String prefix : TablePair.PREFIXES) {
            List<String> branches = new ArrayList<>();
            for (LateInputs lateInputs : LateInputs.values()) {
                StringBuilder branch = new StringBuilder("SELECT '" + shape.name + "_" + lateInputs + "' AS j");
                for (String column : columns.subList(1, columns.size()))
                    branch.append(", CAST(").append(column).append(" AS VARCHAR) AS ").append(column);
                String query = shape.over(prefix + "LEFT_" + lateInputs, prefix + "RIGHT_" + lateInputs);
                branch.append(" FROM (").append(query).append(") v");
                branches.add(branch.toString());
            }
            sql.append("CREATE LOCAL VIEW ").append(prefix).append("ALL AS ")
                    .append(String.join("\nUNION ALL ", branches)).append(";\n");
        }
        sql.append(DifferentialTester.differenceView(columns));
        return sql.toString();
    }

    /** Each shape gives the same output with and without LATENESS. */
    @Test
    public void sameOutputWithoutLateness() {
        for (FusedShape shape : SHAPES) {
            List<TablePair<?>> pairs = JoinGCIncrementalTests.createTablePairs();
            CompilerCircuitStream ccs = this.getCCS(differentialProgram(pairs, shape));
            // Every branch of LATE_ALL and PLAIN_ALL has its own fused join
            CountGCOperators operators = new CountGCOperators(ccs.compiler);
            ccs.visit(operators);
            Assert.assertEquals(shape.name,
                    Collections.nCopies(2 * LateInputs.values().length, shape.join), operators.joins);
            var tester = new DifferentialTester(ccs, pairs, JoinGCIncrementalTests.COLUMNS);
            JoinGCIncrementalTests.steps(tester);
        }
    }
}
