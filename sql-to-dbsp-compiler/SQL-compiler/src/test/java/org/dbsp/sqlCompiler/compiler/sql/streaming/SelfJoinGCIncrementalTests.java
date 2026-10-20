package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuit;
import org.dbsp.sqlCompiler.compiler.sql.tools.CountGCOperators;
import org.dbsp.sqlCompiler.compiler.sql.tools.DifferentialTester;
import org.dbsp.sqlCompiler.compiler.sql.tools.TablePair;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** GC of self-joins: the two inputs of a self-join read one trace, with one merged RetainValues
 * operator that keeps a row when either input still needs it; when one input keeps every row,
 * the trace is not garbage collected. */
public class SelfJoinGCIncrementalTests extends StreamingTestBase {
    /** A row of the input: key k, time ts, and a value v that makes rows distinct. */
    record Row(int k, @Nullable Integer ts, int v) {}

    /** The LATENESS of column ts. */
    static final int LATENESS = 10;

    /** A self-join of table T; {@code T} is replaced by the name of the table.
     * @param name   Name of the view.
     * @param join   The join condition.
     * @param gc     True if both inputs have a bound, so that the join retains values; false if
     *               only the right input has one, so that the left input keeps every row and the
     *               shared trace is not garbage collected. */
    record Shape(String name, String join, boolean gc) {
        String view(String table) {
            return "SELECT a.k, a.ts AS a_ts, b.ts AS b_ts FROM " + table + " a JOIN " +
                    table + " b ON a.k = b.k AND " + this.join;
        }
    }

    /** The bounds of the two inputs differ in the first two shapes and agree in the third; in the
     * last shape only the right input has a bound. */
    static final List<Shape> SHAPES = List.of(
            new Shape("back", "b.ts BETWEEN a.ts - 5 AND a.ts", true),
            new Shape("ahead", "b.ts BETWEEN a.ts + 1 AND a.ts + 7", true),
            new Shape("around", "b.ts BETWEEN a.ts - 3 AND a.ts + 3", true),
            new Shape("open", "b.ts >= a.ts - 5", false));

    static final List<String> COLUMNS = List.of("j", "k", "a_ts", "b_ts");

    /** A program with the self-joins {@code shapes} over LATE_T and over PLAIN_T, and view D; column
     * j of the views names the shape of each row.  For the shape {@code back}:
     * <pre>
     * CREATE TABLE LATE_T (k INT NOT NULL, ts INT LATENESS 10, v INT NOT NULL);
     * CREATE TABLE PLAIN_T (k INT NOT NULL, ts INT, v INT NOT NULL);
     * CREATE LOCAL VIEW LATE_ALL AS SELECT 'back' AS j, CAST(k AS VARCHAR) AS k, CAST(a_ts AS VARCHAR) AS a_ts,
     *     CAST(b_ts AS VARCHAR) AS b_ts FROM (
     *     SELECT a.k, a.ts AS a_ts, b.ts AS b_ts FROM LATE_T a JOIN LATE_T b
     *     ON a.k = b.k AND b.ts BETWEEN a.ts - 5 AND a.ts) v;
     * CREATE LOCAL VIEW PLAIN_ALL AS ... the same over PLAIN_T ...;
     * CREATE VIEW D AS ... the symmetric difference of LATE_ALL and PLAIN_ALL, plus the error view ...;
     * </pre>
     * With several shapes, the branches of LATE_ALL and PLAIN_ALL are joined by UNION ALL. */
    static String program(TablePair<Row> pair, List<Shape> shapes) {
        StringBuilder sql = new StringBuilder();
        sql.append(pair.create("LATE_", true));
        sql.append(pair.create("PLAIN_", false));
        for (String prefix : TablePair.PREFIXES) {
            List<String> branches = new ArrayList<>();
            for (Shape shape : shapes) {
                branches.add("SELECT '" + shape.name + "' AS j, CAST(k AS VARCHAR) AS k, " +
                        "CAST(a_ts AS VARCHAR) AS a_ts, CAST(b_ts AS VARCHAR) AS b_ts FROM (" +
                        shape.view(prefix + "T") + ") v");
            }
            sql.append("CREATE LOCAL VIEW ").append(prefix).append("ALL AS ")
                    .append(String.join("\nUNION ALL ", branches)).append(";\n");
        }
        sql.append(DifferentialTester.differenceView(COLUMNS));
        return sql.toString();
    }

    /** The operator at the start of the chain of single-input operators that ends at {@code port}. */
    static DBSPOperator source(OutputPort port) {
        DBSPOperator node = port.node();
        while (node.is(DBSPUnaryOperator.class))
            node = node.to(DBSPUnaryOperator.class).input().node();
        return node;
    }

    /** Each self-join integrates one trace; the self-join of LATE_T has one RetainValues operator
     * unless one of its inputs keeps every row. */
    @Test
    public void oneTracePerSelfJoin() {
        TablePair<Row> pair = new TablePair<>("T", true, Row.class, Set.of("ts"), LATENESS);
        for (Shape shape : SHAPES) {
            CompilerCircuit cc = this.getCC(program(pair, List.of(shape)));
            var visitor = new CountGCOperators(cc.compiler) {
                int selfJoins = 0;

                @Override
                public void postorder(DBSPJoinFilterMapOperator operator) {
                    // A self-join: both inputs read the same table
                    Assert.assertEquals(shape.name, source(operator.left()), source(operator.right()));
                    // Both inputs read one port, so the join integrates one trace
                    Assert.assertEquals(shape.name, operator.left(), operator.right());
                    this.selfJoins++;
                }
            };
            cc.visit(visitor);
            Assert.assertEquals(shape.name, shape.gc ? "V" : "-", visitor.kinds());
            // One self-join of LATE_T and one of PLAIN_T
            Assert.assertEquals(shape.name, 2, visitor.selfJoins);
        }
    }

    /** The steps of the differential test of a program with the self-joins {@code shapes}. */
    void sameOutputWithoutLateness(List<Shape> shapes) {
        TablePair<Row> pair = new TablePair<>("T", true, Row.class, Set.of("ts"), LATENESS);
        var tester = new DifferentialTester(this.getCCS(program(pair, shapes)), List.of(pair), COLUMNS);
        Set<String> t = Set.of("T");

        tester.insert(new Row(0, 100, 1), new Row(0, 103, 2), new Row(0, 107, 3), new Row(1, 100, 4),
                new Row(1, 106, 5), new Row(0, null, 6), new Row(2, 101, 7));
        // The waterline moves to 140
        tester.insert(new Row(0, 150, 8));
        // Rows at and above the waterline, which join no row below it: ts < 140 - 7
        tester.insert(t, new Row(0, 140, 9), new Row(1, 141, 10));
        // Rows that join across the waterline
        tester.insert(t, new Row(0, 144, 11), new Row(1, 147, 12));
        // Late changes, which the tester skips
        tester.insert(new Row(0, 136, 13));
        tester.delete(new Row(0, 100, 1));
        // The waterline moves to 190
        tester.insert(new Row(1, 200, 14));
        tester.insert(t, new Row(0, 190, 15), new Row(0, 193, 16), new Row(1, 197, 17), new Row(2, 191, 18));
        tester.delete(t, new Row(0, 193, 16), new Row(1, 200, 14));
        tester.insert(t, new Row(0, 199, 19), new Row(2, 195, 20), new Row(1, null, 21));
        tester.delete(t, new Row(0, 150, 8), new Row(0, null, 6));
    }

    /** Check that each self-join gives the same output with and without LATENESS. */
    @Test
    public void sameOutputWithoutLateness() {
        for (Shape shape : SHAPES)
            this.sameOutputWithoutLateness(List.of(shape));
    }

    /** Check that two self-joins of one table give the same output with and without LATENESS. */
    @Test
    public void twoSelfJoinsSameOutputWithoutLateness() {
        this.sameOutputWithoutLateness(List.of(SHAPES.get(0), SHAPES.get(1)));
    }
}
