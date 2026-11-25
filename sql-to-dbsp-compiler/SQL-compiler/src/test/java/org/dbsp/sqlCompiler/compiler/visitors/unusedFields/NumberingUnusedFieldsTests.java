package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.circuit.operator.DBSPRankOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPRowNumberOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/** Tests for removing unused fields in {@link DBSPRowNumberOperator} and {@link DBSPRankOperator}.
 * Each window is in a local view, so Calcite cannot remove the columns that the readers of the
 * view do not use. */
public class NumberingUnusedFieldsTests extends SqlIoTest {
    /** Field count of the value part of the input and of the output of a RowNumber or Rank */
    record Shape(int inputFields, int outputFields) {}

    static class NumberingShapes extends CircuitVisitor {
        final List<Shape> shapes = new ArrayList<>();

        NumberingShapes(DBSPCompiler compiler) {
            super(compiler);
        }

        void add(DBSPUnaryOperator operator) {
            int input = operator.input().getOutputIndexedZSetType().getElementTypeTuple().size();
            int output = operator.getOutputIndexedZSetType().getElementTypeTuple().size();
            this.shapes.add(new Shape(input, output));
        }

        @Override
        public void postorder(DBSPRowNumberOperator operator) {
            this.add(operator);
        }

        @Override
        public void postorder(DBSPRankOperator operator) {
            this.add(operator);
        }
    }

    static final String TABLE = """
            CREATE TABLE T(id INT NOT NULL, g INT, ts INT, a VARCHAR, b INT);
            """;

    /** The shape of the only RowNumber or Rank in the circuit */
    static Shape shape(CompilerCircuitStream ccs) {
        NumberingShapes shapes = new NumberingShapes(ccs.compiler);
        ccs.visit(shapes);
        Assert.assertEquals(1, shapes.shapes.size());
        return shapes.shapes.get(0);
    }

    /** Two readers of the view use different columns. */
    @Test
    public void testTwoReaders() {
        var ccs = this.getCCS(TABLE + """
                CREATE LOCAL VIEW W AS
                SELECT *, ROW_NUMBER() OVER (PARTITION BY g ORDER BY ts) AS rn FROM T;
                CREATE VIEW V AS
                SELECT g AS x, rn FROM W UNION ALL SELECT id, rn FROM W;""");
        // The input keeps id, g, ts; the output keeps id, g, rn
        Assert.assertEquals(new Shape(3, 3), shape(ccs));
        // Expected output validated with Postgres 14.
        ccs.stepWeightOne("INSERT INTO T VALUES (1, 10, 5, 'x', 0), (2, 10, 7, 'y', 0), (3, 20, 1, 'z', 0);",
                """
                 x  | rn
                ---------
                 1  | 1
                 2  | 2
                 3  | 1
                 10 | 1
                 10 | 2
                 20 | 1""");
    }

    /** Rows 1 and 2 differ only in the columns that the input drops,
     * so they get consecutive row numbers. */
    @Test
    public void testRowNumberTies() {
        var ccs = this.getCCS(TABLE + """
                CREATE LOCAL VIEW W AS
                SELECT *, ROW_NUMBER() OVER (PARTITION BY g ORDER BY ts) AS rn FROM T;
                CREATE VIEW V AS SELECT g, rn FROM W;""");
        Assert.assertEquals(new Shape(2, 2), shape(ccs));
        // Expected output validated with Postgres 14.
        ccs.stepWeightOne("""
                INSERT INTO T VALUES (1, 10, 5, 'x', 0), (2, 10, 5, 'y', 1), (3, 10, 7, 'z', 0), (4, 20, 1, 'w', 0);""",
                """
                 g  | rn
                ---------
                 10 | 1
                 10 | 2
                 10 | 3
                 20 | 1""");
    }

    /** Rows 1 and 2 differ only in the columns that the input drops, so they get the same rank. */
    @Test
    public void testRankTies() {
        var ccs = this.getCCS(TABLE + """
                CREATE LOCAL VIEW W AS
                SELECT *, RANK() OVER (PARTITION BY g ORDER BY ts) AS r FROM T;
                CREATE VIEW V AS SELECT g, r FROM W;""");
        Assert.assertEquals(new Shape(2, 2), shape(ccs));
        // Expected output validated with Postgres 14.
        ccs.stepWeightOne("""
                INSERT INTO T VALUES (1, 10, 5, 'x', 0), (2, 10, 5, 'y', 1), (3, 10, 7, 'z', 0), (4, 20, 1, 'w', 0);""",
                """
                 g  | r
                --------
                 10 | 1
                 10 | 1
                 10 | 3
                 20 | 1""");
    }
}
