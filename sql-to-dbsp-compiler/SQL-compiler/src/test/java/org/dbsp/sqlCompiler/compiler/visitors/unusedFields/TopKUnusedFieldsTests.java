package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/** Tests for the removing unused fields in {@link DBSPIndexedTopKOperator}. */
public class TopKUnusedFieldsTests extends SqlIoTest {
    /** Field count of the value part of a TopK's input and output,
     * and whether the TopK reports its output as a multiset. */
    record Shape(int inputFields, int outputFields, boolean isMultiset) {}

    static class TopKShapes extends CircuitVisitor {
        final List<Shape> shapes = new ArrayList<>();

        TopKShapes(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public void postorder(DBSPIndexedTopKOperator operator) {
            int input = operator.input().getOutputIndexedZSetType().getElementTypeTuple().size();
            int output = operator.getOutputIndexedZSetType().getElementTypeTuple().size();
            this.shapes.add(new Shape(input, output, operator.isMultiset));
        }
    }

    static class CountDistinct extends CircuitVisitor {
        int count = 0;

        CountDistinct(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public void postorder(DBSPDistinctOperator operator) {
            this.count++;
        }

        @Override
        public void postorder(DBSPStreamDistinctOperator operator) {
            this.count++;
        }
    }

    static final String TABLE = """
            CREATE TABLE T(id INT NOT NULL, ts INT NOT NULL, a INT, b INT, c INT);
            """;

    CompilerCircuitStream compile(String sql) {
        return this.getCCS(TABLE + sql);
    }

    /** The shape of the only TopK in the circuit */
    static Shape shape(CompilerCircuitStream ccs) {
        TopKShapes shapes = new TopKShapes(ccs.compiler);
        ccs.visit(shapes);
        Assert.assertEquals(1, shapes.shapes.size());
        return shapes.shapes.get(0);
    }

    /** The consumer reads two of the five columns and not the rank. */
    @Test
    public void testProjectionAfterTopK() {
        var ccs = this.compile("""
                CREATE VIEW V AS
                SELECT id, a FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) AS rn FROM T)
                WHERE rn = 1;""");
        Assert.assertEquals(new Shape(3, 2, false), shape(ccs));
        // Expected output validated with Postgres 14.
        ccs.stepWeightOne("INSERT INTO T VALUES (1, 10, 100, 0, 0), (1, 20, 200, 0, 0), (2, 5, 500, 0, 0);",
                """
                 id | a
                -------
                 1  | 200
                 2  | 500""");
    }

    /** The rank is read by the consumer, so the output stays a set. */
    @Test
    public void testRankKept() {
        var ccs = this.compile("""
                CREATE VIEW V AS
                SELECT id, a, rn FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) AS rn FROM T)
                WHERE rn <= 2;""");
        Assert.assertEquals(new Shape(3, 3, false), shape(ccs));
        // Expected output validated with Postgres 14.
        ccs.stepWeightOne("INSERT INTO T VALUES (1, 10, 100, 0, 0), (1, 20, 200, 0, 0), (1, 30, 300, 0, 0), (2, 5, 500, 0, 0);",
                """
                 id | a   | rn
                --------------
                 1  | 300 | 1
                 1  | 200 | 2
                 2  | 500 | 1 """);
    }

    /** The rank is emitted in the first position of the output. */
    @Test
    public void testRankFirst() {
        var ccs = this.compile("""
                CREATE VIEW V AS
                SELECT rn, id FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) AS rn FROM T)
                WHERE rn <= 2;""");
        Assert.assertEquals(new Shape(2, 2, false), shape(ccs));
        // Expected output validated with Postgres 14.
        ccs.stepWeightOne("INSERT INTO T VALUES (1, 10, 100, 0, 0), (1, 20, 200, 0, 0), (1, 30, 300, 0, 0), (2, 5, 500, 0, 0);",
                """
                 rn | id
                --------
                 1  | 1
                 2  | 1
                 1  | 2 """);
    }

    /** The filter left-over from the TopK condition reads the rank:
     * the rank must survive until that filter. */
    @Test
    public void testRankInFilter() {
        var ccs = this.compile("""
                CREATE VIEW V AS
                SELECT id, a FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) AS rn FROM T)
                WHERE rn <= 3 AND MOD(rn, 2) = 1;""");
        Assert.assertEquals(new Shape(3, 3, false), shape(ccs));
        // Expected output validated with Postgres 14.
        ccs.stepWeightOne("INSERT INTO T VALUES (1, 10, 100, 0, 0), (1, 20, 200, 0, 0), (1, 30, 300, 0, 0), (2, 5, 500, 0, 0);",
                """
                 id | a
                -------
                 1  | 300
                 1  | 100
                 2  | 500""");
    }

    /** Two rows that agree on the projected columns: without the rank the
     * TopK emits them as one row with weight 2, so it must report a multiset,
     * and the DISTINCT that removes the duplicate must stay in the circuit. */
    @Test
    public void testDistinctAfterTrimmedTopK() {
        var ccs = this.compile("""
                CREATE VIEW V AS
                SELECT DISTINCT id, a FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) AS rn FROM T)
                WHERE rn <= 2;""");
        Assert.assertEquals(new Shape(3, 2, true), shape(ccs));
        CountDistinct distinct = new CountDistinct(ccs.compiler);
        ccs.visit(distinct);
        Assert.assertEquals(1, distinct.count);
        // Expected output validated with Postgres 14.
        ccs.stepWeightOne("INSERT INTO T VALUES (1, 10, 100, 0, 0), (1, 20, 100, 0, 0);",
                """
                 id | a
                -------
                 1  | 100""");
    }

    /** A consumer computing an expression still narrows the TopK to the columns it reads. */
    @Test
    public void testExpressionAfterTopK() {
        var ccs = this.compile("""
                CREATE VIEW V AS
                SELECT id, a + b AS s FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) AS rn FROM T)
                WHERE rn = 1;""");
        Assert.assertEquals(new Shape(4, 3, false), shape(ccs));
        // Expected output validated with Postgres 14.
        ccs.stepWeightOne("INSERT INTO T VALUES (1, 10, 100, 1, 0), (1, 20, 200, 2, 0);",
                """
                 id | s
                -------
                 1  | 202""");
    }

    /** RANK ties are decided by the equality comparator, which must be
     * remapped to the narrowed input row. */
    @Test
    public void testRankTies() {
        var ccs = this.compile("""
                CREATE VIEW V AS
                SELECT id, a FROM (
                    SELECT *, RANK() OVER (PARTITION BY id ORDER BY ts DESC) AS rn FROM T)
                WHERE rn <= 1;""");
        Assert.assertEquals(new Shape(3, 2, true), shape(ccs));
        // Expected output validated with Postgres 14.
        ccs.stepWeightOne("INSERT INTO T VALUES (1, 10, 100, 0, 0), (1, 10, 200, 0, 0), (1, 5, 300, 0, 0);",
                """
                 id | a
                -------
                 1  | 100
                 1  | 200""");
    }

    /** The consumer reads no column at all: the TopK emits empty tuples
     * and stores only the ORDER BY column. */
    @Test
    public void testCountAfterTopK() {
        var ccs = this.compile("""
                CREATE VIEW V AS
                SELECT COUNT(*) AS ct FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) AS rn FROM T)
                WHERE rn <= 2;""");
        Assert.assertEquals(new Shape(1, 0, true), shape(ccs));
        // Expected output validated with Postgres 14.
        ccs.stepWeightOne("INSERT INTO T VALUES (1, 10, 0, 0, 0), (1, 20, 0, 0, 0), (1, 30, 0, 0, 0), (2, 5, 0, 0, 0);",
                """
                 ct
                ---
                 3 """);
    }

    /** Two consumers read different columns of the same TopK:
     * the TopK keeps the union of the columns they read. */
    @Test
    public void testSharedTopK() {
        var ccs = this.compile("""
                CREATE LOCAL VIEW L AS
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) AS rn FROM T)
                WHERE rn = 1;
                CREATE VIEW V AS
                SELECT id, a AS v FROM L
                UNION ALL
                SELECT id, c AS v FROM L;""");
        // Stored: id, ts, a, c.  Emitted: id, a, c.
        Assert.assertEquals(new Shape(4, 3, false), shape(ccs));
        // Expected output validated with Postgres 14.
        ccs.stepWeightOne("INSERT INTO T VALUES (1, 10, 100, 0, 7), (1, 20, 200, 0, 8), (2, 5, 500, 0, 9);",
                """
                 id | v
                --------
                 1  | 200
                 1  | 8
                 2  | 500
                 2  | 9""");
    }

    /** DENSE_RANK keeps every row of the two best timestamps; its output is a multiset. */
    @Test
    public void testDenseRankTies() {
        var ccs = this.compile("""
                CREATE VIEW V AS
                SELECT id, a FROM (
                    SELECT *, DENSE_RANK() OVER (PARTITION BY id ORDER BY ts DESC) AS rn FROM T)
                WHERE rn <= 2;""");
        Assert.assertEquals(new Shape(3, 2, true), shape(ccs));
        // Expected output validated with Postgres 14.
        ccs.stepWeightOne("INSERT INTO T VALUES (1, 10, 100, 0, 0), (1, 10, 200, 0, 0), (1, 5, 300, 0, 0), (1, 1, 400, 0, 0);",
                """
                 id | a
                --------
                 1  | 100
                 1  | 200
                 1  | 300""");
    }

    /** FIRST_VALUE is a TopK with k = 1 whose producer emits one column:
     * the TopK stores that column and the ORDER BY column only. */
    @Test
    public void testFirstValue() {
        var ccs = this.compile("""
                CREATE VIEW V AS
                SELECT id, a, FIRST_VALUE(b) OVER (PARTITION BY id ORDER BY ts) AS fb FROM T;""");
        Assert.assertEquals(new Shape(2, 1, false), shape(ccs));
        // Expected output validated with Postgres 14.
        ccs.stepWeightOne("INSERT INTO T VALUES (1, 10, 100, 1, 0), (1, 20, 200, 2, 0), (2, 5, 500, 3, 0);",
                """
                 id | a   | fb
                --------------
                 1  | 100 | 1
                 1  | 200 | 1
                 2  | 500 | 3""");
    }

    /** ORDER BY ... LIMIT 1 is a TopK over a single group; with one row it stays a set. */
    @Test
    public void testLimitOne() {
        var ccs = this.compile("""
                CREATE VIEW V AS SELECT id, a FROM T ORDER BY ts LIMIT 1;""");
        Assert.assertEquals(new Shape(3, 2, false), shape(ccs));
        // Expected output validated with Postgres 14.
        ccs.stepWeightOne("INSERT INTO T VALUES (1, 10, 100, 0, 0), (2, 5, 500, 0, 0), (3, 7, 700, 0, 0);",
                """
                 id | a
                --------
                 2  | 500""");
    }

    /** ORDER BY ... LIMIT 2 drops the rank and may repeat rows: a multiset. */
    @Test
    public void testLimitTwo() {
        var ccs = this.compile("""
                CREATE VIEW V AS SELECT id, a FROM T ORDER BY ts LIMIT 2;""");
        Assert.assertEquals(new Shape(3, 2, true), shape(ccs));
        // Expected output validated with Postgres 14.
        ccs.stepWeightOne("INSERT INTO T VALUES (1, 10, 100, 0, 0), (2, 5, 500, 0, 0), (3, 7, 700, 0, 0);",
                """
                 id | a
                --------
                 2  | 500
                 3  | 700""");
    }

    /** The soft-deletes pattern of docs/sql/streaming.md: a temporal filter keeps the
     * recent changes, the latest version per key is kept unless it is a deletion, and
     * a parent view filters and projects.  The projection crosses the view boundaries,
     * two filters, the D-I pair around the TopK, and the window of the temporal filter. */
    @Test
    public void testSoftDeletes() {
        var ccs = this.getCCS("""
                CREATE TABLE changes(
                    k INT NOT NULL, ts TIMESTAMP NOT NULL, deleted BOOLEAN,
                    name VARCHAR, qty INT, kind VARCHAR,
                    note VARCHAR, owner VARCHAR, price DECIMAL(10, 2), created DATE);
                CREATE LOCAL VIEW recent AS
                SELECT * FROM changes
                WHERE ts >= NOW() - INTERVAL 7 DAYS AND ts <= NOW();
                CREATE LOCAL VIEW latest AS
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY ts DESC, deleted NULLS FIRST) AS rn
                    FROM recent)
                WHERE rn = 1 AND deleted IS NOT TRUE;
                CREATE VIEW special AS
                SELECT name, qty FROM latest WHERE kind LIKE '%special%';""").withStringTrim();
        // Stored: ts, deleted, name, qty, kind; k survives only in the key.
        // Emitted: deleted, name, qty, kind.
        Assert.assertEquals(new Shape(5, 4, false), shape(ccs));
        // The window passes k, ts, deleted, name, qty, kind; the four columns nobody
        // reads (note, owner, price, created) are gone before it.
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            boolean seen = false;

            @Override
            public void postorder(DBSPWindowOperator operator) {
                int fields = operator.left().getOutputIndexedZSetType().getElementTypeTuple().size();
                Assert.assertEquals(6, fields);
                this.seen = true;
            }

            @Override
            public void endVisit() {
                Assert.assertTrue("no window operator in the circuit", this.seen);
            }
        });
        // Expected output validated with Postgres 14.  Key 10 is older than the window.
        ccs.stepWeightOne("""
                INSERT INTO NOW VALUES ('2020-01-10 00:00:00');
                INSERT INTO changes VALUES
                 (7, '2020-01-08 00:00:00', FALSE, 'n1', 10, 'special-a', 'x', 'o1', 1.50, '2020-01-01'),
                 (7, '2020-01-09 00:00:00', FALSE, 'n1', 20, 'special-a', 'x', 'o1', 1.75, '2020-01-01'),
                 (8, '2020-01-09 00:00:00', FALSE, 'n2', 30, 'plain', 'x', 'o2', 2.00, '2020-01-02'),
                 (9, '2020-01-08 00:00:00', FALSE, 'n3', 40, 'special', 'x', 'o3', 3.00, '2020-01-03'),
                 (9, '2020-01-09 00:00:00', TRUE, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
                 (10, '2019-12-01 00:00:00', FALSE, 'n4', 50, 'special', 'x', 'o4', 4.00, '2019-11-30');""",
                """
                 name | qty
                ------------
                 n1   | 20""");
    }
}
