package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.junit.Assert;
import org.junit.Test;

/** The --jit primary-key dedup in {@link ExpandIndexedInputs}.
 *
 * <p>With --jit the source's indexed z-set drops the primary-key columns from the value, so
 * {@code key ++ value} holds each column once; without --jit the value is the whole row (the
 * shape the Rust backend expects).  These tests pin the key/value arities of the
 * {@link DBSPSourceMapOperator} the pass produces for both settings. */
public class DedupKeysTests extends SqlIoTest {
    /** Records the key/value arities of every source-map operator in a circuit. */
    static class SourceMapShape extends CircuitVisitor {
        int keyArity = -1;
        int valueArity = -1;
        int sources = 0;

        SourceMapShape(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public void postorder(DBSPSourceMapOperator node) {
            DBSPTypeIndexedZSet ix = node.getOutputIndexedZSetType();
            this.keyArity = ix.keyType.to(DBSPTypeTuple.class).size();
            this.valueArity = ix.elementType.to(DBSPTypeTuple.class).size();
            this.sources++;
        }
    }

    private SourceMapShape shape(String sql, boolean jit) {
        CompilerOptions options = this.testOptions();
        options.ioOptions.interpreterJson = jit;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation(sql);
        DBSPCircuit circuit = compiler.getFinalCircuit(false);
        SourceMapShape shape = new SourceMapShape(compiler);
        shape.apply(circuit);
        return shape;
    }

    /** One key column in the middle of the row: the value keeps the two non-key columns. */
    @Test
    public void singleColumnKeyInMiddle() {
        String sql = """
                CREATE TABLE t(a INT, id INT NOT NULL PRIMARY KEY, b INT);
                CREATE VIEW v AS SELECT * FROM t;""";
        SourceMapShape jit = this.shape(sql, true);
        Assert.assertEquals(1, jit.sources);
        Assert.assertEquals(1, jit.keyArity);
        Assert.assertEquals(2, jit.valueArity);

        SourceMapShape plain = this.shape(sql, false);
        Assert.assertEquals(1, plain.sources);
        Assert.assertEquals(1, plain.keyArity);
        Assert.assertEquals(3, plain.valueArity);
    }

    /** A composite key: two key columns, one value column. */
    @Test
    public void multiColumnKey() {
        String sql = """
                CREATE TABLE t(k1 INT NOT NULL, k2 INT NOT NULL, v INT, PRIMARY KEY(k1, k2));
                CREATE VIEW v AS SELECT * FROM t;""";
        SourceMapShape jit = this.shape(sql, true);
        Assert.assertEquals(2, jit.keyArity);
        Assert.assertEquals(1, jit.valueArity);

        SourceMapShape plain = this.shape(sql, false);
        Assert.assertEquals(2, plain.keyArity);
        Assert.assertEquals(3, plain.valueArity);
    }

    /** Corner case: every column is part of the key, so the deduped value is the empty tuple. */
    @Test
    public void allColumnsAreKey() {
        String sql = """
                CREATE TABLE t(k1 INT NOT NULL, k2 INT NOT NULL, PRIMARY KEY(k1, k2));
                CREATE VIEW v AS SELECT * FROM t;""";
        SourceMapShape jit = this.shape(sql, true);
        Assert.assertEquals(2, jit.keyArity);
        Assert.assertEquals(0, jit.valueArity);

        SourceMapShape plain = this.shape(sql, false);
        Assert.assertEquals(2, plain.keyArity);
        Assert.assertEquals(2, plain.valueArity);
    }

    /** The key column is last: exercises the interleave placing a value column before the key. */
    @Test
    public void keyColumnLast() {
        String sql = """
                CREATE TABLE t(a INT, b INT, id INT NOT NULL PRIMARY KEY);
                CREATE VIEW v AS SELECT * FROM t;""";
        SourceMapShape jit = this.shape(sql, true);
        Assert.assertEquals(1, jit.keyArity);
        Assert.assertEquals(2, jit.valueArity);
    }

    /** A table with no primary key stays a plain multiset under both settings: no source-map. */
    @Test
    public void noKeyStaysMultiset() {
        String sql = """
                CREATE TABLE t(a INT, b INT);
                CREATE VIEW v AS SELECT * FROM t;""";
        Assert.assertEquals(0, this.shape(sql, true).sources);
        Assert.assertEquals(0, this.shape(sql, false).sources);
    }

    /** With --jit, LATENESS on a primary-key table is rejected with a clear "not yet
     * implemented" error rather than emitting an indexed z-set whose value duplicates the key:
     * the lateness/waterline path does not dedup the key columns yet. Without --jit the same
     * table compiles. */
    @Test
    public void latenessOnPrimaryKeyTableRejectedUnderJit() {
        CompilerOptions options = this.testOptions();
        options.ioOptions.interpreterJson = true;
        options.languageOptions.incrementalize = true;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation(
                "CREATE TABLE t(id INT NOT NULL PRIMARY KEY, " +
                "ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOURS, a INT);" +
                "CREATE VIEW v AS SELECT * FROM t;");
        try {
            compiler.getFinalCircuit(false);
            Assert.fail("expected --jit + LATENESS on a primary-key table to be rejected");
        } catch (Exception e) {
            String message = e.getMessage() == null ? e.toString() : e.getMessage();
            Assert.assertTrue("unexpected error: " + e,
                    message.contains("not yet supported with --jit"));
        }
    }
}
