package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.junit.Assert;
import org.junit.Test;

/** The --gen2 primary-key dedup in {@link ExpandIndexedInputs} and in {@code CREATE INDEX}.
 *
 * <p>With --gen2 the source's indexed z-set drops the primary-key columns from the value, so
 * {@code key ++ value} holds each column once; without --gen2 the value is the whole row (the
 * shape the Rust backend expects).  A {@code CREATE INDEX} follows the same rule, which makes
 * the index payload a user-visible difference between the two engines.  These tests pin the
 * key/value arities of the {@link DBSPSourceMapOperator} and of the index's
 * {@link DBSPMapIndexOperator} for both settings.
 *
 * <p>Note {@code --gen2} rather than {@code --jit}: {@code --jit} emits the IR without
 * changing how it is built, and {@code --gen2} is what asks for the dedup. */
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

    /** Records the key/value arities of every map-index operator in a circuit. */
    static class IndexShape extends CircuitVisitor {
        int keyArity = -1;
        int valueArity = -1;
        int indexes = 0;

        IndexShape(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public void postorder(DBSPMapIndexOperator node) {
            DBSPTypeIndexedZSet ix = node.getOutputIndexedZSetType();
            this.keyArity = ix.keyType.to(DBSPTypeTuple.class).size();
            this.valueArity = ix.elementType.to(DBSPTypeTuple.class).size();
            this.indexes++;
        }
    }

    private IndexShape indexShape(String sql, boolean gen2) {
        CompilerOptions options = this.testOptions();
        options.ioOptions.gen2 = gen2;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation(sql);
        DBSPCircuit circuit = compiler.getFinalCircuit(false);
        IndexShape shape = new IndexShape(compiler);
        shape.apply(circuit);
        return shape;
    }

    private SourceMapShape shape(String sql, boolean gen2) {
        CompilerOptions options = this.testOptions();
        options.ioOptions.gen2 = gen2;
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

    /** CREATE INDEX with the key columns given out of row order: the key tuple follows the
     * index's column order while the deduped value keeps the remaining columns in row order,
     * so the two cannot be matched up positionally.  This is a user-visible difference: the
     * same CREATE INDEX yields a 1-column payload on the Gen-2 engine and a 3-column payload
     * on the Rust backend. */
    @Test
    public void createIndexKeyOutOfRowOrder() {
        String sql = """
                CREATE TABLE t(a INT, id INT NOT NULL, b VARCHAR);
                CREATE VIEW v AS SELECT * FROM t;
                CREATE INDEX vi ON v(b, id);""";
        IndexShape gen2 = this.indexShape(sql, true);
        Assert.assertEquals(1, gen2.indexes);
        Assert.assertEquals(2, gen2.keyArity);
        // Only `a` remains in the value; `b` and `id` moved into the key.
        Assert.assertEquals(1, gen2.valueArity);

        IndexShape plain = this.indexShape(sql, false);
        Assert.assertEquals(1, plain.indexes);
        Assert.assertEquals(2, plain.keyArity);
        Assert.assertEquals(3, plain.valueArity);
    }

    /** Corner case: CREATE INDEX over every column, so the deduped payload is the empty
     * tuple. */
    @Test
    public void createIndexOverAllColumns() {
        String sql = """
                CREATE TABLE t(a INT, b VARCHAR);
                CREATE VIEW v AS SELECT * FROM t;
                CREATE INDEX vi ON v(a, b);""";
        IndexShape gen2 = this.indexShape(sql, true);
        Assert.assertEquals(2, gen2.keyArity);
        Assert.assertEquals(0, gen2.valueArity);

        IndexShape plain = this.indexShape(sql, false);
        Assert.assertEquals(2, plain.keyArity);
        Assert.assertEquals(2, plain.valueArity);
    }

    /** --jit asks for the circuit IR without changing how it is built, so it must not dedup:
     * only --gen2 does.  Pins the split, since the two flags were once one. */
    @Test
    public void jitAloneDoesNotDedup() {
        String sql = """
                CREATE TABLE t(a INT, id INT NOT NULL PRIMARY KEY, b INT);
                CREATE VIEW v AS SELECT * FROM t;
                CREATE INDEX vi ON v(id);""";
        CompilerOptions options = this.testOptions();
        options.ioOptions.interpreterJson = true;
        Assert.assertTrue("--jit asks for the circuit IR", options.ioOptions.emitCircuitIr());
        Assert.assertFalse("--jit alone does not dedup", options.ioOptions.gen2);

        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation(sql);
        DBSPCircuit circuit = compiler.getFinalCircuit(false);

        SourceMapShape source = new SourceMapShape(compiler);
        source.apply(circuit);
        Assert.assertEquals(1, source.keyArity);
        Assert.assertEquals("--jit keeps the whole row in the value", 3, source.valueArity);

        IndexShape index = new IndexShape(compiler);
        index.apply(circuit);
        Assert.assertEquals(1, index.keyArity);
        Assert.assertEquals("--jit keeps the whole row in the index payload", 3, index.valueArity);
    }

    /** --gen2 implies --jit: it asks for the circuit IR as well as the dedup. */
    @Test
    public void gen2ImpliesJit() {
        CompilerOptions options = this.testOptions();
        options.ioOptions.gen2 = true;
        Assert.assertTrue("--gen2 implies --jit", options.ioOptions.emitCircuitIr());
        Assert.assertFalse("--gen2 does not set --jit itself", options.ioOptions.interpreterJson);
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

    /** With --gen2, LATENESS on a primary-key table is rejected with a clear "not yet
     * implemented" error rather than emitting an indexed z-set whose value duplicates the key:
     * the lateness/waterline path does not dedup the key columns yet.  The message names the
     * engine rather than the flag, since a user selects the engine through `runtime_version`
     * and never types --gen2. */
    @Test
    public void latenessOnPrimaryKeyTableRejectedUnderGen2() {
        CompilerOptions options = this.testOptions();
        options.ioOptions.gen2 = true;
        options.languageOptions.incrementalize = true;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation(
                "CREATE TABLE t(id INT NOT NULL PRIMARY KEY, " +
                "ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOURS, a INT);" +
                "CREATE VIEW v AS SELECT * FROM t;");
        try {
            compiler.getFinalCircuit(false);
            Assert.fail("expected --gen2 + LATENESS on a primary-key table to be rejected");
        } catch (Exception e) {
            String message = e.getMessage() == null ? e.toString() : e.getMessage();
            Assert.assertTrue("unexpected error: " + e,
                    message.contains("not yet supported by the Gen-2 engine"));
            Assert.assertFalse("the message must not name a flag the user never typed: " + e,
                    message.contains("--gen2"));
        }
    }
}
