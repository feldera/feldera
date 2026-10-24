package org.dbsp.sqlCompiler.compiler.sql.recursive;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustFileWriter;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustVisitor;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitPostfix;
import org.dbsp.util.IndentStream;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** The tests of {@link RecursiveTests}, using the legacy Rust code generator for
 * recursive components, which may be deprecated soon. */
public class TupleRecursiveTests extends RecursiveTests {
    @BeforeClass
    public static void emitTupleRecursion() {
        ToRustVisitor.useTupleRecursionApi = true;
    }

    /** A tuple cannot carry 16 streams, so the program is rejected instead of compiled */
    @Override
    @Test
    public void issue5193() {
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(mutuallyRecursiveViews(16));
        DBSPCircuit circuit = getCircuit(compiler);
        RustFileWriter writer = new RustFileWriter(new CircuitPostfix(compiler));
        writer.setOutputBuilder(new IndentStream(new StringBuilder()));
        writer.add(circuit);
        UnsupportedException exception = Assert.assertThrows(
                UnsupportedException.class, () -> writer.write(compiler));
        Assert.assertTrue(exception.getMessage(),
                exception.getMessage().contains("16 mutually recursive views"));
    }
}
