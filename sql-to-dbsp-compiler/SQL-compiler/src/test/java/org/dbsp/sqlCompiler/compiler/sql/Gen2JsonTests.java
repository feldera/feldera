package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Assert;
import org.junit.Test;

/** The circuit JSON written for the Gen-2 engine ({@code --gen2}) carries no Rust-codegen
 * wrappers: a {@code TYPEDBOX} is written as the expression it boxes, and a
 * {@code TypedBox<T, _>} type as {@code T}.  The {@code --jit} JSON keeps both. */
public class Gen2JsonTests extends SqlIoTest {
    /** A temporal filter against NOW(): the Rust backend boxes its window bounds. */
    static final String WINDOW_PROGRAM = """
            CREATE TABLE events(ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR, id BIGINT NOT NULL);
            CREATE VIEW recent AS SELECT id FROM events WHERE ts >= NOW() - INTERVAL 1 HOUR;""";

    String circuitJson(boolean gen2) {
        CompilerOptions options = this.testOptions();
        options.ioOptions.gen2 = gen2;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation(WINDOW_PROGRAM);
        DBSPCircuit circuit = compiler.getFinalCircuit(false);
        ToJsonOuterVisitor visitor = ToJsonOuterVisitor.create(compiler, 1);
        visitor.apply(circuit);
        return visitor.getJsonString();
    }

    @Test
    public void jitJsonKeepsTypedBox() {
        String json = this.circuitJson(false);
        Assert.assertTrue(json.contains("\"TYPEDBOX\""));
        Assert.assertTrue(json.contains("\"DBSPTypeTypedBox\""));
    }

    @Test
    public void gen2JsonDropsTypedBox() {
        String json = this.circuitJson(true);
        Assert.assertFalse(json.contains("TYPEDBOX"));
        Assert.assertFalse(json.contains("TypedBox"));
        // The window and the bound it boxed (NOW() - INTERVAL 1 HOUR) are still there, unwrapped.
        Assert.assertTrue(json.contains("\"DBSPWindowOperator\""));
        Assert.assertTrue(json.contains("\"DBSPTimeAddSub\""));
    }
}
