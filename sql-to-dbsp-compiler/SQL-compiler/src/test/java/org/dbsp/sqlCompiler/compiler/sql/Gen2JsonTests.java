package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Assert;
import org.junit.Test;

/** The circuit compiled for the Gen-2 engine ({@code --gen2}) carries no Rust-codegen
 * artifacts: a constant stays where it is used instead of moving to a {@code static}
 * declaration.  The {@code --jit} circuit keeps the Rust forms. */
public class Gen2JsonTests extends SqlIoTest {
    /** A string and a decimal constant, which the Rust backend hoists into statics. */
    static final String CONSTANT_PROGRAM = """
            CREATE TABLE sales(region VARCHAR NOT NULL, price DECIMAL(10, 2));
            CREATE VIEW labelled AS SELECT region || '-suffix', price * 1.25 FROM sales;""";

    String circuitJson(String program, boolean gen2) {
        CompilerOptions options = this.testOptions();
        options.ioOptions.gen2 = gen2;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation(program);
        DBSPCircuit circuit = compiler.getFinalCircuit(false);
        Assert.assertNotNull(circuit);
        ToJsonOuterVisitor visitor = ToJsonOuterVisitor.create(compiler, 1);
        visitor.apply(circuit);
        return visitor.getJsonString();
    }

    @Test
    public void jitJsonHoistsConstantsIntoStatics() {
        String json = this.circuitJson(CONSTANT_PROGRAM, false);
        Assert.assertTrue(json.contains("\"DBSPStaticItem\""));
        Assert.assertTrue(json.contains("\"DBSPStaticExpression\""));
    }

    @Test
    public void gen2JsonKeepsConstantsInline() {
        String json = this.circuitJson(CONSTANT_PROGRAM, true);
        Assert.assertFalse(json.contains("DBSPStaticItem"));
        Assert.assertFalse(json.contains("DBSPStaticExpression"));
        // The constants are still in the projection, as literals.
        Assert.assertTrue(json.contains("-suffix"));
        Assert.assertTrue(json.contains("\"DBSPDecimalLiteral\""));
    }
}
