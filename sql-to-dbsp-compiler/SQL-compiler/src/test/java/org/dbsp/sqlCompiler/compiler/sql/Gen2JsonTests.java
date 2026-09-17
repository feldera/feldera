package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Assert;
import org.junit.Test;

/** The circuit JSON written for the Gen-2 engine ({@code --gen2}) carries no Rust-codegen
 * artifacts: a {@code TYPEDBOX} is written as the expression it boxes, a
 * {@code TypedBox<T, _>} type as {@code T}, a {@code clone()} as the expression it clones, and
 * an aggregate operator keeps its per-aggregate list instead of one fold over a tuple
 * accumulator.  The {@code --jit} JSON keeps the Rust forms. */
public class Gen2JsonTests extends SqlIoTest {
    /** A temporal filter against NOW(): the Rust backend boxes its window bounds. */
    static final String WINDOW_PROGRAM = """
            CREATE TABLE events(ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR, id BIGINT NOT NULL);
            CREATE VIEW recent AS SELECT id FROM events WHERE ts >= NOW() - INTERVAL 1 HOUR;""";

    /** Three aggregates that cannot use the linear form: a floating-point SUM, an ARRAY_AGG
     * (an in-place step), and a BIT_XOR. */
    static final String FOLD_PROGRAM = """
            CREATE TABLE sales(region VARCHAR NOT NULL, qty INT, score DOUBLE, tag VARCHAR);
            CREATE VIEW fold_agg AS
            SELECT region, SUM(score), ARRAY_AGG(tag), BIT_XOR(qty) FROM sales GROUP BY region;""";

    String circuitJson(boolean gen2) {
        return this.circuitJson(WINDOW_PROGRAM, gen2);
    }

    String circuitJson(String program, boolean gen2) {
        CompilerOptions options = this.testOptions();
        options.ioOptions.gen2 = gen2;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation(program);
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

    @Test
    public void jitJsonKeepsClone() {
        // ARRAY_AGG(tag) clones the VARCHAR it appends.
        Assert.assertTrue(this.circuitJson(FOLD_PROGRAM, false).contains("\"DBSPCloneExpression\""));
    }

    @Test
    public void gen2JsonDropsClone() {
        String json = this.circuitJson(FOLD_PROGRAM, true);
        Assert.assertFalse(json.contains("DBSPCloneExpression"));
        // The cloned field access is still the argument of the step.
        Assert.assertTrue(json.contains("\"array_aggN\""));
    }

    static int occurrences(String text, String pattern) {
        return text.split(java.util.regex.Pattern.quote(pattern), -1).length - 1;
    }

    @Test
    public void jitJsonPacksTheAggregatesIntoOneFold() {
        String json = this.circuitJson(FOLD_PROGRAM, false);
        Assert.assertEquals(1, occurrences(json, "\"DBSPFold\""));
        // The packed step writes the three accumulator fields through a mutable reference.
        Assert.assertTrue(json.contains("\"DBSPAssignmentExpression\""));
        Assert.assertFalse(json.contains("\"DBSPAggregateList\""));
    }

    @Test
    public void gen2JsonKeepsTheAggregateList() {
        String json = this.circuitJson(FOLD_PROGRAM, true);
        Assert.assertEquals(1, occurrences(json, "\"DBSPAggregateList\""));
        // One entry per SQL aggregate, each with its own zero, step, and post-processing.
        Assert.assertEquals(3, occurrences(json, "\"NonLinearAggregate\""));
        Assert.assertFalse(json.contains("\"DBSPFold\""));
        // A step returns its new accumulator; nothing assigns into a packed tuple.
        Assert.assertFalse(json.contains("\"DBSPAssignmentExpression\""));
    }
}
