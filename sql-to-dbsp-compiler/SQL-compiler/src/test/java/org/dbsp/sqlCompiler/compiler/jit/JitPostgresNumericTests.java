package org.dbsp.sqlCompiler.compiler.jit;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.postgres.PostgresNumericTests;
import org.junit.Ignore;
import org.junit.Test;

public class JitPostgresNumericTests extends PostgresNumericTests {
    @Override
    public CompilerOptions getOptions(boolean optimize) {
        CompilerOptions options = super.getOptions(optimize);
        options.ioOptions.jit = true;
        return options;
    }

    // Most bugs below are described in
    // https://github.com/feldera/feldera/issues/896

    @Override @Test @Ignore("abs not supported in JIT")
    public void testFunctionsNumeric0() {
        super.testFunctionsNumeric0();
    }

    @Test @Override @Ignore("sqrt not supported in JIT")
    public void testSqrt() {
        super.testSqrt();
    }

    @Override @Test @Ignore("sqrt not supported in JIT")
    public void squareRootTest() {
        super.squareRootTest();
    }

    @Override @Test @Ignore("log not implemented in JIT")
    public void testLog() {
        super.testLog();
    }

    @Override @Test @Ignore("ln not implemented in JIT")
    public void logarithmTest() {
        super.logarithmTest();
    }

    @Override @Test @Ignore("log10 not implemented in JIT")
    public void logarithm10Test() {
        super.logarithm10Test();
    }

    @Override @Test @Ignore("abs(f64) https://github.com/feldera/feldera/issues/897")
    public void testFunctions0() {
        super.testFunctions0();
    }

    @Override @Test @Ignore("Rounded results are wrong")
    public void testMultiply() { super.testMultiply(); }

    @Override @Test @Ignore("round not implemented in JIT")
    public void testFunctions1() {
        super.testFunctions1();
    }

    @Override @Test @Ignore("round not implemented in JIT")
    public void testRoundAdd() {
        super.testRoundAdd();
    }

    @Override @Test @Ignore("round not implemented in JIT")
    public void testRoundSubtraction() {
        super.testRoundSubtraction();
    }

    @Override @Test @Ignore("round not implemented in JIT")
    public void testRoundMultiply() {
        super.testRoundMultiply();
    }

    @Override @Test @Ignore("round not implemented in JIT")
    public void testDivisionRound() {
        super.testDivisionRound();
    }

    @Test @Ignore("incorrect precision results")
    public void testDivision() {
        super.testDivision();
    }
}
