package org.dbsp.sqlCompiler.compiler.jit;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.postgres.PostgresStringTests;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("Not yet working")
public class JitPostgresStringTests extends PostgresStringTests {
    @Override
    public CompilerOptions getOptions(boolean optimize) {
        CompilerOptions options = super.getOptions(optimize);
        options.ioOptions.jit = true;
        return options;
    }

    @Test @Override @Ignore("BINARY type not yet supported by JIT")
    public void testBinary() {
        super.testBinary();
    }

    @Test @Override @Ignore("LIKE not yet supported by JIT")
    public void testLike2() {
        super.testLike2();
    }

    @Test @Override @Ignore("LIKE not yet supported by JIT")
    public void testRlike() {
        super.testRlike();
    }

    @Test @Override @Ignore("LIKE not yet supported by JIT")
    public void testRlike2() {
        super.testRlike2();
    }

    @Test @Override @Ignore("LIKE not yet supported by JIT")
    public void testLike3() {
        super.testLike3();
    }

    @Test @Override @Ignore("LIKE not yet supported by JIT")
    public void testLikeCombinations() {
        super.testLikeCombinations();
    }

    @Test @Override @Ignore("OVERLAY not yet supported by JIT")
    public void testOverlay() {
        super.testOverlay();
    }

    @Test @Override @Ignore("TRIM not yet implemented in JIT")
    public void testTrim() {
        super.testTrim();
    }

    @Test @Override @Ignore("TRIM not yet implemented in JIT")
    public void testTrimConstant() {
        super.testTrimConstant();
    }

    @Test @Override @Ignore("TRIM not yet implemented in JIT")
    public void testTrimArg() {
        super.testTrimArg();
    }

    @Test @Override @Ignore("CHR not yet implemented in JIT")
    public void testChr() {
        super.testChr();
    }

    @Test @Override @Ignore("ASCII not yet implemented in JIT")
    public void testAscii() {
        super.testAscii();
    }

    @Test @Override @Ignore("SUBSTRING not yet implemented in JIT")
    public void testSubstring() {
        super.testSubstring();
    }

    @Test @Override @Ignore("SUBSTRING not yet implemented in JIT")
    public void testSubstringOverflow() {
        super.testSubstringOverflow();
    }

    @Test @Override @Ignore("SUBSTRING not yet implemented in JIT")
    public void testNegativeSubstringLength() {
        super.testNegativeSubstringLength();
    }

    @Test @Override @Ignore("POSITION not yet implemented in JIT")
    public void testPosition() {
        super.testPosition();
    }

    @Test @Override @Ignore("POSITION not yet implemented in JIT")
    public void testStrpos() {
        super.testStrpos();
    }

    @Test @Override @Ignore("LENGTH not yet implemented in JIT")
    public void testLength() {
        super.testLength();
    }
}
