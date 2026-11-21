package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.junit.Assert;
import org.junit.Test;

/** Regression coverage for type inconsistencies found by the P0 scalar probes. */
public class TypeCoercionTests {
    private static DBSPCompiler compile(String program, int optimizationLevel) {
        CompilerOptions options = new CompilerOptions();
        options.ioOptions.quiet = true;
        options.languageOptions.optimizationLevel = optimizationLevel;
        options.languageOptions.throwOnError = false;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation(program);
        compiler.getFinalCircuit(false);
        return compiler;
    }

    private static void assertNullable(String program, boolean nullable, int optimizationLevel) {
        var compiler = compile(program, optimizationLevel);
        Assert.assertFalse(compiler.messages.toString(), compiler.hasErrors());
        for (var output: compiler.metadata.asJson().get("outputs")) {
            if (output.get("name").asText().equals("v")) {
                Assert.assertEquals(program, nullable,
                        output.get("fields").get(0).get("columntype").get("nullable").asBoolean());
                return;
            }
        }
        Assert.fail("Missing output view v");
    }

    @Test
    public void booleanNumericEqualityWorksWithAssertions() {
        for (String numeric: new String[] {"REAL", "DOUBLE", "DECIMAL(10,2)"}) {
            for (String operator: new String[] {"=", "<>"}) {
                for (String expression: new String[] {"a " + operator + " b", "b " + operator + " a"})
                    assertNullable("CREATE TABLE t(a BOOLEAN, b " + numeric + ");"
                            + "CREATE VIEW v AS SELECT " + expression + " AS result FROM t;", true, 0);
            }
        }
    }

    @Test
    public void explicitBooleanNumericCastsKeepExistingRejection() {
        for (String numeric: new String[] {"REAL", "DOUBLE", "DECIMAL(10,2)"}) {
            var compiler = compile("CREATE TABLE t(a BOOLEAN); CREATE VIEW v AS SELECT CAST(a AS "
                    + numeric + ") AS result FROM t;", 0);
            Assert.assertTrue(compiler.messages.toString(), compiler.hasErrors());
            Assert.assertFalse(compiler.messages.toString(), compiler.messages.messages.stream()
                    .anyMatch(message -> message.errorType.contains("bug in the compiler")
                            || message.errorType.equals("Compiler error")));
        }
    }

    @Test
    public void booleanLiteralComparisonsKeepExistingCoercion() {
        for (String expression: new String[] {"a = 0", "1 = a", "a <> 1", "0 <> a"})
            assertNullable("CREATE TABLE t(a BOOLEAN); CREATE VIEW v AS SELECT "
                    + expression + " AS result FROM t;", true, 0);
    }

    @Test
    public void booleanIntegerComparisonsKeepExistingCoercion() {
        for (String numeric: new String[] {"TINYINT", "SMALLINT", "INT", "BIGINT"}) {
            for (String expression: new String[] {"a = b", "b = a", "a <> b", "b <> a"})
                assertNullable("CREATE TABLE t(a BOOLEAN, b " + numeric + ");"
                        + "CREATE VIEW v AS SELECT " + expression + " AS result FROM t;", true, 0);
        }
    }

}
