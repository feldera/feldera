package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustInnerVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.visitors.inner.BetaReduction;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Objects;

public class QuotedFunctionParameterTests {
    @Test public void quotedNamesPreserveArgumentPositionsAndAvoidLegalNames() {
        for (int level: new int[] {0, 2}) {
            for (String name: new String[] {"a\"b", "with space", "123"}) {
                for (int selected = 0; selected < 2; selected++) {
                    String quoted = "\"" + name.replace("\"", "\"\"") + "\"";
                    String legal = "__feldera_arg_0";
                    var options = new CompilerOptions();
                    options.ioOptions.quiet = true;
                    options.languageOptions.optimizationLevel = level;
                    var compiler = new DBSPCompiler(options);
                    compiler.submitStatementsForCompilation("CREATE FUNCTION f(" + quoted
                            + " INT NOT NULL, " + legal + " INT NOT NULL) RETURNS INT NOT NULL AS "
                            + (selected == 0 ? quoted : legal) + "; CREATE VIEW v AS SELECT f(17,29) AS n;");
                    Assert.assertNotNull(compiler.getFinalCircuit(false));
                    Assert.assertEquals(compiler.messages.toString(), 0, compiler.messages.errorCount());
                    var udf = Objects.requireNonNull(compiler.getCustomFunctions().getUDF(new ProgramIdentifier("f")));
                    var typeCompiler = new TypeCompiler(compiler);
                    var implementation = Objects.requireNonNull(udf.getImplementation(typeCompiler, compiler));
                    var declaration = udf.getDeclaration(typeCompiler);
                    var names = new HashSet<String>();
                    for (int index = 0; index < 2; index++) {
                        String actual = implementation.parameters.get(index).name;
                        Assert.assertTrue(actual, Utilities.isLegalRustIdentifier(actual));
                        Assert.assertTrue("Duplicate parameter " + actual, names.add(actual));
                        Assert.assertEquals(actual, declaration.parameters.get(index).name);
                    }
                    Assert.assertEquals(legal, implementation.parameters.get(1).name);
                    var applied = new DBSPApplyExpression(implementation.asClosure(),
                            new DBSPI32Literal(17), new DBSPI32Literal(29));
                    var result = new BetaReduction(compiler).reduce(applied).to(DBSPI32Literal.class);
                    Assert.assertEquals(Integer.valueOf(selected == 0 ? 17 : 29), result.value);
                    String rust = ToRustInnerVisitor.toRustString(compiler, implementation, null, false);
                    Assert.assertFalse(rust, rust.contains(name + ":"));
                }
            }
        }
    }

    @Test public void externalDeclarationsAlsoUseRustParameterNames() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        compiler.submitStatementsForCompilation("CREATE FUNCTION f(\"a b\" INT) RETURNS INT;");
        compiler.getFinalCircuit(false);
        Assert.assertEquals(compiler.messages.toString(), 0, compiler.messages.errorCount());
        var udf = Objects.requireNonNull(compiler.getCustomFunctions().getUDF(new ProgramIdentifier("f")));
        var declaration = udf.getDeclaration(new TypeCompiler(compiler));
        Assert.assertTrue(Utilities.isLegalRustIdentifier(declaration.parameters.get(0).name));
        Assert.assertEquals("a b", udf.parameterList.get(0).getName());
    }
}
