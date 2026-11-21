package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Select original corpus cases without changing their SQL, options or oracles. */
public class CorrelationScalarCorpusRuntimeTests extends BaseSQLTests {
    @Before public void requireRuntime() {
        Assume.assumeNotNull(System.getProperty("correlation.binary"));
        Assert.assertFalse(CorpusExporter.exportOnly());
        Assert.assertFalse(Gen2Runner.isEnabled());
    }

    private static Path output(String name) throws IOException {
        var result = Path.of(System.getProperty("correlation.output"), name);
        Files.createDirectories(result.getParent()); Files.createDirectory(result);
        return result;
    }

    @Test public void originalCorrelatedValuesRegression() throws Exception {
        var directory = output("values");
        class CorpusRun extends org.dbsp.sqlCompiler.compiler.sql.simple.ReduceExpressionsTests {
            private final boolean rust;
            private RustCorrelationRewrite rewrite;
            CorpusRun(boolean rust) {
                this.rust = rust;
                this.currentTestInformation = "CorrelationScalarCorpusRuntimeTests#correlatedValues/" + rust;
            }
            @Override public CompilerCircuitStream getCCS(String sql) {
                if (!this.rust) return super.getCCS(sql);
                var compiler = this.testCompiler(); this.prepareInputs(compiler);
                try {
                    this.rewrite = new RustCorrelationRewrite(compiler,
                            Path.of(System.getProperty("correlation.binary")), directory.resolve("rust"));
                } catch (IOException error) { throw new UncheckedIOException(error); }
                compiler.getSqlToRelCompiler().optimizerInputTransform = this.rewrite;
                compiler.submitStatementsForCompilation(sql);
                return super.getCCS(compiler);
            }
        }
        new CorpusRun(false).correlatedFilterOverValues();
        var rewritten = new CorpusRun(true); rewritten.correlatedFilterOverValues();
        Assert.assertEquals("Every correlated view must be rewritten", 3, rewritten.rewrite.rewritten);
    }

    @Test public void originalScalarMinAndCountVariants() throws Exception {
        var directory = output("foodmart");
        class CorpusRun extends org.dbsp.sqlCompiler.compiler.sql.quidem.FoodmartTests {
            private final boolean rust;
            private final List<RustCorrelationRewrite> rewrites = new ArrayList<>();
            private int comparisons;
            private int selected;
            CorpusRun(boolean rust) {
                this.rust = rust;
                this.currentTestInformation = "CorrelationScalarCorpusRuntimeTests#foodmartScalar/" + rust;
            }
            @Override public void compare(String query, String expected, boolean optimize,
                                          int rowCount, boolean trimTrailingSpaces) {
                // Exactly original corpus cases /0../3: the first two SQL
                // queries, each with its optimized and unoptimized variant.
                if (this.comparisons++ >= 4) return;
                this.selected++;
                super.compare(query, expected, optimize, rowCount, trimTrailingSpaces);
            }
            @Override public void prepareInputs(DBSPCompiler compiler) {
                if (this.rust) {
                    try {
                        var rewrite = new RustCorrelationRewrite(compiler,
                                Path.of(System.getProperty("correlation.binary")),
                                directory.resolve("compiler-" + this.rewrites.size()));
                        this.rewrites.add(rewrite);
                        compiler.getSqlToRelCompiler().optimizerInputTransform = rewrite;
                    } catch (IOException error) { throw new UncheckedIOException(error); }
                }
                super.prepareInputs(compiler);
            }
        }
        var baseline = new CorpusRun(false); baseline.testScalar();
        var rewritten = new CorpusRun(true); rewritten.testScalar();
        Assert.assertEquals(4, baseline.selected); Assert.assertEquals(4, rewritten.selected);
        Assert.assertEquals(4, rewritten.rewrites.stream().mapToInt(rewrite -> rewrite.rewritten).sum());
    }
}
