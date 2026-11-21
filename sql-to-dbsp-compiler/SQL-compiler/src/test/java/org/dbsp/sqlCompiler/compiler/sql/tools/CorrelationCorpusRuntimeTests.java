package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Execute unchanged corpus tests with their original compiler options. */
public class CorrelationCorpusRuntimeTests extends BaseSQLTests {
    @Test public void originalUnpivotCorpusOraclesExecuteWithRustDecorrelation() throws Exception {
        String executable = System.getProperty("correlation.binary");
        Assume.assumeNotNull(executable);
        Assert.assertFalse("This probe requires actual classic execution", CorpusExporter.exportOnly());
        Assert.assertFalse("This probe requires the classic runtime", Gen2Runner.isEnabled());
        var output = Path.of(System.getProperty("correlation.output"), "unpivot");
        Files.createDirectories(output.getParent()); Files.createDirectory(output);
        class CorpusRun extends org.dbsp.sqlCompiler.compiler.sql.quidem.PivotTests {
            private final boolean rust;
            private final List<RustCorrelationRewrite> rewrites = new ArrayList<>();
            CorpusRun(boolean rust) {
                this.rust = rust;
                this.currentTestInformation = "CorrelationRuntimeTests#originalUnpivot/" + (rust ? "rust" : "java");
            }
            @Override public void prepareInputs(org.dbsp.sqlCompiler.compiler.DBSPCompiler compiler) {
                if (this.rust) {
                    try {
                        var rewrite = new RustCorrelationRewrite(compiler, Path.of(executable),
                                output.resolve("compiler-" + this.rewrites.size()));
                        this.rewrites.add(rewrite);
                        compiler.getSqlToRelCompiler().optimizerInputTransform = rewrite;
                    } catch (java.io.IOException error) { throw new java.io.UncheckedIOException(error); }
                }
                super.prepareInputs(compiler);
            }
        }
        new CorpusRun(false).unpivotTests();
        var rewritten = new CorpusRun(true); rewritten.unpivotTests();
        Assert.assertEquals("Both registered correlated query variants must execute through Rust", 2,
                rewritten.rewrites.stream().mapToInt(rewrite -> rewrite.rewritten).sum());
    }

}
