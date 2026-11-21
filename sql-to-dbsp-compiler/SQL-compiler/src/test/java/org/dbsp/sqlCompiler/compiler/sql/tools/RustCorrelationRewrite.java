package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.rel.RelNode;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer.CalciteOptimizer;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/** Execute the Rust process and reconstruct its explicit constructors before
 * Java's decorrelation stage. Unsupported inputs fail the test, with no fallback. */
final class RustCorrelationRewrite implements BiFunction<String, RelNode, RelNode> {
    private final DBSPCompiler compiler;
    private final Path binary;
    private final Path directory;
    private final S1BuiltinOperatorRegistry registry = new S1BuiltinOperatorRegistry();
    private int invocations;
    int rewritten;

    RustCorrelationRewrite(DBSPCompiler compiler, Path binary, Path directory) throws IOException {
        this.compiler = compiler; this.binary = binary; this.directory = directory;
        Files.createDirectory(directory);
    }

    @Override public RelNode apply(String step, RelNode plan) {
        if (!step.equals(CorrelationPlanProbe.STAGE)) return plan;
        int index = this.invocations++;
        if (!CalciteOptimizer.containsCorrelate(plan)) return plan;
        try {
            var root = this.directory.resolve("invocation-" + index); Files.createDirectory(root);
            var fragment = CorrelationPlanProbe.encode("invocation-" + index, plan, this.compiler, this.registry);
            var input = root.resolve("fragment.jsonl");
            Files.writeString(input, fragment + "\n", StandardOpenOption.CREATE_NEW);
            var output = root.resolve("rust");
            var process = new ProcessBuilder(this.binary.toString(), input.toString(), output.toString())
                    .redirectErrorStream(true).redirectOutput(root.resolve("process.log").toFile()).start();
            try {
                if (!process.waitFor(30, TimeUnit.SECONDS)) {
                    process.destroyForcibly(); process.waitFor();
                    throw new IOException("Rust decorrelator exceeded 30 seconds");
                }
            } catch (InterruptedException error) {
                process.destroyForcibly(); Thread.currentThread().interrupt();
                throw new IOException("Rust decorrelator interrupted", error);
            }
            if (process.exitValue() != 0) throw new IOException("Rust decorrelator failed: " + process.exitValue());
            var mapper = Utilities.deterministicObjectMapper().copy()
                    .enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
            var lines = Files.readAllLines(output.resolve("recipes.jsonl"));
            if (lines.size() != 1) throw new IOException("Expected one Rust recipe");
            var outcome = mapper.readTree(lines.get(0));
            if (!outcome.path("id").equals(fragment.path("id"))
                    || !outcome.path("status").asText().equals("rewritten"))
                throw new IOException("Rust decorrelator refused input: " + outcome);
            var roots = CorrelationRecipe.decode(fragment, outcome.path("recipe"), this.registry);
            if (roots.size() != 1 || !roots.get(0).getRowType().equals(plan.getRowType()))
                throw new IOException("Rust decorrelator changed the root contract");
            this.rewritten++;
            return roots.get(0);
        } catch (IOException error) {
            throw new UncheckedIOException(error);
        }
    }
}
