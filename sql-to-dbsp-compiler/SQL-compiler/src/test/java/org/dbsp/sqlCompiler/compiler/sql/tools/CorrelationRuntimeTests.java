package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Explicit P0 integration run: independently specified signed output bags,
 * executed by both the original compiler and the Rust decorrelator through S1.
 * Requires -Dcorrelation.binary, -Dcorrelation.fixtures and -Dcorrelation.output. */
public class CorrelationRuntimeTests extends BaseSQLTests {
    private static String changes(JsonNode inputs, int sign) {
        var sql = new StringBuilder();
        inputs.fields().forEachRemaining(table -> {
            if (!table.getKey().matches("[A-Za-z][A-Za-z0-9_]*"))
                throw new IllegalArgumentException("Invalid fixture table name");
            for (var entry: table.getValue()) {
                long weight = entry.path("weight").longValue() * sign;
                if (Math.abs(weight) > 1000) throw new IllegalArgumentException("Fixture weight limit");
                var cells = new ArrayList<String>();
                for (var cell: entry.path("row")) {
                    if (!cell.isNull() && !cell.isInt()) throw new IllegalArgumentException("Expected integer fixture");
                    cells.add(cell.isNull() ? "NULL" : cell.toString());
                }
                for (long repeat = 0; repeat < Math.abs(weight); repeat++)
                    sql.append(weight > 0 ? "INSERT INTO " : "REMOVE FROM ").append(table.getKey())
                            .append(" VALUES (").append(String.join(", ", cells)).append(");\n");
            }
        });
        return sql.toString();
    }

    private static String expected(JsonNode rows, int width, int sign) {
        var names = new ArrayList<String>();
        for (int index = 0; index < width; index++) names.add("c" + index);
        names.add("weight");
        var result = new StringBuilder(String.join(" | ", names)).append("\n")
                .append("---".repeat(width + 1)).append("\n");
        for (var row: rows) {
            Assert.assertEquals(width, row.path("row").size());
            var values = new ArrayList<String>();
            for (var value: row.path("row")) values.add(value.isNull() ? "NULL" : value.toString());
            values.add(Long.toString(row.path("weight").longValue() * sign));
            result.append(String.join(" | ", values)).append("\n");
        }
        return result.toString().stripTrailing();
    }

    @Test public void independentlySpecifiedBagsMatchBeforeAndAfterRetractions() throws Exception {
        String executable = System.getProperty("correlation.binary");
        Assume.assumeNotNull(executable);
        var mapper = Utilities.deterministicObjectMapper();
        var manifest = mapper.readTree(Files.readString(Path.of(System.getProperty("correlation.fixtures"))));
        var output = Path.of(System.getProperty("correlation.output"), "bags");
        Files.createDirectories(output.getParent()); Files.createDirectory(output);
        for (var fixture: manifest.path("fixtures")) {
            for (boolean rust: List.of(false, true)) {
                var compiler = this.testCompiler(); compiler.options.languageOptions.incrementalize = true;
                RustCorrelationRewrite rewrite = null;
                if (rust) {
                    rewrite = new RustCorrelationRewrite(compiler, Path.of(executable),
                            output.resolve(fixture.path("id").asText()));
                    compiler.getSqlToRelCompiler().optimizerInputTransform = rewrite;
                }
                compiler.submitStatementsForCompilation(fixture.path("schema_sql").asText()
                        + " CREATE VIEW v AS " + fixture.path("query_sql").asText() + ";");
                var circuit = this.getCCS(compiler);
                if (rewrite != null) Assert.assertTrue("Fixture did not exercise Rust", rewrite.rewritten > 0);
                int width = circuit.circuit.getSingleOutputType()
                        .to(org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet.class).elementType
                        .to(org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple.class).size();
                var empty = mapper.createArrayNode();
                circuit.step("", expected(empty, width, 1));
                circuit.step(changes(fixture.path("inputs"), 1), expected(fixture.path("expected"), width, 1));
                circuit.step(changes(fixture.path("inputs"), -1), expected(fixture.path("expected"), width, -1));
            }
        }
    }

    @Test public void incrementalDomainChangesMatch() throws Exception {
        String executable = System.getProperty("correlation.binary");
        Assume.assumeNotNull(executable);
        var mapper = Utilities.deterministicObjectMapper();
        var manifest = mapper.readTree(Files.readString(Path.of(System.getProperty("correlation.fixtures"))));
        var extension = manifest.path("incremental_extension");
        JsonNode fixture = null;
        for (var candidate: manifest.path("fixtures"))
            if (candidate.path("id").equals(extension.path("fixture"))) fixture = candidate;
        Assert.assertNotNull(fixture);
        var output = Path.of(System.getProperty("correlation.output"), "last-duplicate");
        Files.createDirectories(output.getParent()); Files.createDirectory(output);
        for (boolean rust: List.of(false, true)) {
            var compiler = this.testCompiler(); compiler.options.languageOptions.incrementalize = true;
            RustCorrelationRewrite rewrite = null;
            if (rust) {
                rewrite = new RustCorrelationRewrite(compiler, Path.of(executable), output.resolve("rust"));
                compiler.getSqlToRelCompiler().optimizerInputTransform = rewrite;
            }
            compiler.submitStatementsForCompilation(fixture.path("schema_sql").asText()
                    + " CREATE VIEW v AS " + fixture.path("query_sql").asText() + ";");
            var circuit = this.getCCS(compiler);
            if (rewrite != null) Assert.assertTrue("Fixture did not exercise Rust", rewrite.rewritten > 0);
            int width = circuit.circuit.getSingleOutputType()
                    .to(org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet.class).elementType
                    .to(org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple.class).size();
            circuit.step("", expected(mapper.createArrayNode(), width, 1));
            circuit.step(changes(extension.path("replace_initial_inputs"), 1),
                    expected(extension.path("initial_expected"), width, 1));
            for (var step: extension.path("steps"))
                circuit.step(changes(step.path("input_deltas"), 1), expected(step.path("expected_delta"), width, 1));
        }
    }
}
