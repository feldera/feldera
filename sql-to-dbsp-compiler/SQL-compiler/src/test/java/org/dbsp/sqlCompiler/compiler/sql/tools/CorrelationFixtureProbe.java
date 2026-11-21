package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Validate the independently specified SQL fixtures and retain stage inputs. */
public final class CorrelationFixtureProbe {
    private CorrelationFixtureProbe() { }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) throw new IOException("usage: CorrelationFixtureProbe FIXTURES.json OUTPUT_DIR");
        var mapper = Utilities.deterministicObjectMapper();
        var fixtures = mapper.readTree(Files.readString(Path.of(args[0]))).path("fixtures");
        if (!fixtures.isArray() || fixtures.isEmpty()) throw new IOException("Empty fixture list");
        var output = Path.of(args[1]); Files.createDirectory(output);
        var registry = new S1BuiltinOperatorRegistry();
        int accepted = 0, failed = 0, fragments = 0;
        try (var records = Files.newBufferedWriter(output.resolve("cases.jsonl"), StandardOpenOption.CREATE_NEW);
             var coverage = Files.newBufferedWriter(output.resolve("coverage.jsonl"), StandardOpenOption.CREATE_NEW)) {
            for (var fixture: fixtures) {
                var input = mapper.createObjectNode(); input.set("id", fixture.path("id"));
                var options = new CompilerOptions(); options.ioOptions.quiet = true;
                input.set("options", mapper.valueToTree(options));
                input.putArray("submissions").addObject().put("batch", 0).put("many", true)
                        .put("sql", fixture.path("schema_sql").asText() + " CREATE VIEW v AS "
                                + fixture.path("query_sql").asText() + ";");
                var result = mapper.createObjectNode(); result.set("id", fixture.path("id"));
                try {
                    var captured = CorrelationPlanProbe.capture(input, registry);
                    result.put("compiled", true).put("fragments", captured.plans().size());
                    result.set("transport_failures", mapper.valueToTree(captured.failures()));
                    for (var record: captured.plans()) {
                        records.write(record.toString()); records.newLine(); fragments++;
                    }
                    accepted++;
                } catch (Exception error) {
                    result.put("compiled", false).put("error", error.toString()); failed++;
                }
                records.flush(); coverage.write(result.toString()); coverage.newLine(); coverage.flush();
            }
        }
        Files.writeString(output.resolve("complete.json"), mapper.createObjectNode()
                .put("complete", true).put("fixtures", fixtures.size()).put("compiled", accepted)
                .put("failed", failed).put("fragments", fragments).put("runtime_executed", false)
                .toPrettyString() + "\n", StandardOpenOption.CREATE_NEW);
    }
}
