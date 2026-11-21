package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.RelNode;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer.CalciteOptimizer;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

/** Case-identified snapshots of the four subquery/decorrelation stages. */
public final class CorrelationProbe {
    private CorrelationProbe() { }
    static final Set<String> STAGES = Set.of("Remove sub-queries",
            "Decorrelate inner queries 1", "Decorrelate inner queries 2", "Decorrelate");

    static List<ObjectNode> capture(JsonNode input) throws IOException {
        var records = new ArrayList<ObjectNode>();
        var compiled = S1ExpressionCorpusProbe.compile(input, compiler -> {
            // The S1 helper normally stops before the circuit optimizer. This
            // census checks final schema/diagnostics against the exported case.
            compiler.options.ioOptions.inputCircuit = input.at("/options/ioOptions/inputCircuit").asBoolean();
            compiler.getSqlToRelCompiler().optimizerObserver = new CalciteOptimizer.Observer() {
            private ObjectNode current;
            private int sequence;

            @Override public void before(String step, RelNode plan) {
                if (!STAGES.contains(step)) return;
                if (this.current != null) throw new IllegalStateException("Unfinished stage observation");
                this.current = Utilities.deterministicObjectMapper().createObjectNode();
                this.current.put("sequence", this.sequence++).put("step", step);
                this.current.set("before", CorrelationShapes.capture(plan));
            }

            @Override public void after(String step, RelNode plan, boolean completed) {
                if (!STAGES.contains(step)) return;
                if (this.current == null || !this.current.path("step").asText().equals(step))
                    throw new IllegalStateException("Stage observation order changed");
                this.current.put("completed", completed);
                this.current.set("after", CorrelationShapes.capture(plan));
                records.add(this.current);
                this.current = null;
            }
            };
        });
        if (input.has("schema") && !compiled.metadata.asJson().equals(input.path("schema")))
            throw new IOException("Observed compilation changed the exported schema");
        if (input.has("diagnostics")
                && !compiled.messages.toJson(compiled.sources).equals(input.path("diagnostics")))
            throw new IOException("Observed compilation changed the exported diagnostics");
        return records;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) throw new IOException("usage: CorrelationProbe INPUT.jsonl OUTPUT_DIR");
        var mapper = Utilities.deterministicObjectMapper().copy()
                .enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        var output = Path.of(args[1]); Files.createDirectory(output);
        long cases = 0, stages = 0, correlatedCases = 0;
        var identities = new HashSet<String>();
        try (var reader = Files.newBufferedReader(Path.of(args[0]));
             var writer = Files.newBufferedWriter(output.resolve("cases.jsonl"), StandardOpenOption.CREATE_NEW)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.length() > 64 * 1024 * 1024 || cases >= 20_000)
                    throw new IOException("Correlation input exceeds limit");
                var input = mapper.readTree(line);
                var identity = input.path("id");
                if (!identity.isTextual() || identity.asText().isEmpty()
                        || identity.asText().length() > 4096 || !identities.add(identity.asText()))
                    throw new IOException("Missing, overlarge or duplicate correlation case identity");
                if (!input.has("schema") || !input.has("diagnostics"))
                    throw new IOException("Corpus input requires schema and diagnostic oracles");
                List<ObjectNode> observations;
                try { observations = capture(input); }
                catch (RuntimeException | IOException error) {
                    throw new IOException("Correlation capture failed for " + input.path("id"), error);
                }
                var record = mapper.createObjectNode().put("schema_version", 1);
                record.set("id", input.path("id"));
                record.put("stages_observed", observations.size());
                var retained = record.putArray("stages");
                for (var observation: observations) {
                    stages++;
                    // Keep both sides whenever either contains a Correlate or
                    // RexSubQuery; retain failures even without either form.
                    if (!observation.path("completed").booleanValue()
                            || observation.at("/before/correlates").intValue() != 0
                            || observation.at("/before/subqueries").intValue() != 0
                            || observation.at("/after/correlates").intValue() != 0
                            || observation.at("/after/subqueries").intValue() != 0)
                        retained.add(observation);
                }
                if (!retained.isEmpty()) correlatedCases++;
                writer.write(record.toString()); writer.newLine(); writer.flush(); cases++;
            }
        }
        if (cases == 0) throw new IOException("Empty correlation input");
        Files.writeString(output.resolve("complete.json"), mapper.createObjectNode()
                .put("complete", true).put("cases", cases).put("stages_observed", stages)
                .put("cases_with_retained_stages", correlatedCases).put("runtime_executed", false)
                .put("exported_schemas_equal", true).put("exported_diagnostics_equal", true)
                .toPrettyString() + "\n", StandardOpenOption.CREATE_NEW);
    }
}
