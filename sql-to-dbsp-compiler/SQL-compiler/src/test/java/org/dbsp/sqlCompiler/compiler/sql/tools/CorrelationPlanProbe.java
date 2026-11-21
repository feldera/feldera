package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer.CalciteOptimizer;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.function.UnaryOperator;

/** Export executable, catalog-backed FRel inputs before the first general
 * decorrelation stage. Decoding verifies the transport; it does not establish
 * runtime equivalence of any future rewrite. */
public final class CorrelationPlanProbe {
    private CorrelationPlanProbe() { }
    static final String STAGE = "Decorrelate inner queries 1";
    record Capture(List<ObjectNode> plans, List<ObjectNode> failures) { }

    static ObjectNode encode(String id, RelNode root, DBSPCompiler compiler,
                             S1BuiltinOperatorRegistry registry) throws IOException {
        List<RelNode> roots = List.of(root);
        UnaryOperator<RexNode> identity = UnaryOperator.identity();
        var catalog = S1Catalog.capture(S1ExpressionSharingProbe.collect(roots, identity));
        var captured = S1ExpressionSharingProbe.collect(roots, identity, catalog.values());
        var graph = S1ExpressionObjectsEncoder.encode(captured, registry,
                S1ExpressionCorpusProbe.source(compiler), catalog.types());
        var wire = Utilities.deterministicObjectMapper().createObjectNode().put("id", id);
        wire.set("table", graph.inputs().table()); wire.set("expressions", graph.expressions());
        wire.set("producers", S1InputProducers.encode(captured, graph.inputs()));
        wire.set("properties", S1RelationProperties.encodeShared(captured, graph.inputs(), graph.operators()));
        wire.set("catalog", S1Catalog.encode(catalog, graph));
        S1RelNodes.decodeCatalog(wire, registry);
        return wire;
    }

    static Capture capture(JsonNode input, S1BuiltinOperatorRegistry registry) throws IOException {
        var records = new ArrayList<ObjectNode>();
        var failures = new ArrayList<ObjectNode>();
        S1ExpressionCorpusProbe.compile(input, compiler ->
                compiler.getSqlToRelCompiler().optimizerObserver = new CalciteOptimizer.Observer() {
            private int invocation;

            @Override public void before(String step, RelNode plan) {
                if (!STAGE.equals(step)) return;
                int index = this.invocation++;
                if (!CalciteOptimizer.containsCorrelate(plan)) return;
                try {
                    records.add(encode(input.path("id").asText() + "/correlation-stage/" + index,
                            plan, compiler, registry));
                } catch (IOException error) {
                    // A transient form outside the lowering wire contract is
                    // part of the measured denominator. Leave Java's original
                    // plan intact and continue collecting later invocations.
                    failures.add(Utilities.deterministicObjectMapper().createObjectNode()
                            .put("invocation", index).put("error", error.toString()));
                }
            }

            @Override public void after(String step, RelNode plan, boolean completed) { }
        });
        return new Capture(List.copyOf(records), List.copyOf(failures));
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) throw new IOException("usage: CorrelationPlanProbe INPUT.jsonl OUTPUT_DIR");
        var mapper = Utilities.deterministicObjectMapper().copy()
                .enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        var output = Path.of(args[1]); Files.createDirectory(output);
        var registry = new S1BuiltinOperatorRegistry();
        var identities = new HashSet<String>();
        long cases = 0, fragments = 0, failures = 0;
        try (var reader = Files.newBufferedReader(Path.of(args[0]));
             var writer = Files.newBufferedWriter(output.resolve("cases.jsonl"), StandardOpenOption.CREATE_NEW);
             var coverage = Files.newBufferedWriter(output.resolve("coverage.jsonl"), StandardOpenOption.CREATE_NEW)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.length() > 64 * 1024 * 1024 || cases >= 20_000)
                    throw new IOException("Correlation input exceeds limit");
                var input = mapper.readTree(line);
                var identity = input.path("id");
                if (!identity.isTextual() || identity.asText().isEmpty()
                        || identity.asText().length() > 4096 || !identities.add(identity.asText()))
                    throw new IOException("Missing, overlarge or duplicate correlation case identity");
                Capture captured;
                try { captured = capture(input, registry); }
                catch (RuntimeException | IOException error) {
                    throw new IOException("Correlation FRel export failed for " + input.path("id"), error);
                }
                var row = mapper.createObjectNode(); row.set("id", input.path("id"));
                row.put("fragments", captured.plans().size());
                row.set("failures", mapper.valueToTree(captured.failures()));
                failures += captured.failures().size();
                for (var record: captured.plans()) { writer.write(record.toString()); writer.newLine(); fragments++; }
                writer.flush(); coverage.write(row.toString()); coverage.newLine(); coverage.flush(); cases++;
            }
        }
        if (cases == 0) throw new IOException("Empty correlation input");
        Files.writeString(output.resolve("complete.json"), mapper.createObjectNode()
                .put("complete", true).put("cases", cases).put("fragments", fragments)
                .put("transport_failures", failures).put("all_selected_plans_transported", failures == 0)
                .put("stage", STAGE).put("runtime_executed", false).toPrettyString() + "\n",
                StandardOpenOption.CREATE_NEW);
    }
}
