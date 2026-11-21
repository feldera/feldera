package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/** Import/export audit of catalog-backed optimized-view relations. This does not
 * replay the whole statement stream or prove S1-to-S2 parity. */
public final class S1CatalogProbe {
    private S1CatalogProbe() { }

    static ObjectNode capture(JsonNode input, S1BuiltinOperatorRegistry registry) throws IOException {
        var compiler = S1ExpressionCorpusProbe.compile(input);
        var plans = compiler.getPlans(new org.dbsp.util.IndentStream(new StringBuilder()));
        var roots = plans.entrySet().stream().sorted(Comparator.comparingInt(java.util.Map.Entry::getValue))
                .map(java.util.Map.Entry::getKey).toList();
        var normalize = new S1ExpressionNormalizer(compiler.getSqlToRelCompiler().getRexBuilder());
        var catalog = S1Catalog.capture(S1ExpressionSharingProbe.collect(roots, normalize));
        var captured = S1ExpressionSharingProbe.collect(roots, normalize, catalog.values());
        var graph = S1ExpressionObjectsEncoder.encode(captured, registry, S1ExpressionCorpusProbe.source(compiler), catalog.types());
        var wire = Utilities.deterministicObjectMapper().createObjectNode(); wire.set("id", input.path("id"));
        wire.set("table", graph.inputs().table()); wire.set("expressions", graph.expressions());
        wire.set("producers", S1InputProducers.encode(captured, graph.inputs()));
        wire.set("properties", S1RelationProperties.encode(captured, graph.inputs(), registry));
        wire.set("catalog", S1Catalog.encode(catalog, graph));
        return wire;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3 || (!args[0].equals("export") && !args[0].equals("import")))
            throw new IOException("usage: S1CatalogProbe export|import INPUT.jsonl OUTPUT_DIR");
        boolean exporting = args[0].equals("export");
        var output = Path.of(args[2]); Files.createDirectory(output);
        var mapper = Utilities.deterministicObjectMapper().copy().enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        var registry = new S1BuiltinOperatorRegistry();
        long cases = 0, schemas = 0, tables = 0, scans = 0;
        try (var reader = Files.newBufferedReader(Path.of(args[1])); var writer = Files.newBufferedWriter(output.resolve("cases.jsonl"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                var input = mapper.readTree(line);
                ObjectNode wire;
                try {
                    wire = exporting ? capture(input, registry) : (ObjectNode) input;
                    S1RelNodes.decodeCatalog(wire, registry);
                } catch (RuntimeException | IOException error) {
                    throw new IOException("Catalog transport failed for " + input.path("id"), error);
                }
                writer.write(wire.toString()); writer.newLine(); writer.flush();
                cases++; schemas += wire.at("/catalog/schemas").size(); tables += wire.at("/catalog/tables").size(); scans += wire.at("/catalog/scans").size();
            }
        }
        if (cases == 0) throw new IOException("Empty catalog corpus");
        var complete = mapper.createObjectNode().put("complete", true).put("cases", cases).put("schemas", schemas)
                .put("tables", tables).put("scans", scans).put("S1_parity_proven", false)
                .put("scope", "catalog-backed optimized-view relations");
        Files.writeString(output.resolve("complete.json"), complete.toPrettyString());
    }
}
