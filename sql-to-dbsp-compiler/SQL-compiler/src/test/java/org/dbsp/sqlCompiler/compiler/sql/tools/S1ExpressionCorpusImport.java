package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;

/** Check each transported contextual expression fragment against its captured wire
 * graph and reconstruct its Rex arena. This does not replay whole-program S1. */
public final class S1ExpressionCorpusImport {
    private S1ExpressionCorpusImport() { }

    public static void main(String[] args) throws IOException {
        if (args.length != 3) throw new IOException("usage: S1ExpressionCorpusImport EXPECTED.jsonl IMPORTED.jsonl OUTPUT_DIR");
        var mapper = Utilities.deterministicObjectMapper().copy();
        mapper.enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        mapper.enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        for (int index = 0; index < 2; index++)
            if (Files.size(Path.of(args[index])) > 1024L * 1024 * 1024)
                throw new IOException("Expression corpus exceeds 1 GiB");
        var registry = new S1BuiltinOperatorRegistry();
        var identities = new HashSet<String>();
        String scope = null;
        long fragments = 0, expressions = 0, roots = 0;
        try (var expected = Files.newBufferedReader(Path.of(args[0]));
             var imported = Files.newBufferedReader(Path.of(args[1]))) {
            String line;
            while ((line = expected.readLine()) != null) {
                String transported = imported.readLine();
                if (transported == null) throw new IOException("Missing transported expression case");
                if (line.length() > 16 * 1024 * 1024 || transported.length() > 16 * 1024 * 1024)
                    throw new IOException("Expression case exceeds line limit");
                var original = mapper.readTree(line);
                var restored = mapper.readTree(transported);
                fields(restored, "id", "scope", "fragments");
                String capturedScope = restored.path("scope").asText();
                if (!java.util.Set.of("optimized_projects", "local_fields").contains(capturedScope)
                        || (scope != null && !scope.equals(capturedScope)))
                    throw new IOException("Unknown or mixed expression capture scope");
                scope = capturedScope;
                if (!original.equals(restored)) throw new IOException("Transported expression case changed: " + original.path("id"));
                String id = original.path("id").textValue();
                if (id == null || id.isEmpty() || id.length() > 4096 || !identities.add(id) || identities.size() > 20_000)
                    throw new IOException("Invalid or duplicate expression case identity");
                int previous = -1;
                if (!restored.path("fragments").isArray()) throw new IOException("Missing expression fragments");
                for (var fragment: restored.path("fragments")) {
                    fields(fragment, "relation_ordinal", "table");
                    var ordinal = fragment.path("relation_ordinal");
                    if (!ordinal.isIntegralNumber() || !ordinal.canConvertToInt() || ordinal.intValue() <= previous)
                        throw new IOException("Invalid or unordered fragment ordinals");
                    previous = ordinal.intValue();
                    var decoded = S1ExpressionTable.decode(fragment.path("table"), registry);
                    fragments++; expressions += decoded.expressions().size(); roots += decoded.roots().size();
                }
            }
            if (imported.readLine() != null || identities.isEmpty()) throw new IOException("Extra or empty expression corpus");
        }
        Path output = Path.of(args[2]); Files.createDirectory(output);
        var complete = mapper.createObjectNode();
        complete.put("schema_version", 1).put("complete", true).put("cases", identities.size()).put("fragments", fragments)
                .put("expressions", expressions).put("roots", roots).put("scope", scope)
                .put("runtime_executed", false).put("S1_parity_proven", false);
        Files.writeString(output.resolve("complete.json"), complete.toPrettyString(), StandardOpenOption.CREATE_NEW);
    }

    private static void fields(com.fasterxml.jackson.databind.JsonNode node, String... names) throws IOException {
        if (!node.isObject()) throw new IOException("Missing expression corpus object");
        var actual = new HashSet<String>(); node.fieldNames().forEachRemaining(actual::add);
        if (!actual.equals(java.util.Set.of(names))) throw new IOException("Unknown or missing expression corpus fields");
    }
}
