package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.util.Utilities;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;

/** Independently reconstruct the input-object arena and all evaluation uses. */
public final class S1InputUsesImport {
    public static void main(String[] args) throws IOException {
        if (args.length != 3) throw new IOException("usage: S1InputUsesImport EXPECTED IMPORTED OUTPUT");
        var mapper = Utilities.deterministicObjectMapper().copy()
                .enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                .enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        for (int index = 0; index < 2; index++)
            if (Files.size(Path.of(args[index])) > 1024L * 1024 * 1024) throw new IOException("Input-use corpus too large");
        var identities = new HashSet<String>();
        var registry = new S1BuiltinOperatorRegistry();
        long objects = 0, uses = 0, environments = 0;
        try (var expected = Files.newBufferedReader(Path.of(args[0]));
             var imported = Files.newBufferedReader(Path.of(args[1]))) {
            String line;
            while ((line = expected.readLine()) != null) {
                String other = imported.readLine();
                if (other == null || line.length() > 16 * 1024 * 1024 || other.length() > 16 * 1024 * 1024)
                    throw new IOException("Missing or oversized input-use record");
                var original = mapper.readTree(line); var restored = mapper.readTree(other);
                var names = new HashSet<String>(); restored.fieldNames().forEachRemaining(names::add);
                var expectedNames = new HashSet<>(java.util.Set.of("id", "table", "producers", "properties"));
                if (restored.has("expressions")) expectedNames.add("expressions");
                if (restored.has("catalog") && restored.has("expressions")) expectedNames.add("catalog");
                if (!names.equals(expectedNames) || !original.equals(restored))
                    throw new IOException("Input-use record changed");
                String id = restored.path("id").textValue();
                if (id == null || id.isEmpty() || id.length() > 4096 || !identities.add(id) || identities.size() > 20_000)
                    throw new IOException("Invalid or duplicate input-use case");
                var decoded = S1InputUsesTable.decode(restored.path("table"));
                S1InputProducers.validate(restored.path("producers"), restored.path("table"));
                S1RelationProperties.validateOperators(restored.path("properties"), restored.path("table"), registry);
                if (restored.has("expressions"))
                    S1ExpressionObjectsTable.decode(restored.path("expressions"), restored.path("table"), registry);
                int relationIndex = 0;
                for (var relation: restored.at("/properties/relations")) {
                    int row = restored.at("/producers/relations").get(relationIndex++).path("row").intValue();
                    int width = restored.at("/table/type_table/types").get(row).at("/kind/fields").size();
                    S1RelationMetadata.decode(relation.path("metadata"), width);
                }
                objects += decoded.objects().size(); uses += decoded.uses().size();
                environments += restored.at("/table/environments").size();
            }
            if (imported.readLine() != null || identities.isEmpty()) throw new IOException("Extra or empty input-use corpus");
        }
        Path output = Path.of(args[2]); Files.createDirectory(output);
        var result = mapper.createObjectNode().put("complete", true).put("cases", identities.size())
                .put("objects", objects).put("uses", uses).put("environments", environments)
                .put("S1_parity_proven", false);
        Files.writeString(output.resolve("complete.json"), result.toPrettyString());
    }
}
