package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.util.Utilities;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** Constructor audit over Rust-validated records. Scan tables are explicit
 * placeholders: this measures relation reconstruction, not catalog replay or S2. */
public final class S1RelNodesProbe {
    public static void main(String[] args) throws IOException {
        if (args.length != 2) throw new IOException("usage: S1RelNodesProbe VALIDATED.jsonl OUTPUT_DIR");
        var mapper = Utilities.deterministicObjectMapper().copy()
                .enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                .enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        var registry = new S1BuiltinOperatorRegistry();
        long cases = 0, relations = 0;
        try (var reader = Files.newBufferedReader(Path.of(args[0]))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.length() > 16 * 1024 * 1024 || ++cases > 20_000) throw new IOException("Relation corpus exceeds limit");
                var record = mapper.readTree(line);
                try {
                    var decoded = S1RelNodes.decode(record, registry, (id, name, row) ->
                            org.apache.calcite.prepare.RelOptTableImpl.create(null, row, name,
                                    (org.apache.calcite.linq4j.tree.Expression) null));
                    relations += decoded.nodes().size();
                } catch (RuntimeException | IOException error) {
                    throw new IOException("Relation reconstruction failed for " + record.path("id"), error);
                }
            }
        }
        if (cases == 0) throw new IOException("Empty relation corpus");
        var output = Path.of(args[1]); Files.createDirectory(output);
        Files.writeString(output.resolve("complete.json"), mapper.createObjectNode().put("complete", true)
                .put("cases", cases).put("relations", relations).put("catalog_reconstructed", false)
                .put("S1_parity_proven", false).toPrettyString());
    }
}
