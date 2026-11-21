package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Reconstruct literal values and their type identities after Rust transport. */
public final class S1LiteralCorpusImport {
    private S1LiteralCorpusImport() { }

    public static void main(String[] args) throws IOException {
        if (args.length != 3)
            throw new IllegalArgumentException("Usage: S1LiteralCorpusImport ORIGINAL.jsonl REWRITTEN.jsonl OUTPUT_DIR");
        for (int i = 0; i < 2; i++)
            if (Files.size(Path.of(args[i])) > 1024L * 1024 * 1024)
                throw new IOException("Literal corpus exceeds 1 GiB");
        Path output = Path.of(args[2]);
        Files.createDirectory(output);
        var mapper = Utilities.deterministicObjectMapper();
        mapper.enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        long cases = 0, literals = 0, expanded = 0;
        try (var before = Files.newBufferedReader(Path.of(args[0]));
             var after = Files.newBufferedReader(Path.of(args[1]))) {
            String line;
            while ((line = before.readLine()) != null) {
                String rewritten = after.readLine();
                if (rewritten == null || line.length() > 16 * 1024 * 1024 || rewritten.length() > 16 * 1024 * 1024)
                    throw new IOException("Missing or overlarge literal record");
                JsonNode expected = mapper.readTree(line);
                JsonNode actual = mapper.readTree(rewritten);
                if (!expected.equals(actual))
                    throw new IOException("Literal corpus record changed: " + expected.path("id"));
                var types = S1TypeTable.decode(actual.path("type_table"));
                JsonNode records = actual.path("literals");
                if (!records.isArray() || records.size() != types.roots().size())
                    throw new IOException("Literal/type root count mismatch");
                for (int index = 0; index < records.size(); index++) {
                    JsonNode record = records.get(index);
                    if (record.path("index").intValue() != index || !record.path("status").asText().equals("supported")
                            || record.path("rex_type").asText().equals("SARG"))
                        throw new IOException("Incomplete or reordered literal record");
                    JsonNode literal = record.path("literal");
                    if (!literal.path("origin").equals(mapper.createObjectNode().put("kind", "absent")))
                        throw new IOException("Unsupported literal source origin");
                    if (!literal.path("ty").equals(actual.at("/type_table/roots/" + index)))
                        throw new IOException("Literal type identity changed");
                    var decoded = S1LiteralPayload.decode(literal.path("value"), types.roots().get(index));
                    if (decoded.getType() != types.roots().get(index)
                            || !decoded.getType().getFullTypeString().equals(record.path("sql_type").asText())
                            || !decoded.getTypeName().name().equals(record.path("rex_type").asText()))
                        throw new IOException("Imported literal type/storage changed for " + actual.path("id").asText() + "/" + index
                                + ": shared=" + (decoded.getType() == types.roots().get(index))
                                + ", actual=" + decoded.getType().getFullTypeString() + "/" + decoded.getTypeName()
                                + ", expected=" + record.path("sql_type").asText() + "/" + record.path("rex_type").asText());
                    String valueClass = decoded.getValue() == null ? "null" : decoded.getValue().getClass().getName();
                    if (!valueClass.equals(record.path("value_class").asText())
                            || !mapper.readTree(S1LiteralPayload.encode(decoded).toString()).equals(literal.path("value")))
                        throw new IOException("Imported literal payload changed");
                }
                cases++;
                literals += records.size();
                expanded += actual.path("expanded_search_calls").longValue();
            }
            if (after.readLine() != null || cases == 0)
                throw new IOException("Extra records or empty literal corpus");
        }
        var complete = mapper.createObjectNode();
        complete.put("schema_version", 1);
        complete.put("complete", true);
        complete.put("cases", cases);
        complete.put("literals", literals);
        complete.put("expanded_search_calls", expanded);
        complete.put("runtime_executed", false);
        complete.put("S1_parity_proven", false);
        Files.writeString(output.resolve("complete.json"), complete.toPrettyString(), StandardOpenOption.CREATE_NEW);
    }
}
