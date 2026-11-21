package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Compare each transported corpus record and reconstruct all arena entries.
 * The input is the bounded probe's own evidence, not arbitrary product input. */
public final class S1TypeCorpusImport {
    private S1TypeCorpusImport() { }

    public static void main(String[] args) throws IOException {
        if (args.length != 3)
            throw new IllegalArgumentException("Usage: S1TypeCorpusImport ORIGINAL.jsonl REWRITTEN.jsonl OUTPUT_DIR");
        for (int index = 0; index < 2; index++) {
            if (Files.size(Path.of(args[index])) > 1024L * 1024 * 1024)
                throw new IOException("Type corpus exceeds 1 GiB");
        }
        Path output = Path.of(args[2]);
        Files.createDirectory(output);
        var mapper = Utilities.deterministicObjectMapper();
        mapper.enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        long cases = 0;
        long roots = 0;
        long entries = 0;
        try (var before = Files.newBufferedReader(Path.of(args[0]));
             var after = Files.newBufferedReader(Path.of(args[1]))) {
            String line;
            while ((line = before.readLine()) != null) {
                String rewritten = after.readLine();
                if (rewritten == null || line.length() > 16 * 1024 * 1024 || rewritten.length() > 16 * 1024 * 1024)
                    throw new IOException("Missing or overlarge type corpus record");
                JsonNode expected = mapper.readTree(line);
                JsonNode actual = mapper.readTree(rewritten);
                if (!expected.equals(actual))
                    throw new IOException("Type corpus record changed: " + expected.path("id").asText());
                if (!actual.path("unsupported").isEmpty()
                        || actual.path("type_roots").intValue() != actual.path("supported_roots").intValue())
                    throw new IOException("Incomplete type support: " + actual.path("id").asText());
                var decoded = S1TypeTable.decode(actual.path("type_table"));
                if (decoded.roots().size() != actual.path("type_roots").intValue())
                    throw new IOException("Type root count changed: " + actual.path("id").asText());
                if (!mapper.readTree(S1TypeTable.encodeArena(decoded.types(), decoded.roots()).toString()).equals(actual.path("type_table")))
                    throw new IOException("Imported type arena changed fields or identity");
                cases++;
                roots += decoded.roots().size();
                entries += decoded.types().size();
            }
            if (after.readLine() != null || cases == 0)
                throw new IOException("Extra records or empty type corpus");
        }
        var complete = mapper.createObjectNode();
        complete.put("schema_version", 1);
        complete.put("complete", true);
        complete.put("cases", cases);
        complete.put("type_roots", roots);
        complete.put("arena_entries", entries);
        complete.put("SQL_types_imported", true);
        complete.put("runtime_executed", false);
        complete.put("S1_parity_proven", false);
        Files.writeString(output.resolve("complete.json"), complete.toPrettyString(), StandardOpenOption.CREATE_NEW);
    }
}
