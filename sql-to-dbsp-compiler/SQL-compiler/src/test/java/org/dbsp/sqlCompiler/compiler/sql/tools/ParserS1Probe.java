package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;

/** Capture one side of an ordered semantic shard in its own JVM.
 * Matching original/emitted JVMs see the same case order and cache lifetime.
 * Every program passes strict decoding before semantic projection. */
public final class ParserS1Probe {
    private ParserS1Probe() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 3 || !(args[0].equals("original") || args[0].equals("emitted")))
            throw new IOException("usage: ParserS1Probe original|emitted INPUT.jsonl OUTPUT_DIR");
        var mapper = Utilities.deterministicObjectMapper();
        var registry = new S1BuiltinOperatorRegistry();
        var output = Path.of(args[2]);
        Files.createDirectory(output);
        var ids = new HashSet<String>();
        try (var reader = Files.newBufferedReader(Path.of(args[1]));
             var writer = Files.newBufferedWriter(output.resolve("cases.jsonl"), StandardOpenOption.CREATE_NEW)) {
            String line;
            while ((line = reader.readLine()) != null) {
                var input = mapper.readTree(line);
                String id = input.path("id").asText();
                if (id.isBlank() || !ids.add(id)) throw new IOException("Missing or repeated pair identity");
                com.fasterxml.jackson.databind.node.ObjectNode record;
                try {
                    record = S1Program.capture(input.path(args[0]), registry);
                    if (!record.path("id").asText().equals(id))
                        throw new IOException("S1 case identity differs from its pair");
                    S1Program.decode(record, registry);
                } catch (Exception error) {
                    throw new IOException("S1 parser semantic capture failed for " + id, error);
                }
                writer.write(record.toString());
                writer.newLine();
                writer.flush();
            }
        }
        if (ids.isEmpty()) throw new IOException("Empty S1 semantic input");
        var complete = mapper.createObjectNode().put("complete", true).put("cases", ids.size())
                .put("side", args[0]).put("strict_decode_verified", true)
                .put("scope", "Complete S1 programs in a separate JVM per ordered side; no semantic comparison yet");
        Files.writeString(output.resolve("complete.json"), complete + "\n", StandardOpenOption.CREATE_NEW);
    }
}
