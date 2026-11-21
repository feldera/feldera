package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/** Fixed-fixture literal round trip. Typed mode imports a one-entry scalar type
 * table; neither mode certifies complete S1 type or relational transport. */
public final class S1LiteralProbe {
    private S1LiteralProbe() { }

    private static ObjectNode row(int index, RexLiteral literal, boolean typed, RexBuilder builder) throws IOException {
        var mapper = Utilities.deterministicObjectMapper();
        ObjectNode row = mapper.createObjectNode();
        row.put("id", index);
        row.put("type_description", literal.getType().getFullTypeString());
        if (typed)
            row.set("type_table", S1ScalarType.exportType(literal.getType(), builder.getTypeFactory()));
        ObjectNode value = row.putObject("literal");
        value.put("ty", 0);
        value.putObject("origin").put("kind", "absent");
        value.set("value", S1LiteralPayload.encode(literal));
        return row;
    }

    public static void main(String[] args) throws IOException {
        boolean typed = args.length > 0 && (args[0].equals("export-typed") || args[0].equals("verify-typed"));
        boolean exporting = args.length == 2 && (args[0].equals("export") || args[0].equals("export-typed"));
        boolean verifying = args.length == 3 && (args[0].equals("verify") || args[0].equals("verify-typed"));
        if (!exporting && !verifying)
            throw new IllegalArgumentException("Usage: S1LiteralProbe export[-typed] OUTPUT_DIR | verify[-typed] INPUT.jsonl OUTPUT_DIR");
        Path output = Path.of(args[args.length - 1]);
        Files.createDirectory(output);
        var mapper = Utilities.deterministicObjectMapper();
        mapper.enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        var builder = new RexBuilder(S1LiteralPayloadTests.factory());
        List<RexLiteral> literals = S1LiteralPayloadTests.fixtures(builder);
        if (exporting) {
            try (var writer = Files.newBufferedWriter(output.resolve("cases.jsonl"), StandardOpenOption.CREATE_NEW)) {
                for (int index = 0; index < literals.size(); index++) {
                    writer.write(row(index, literals.get(index), typed, builder).toString());
                    writer.newLine();
                }
            }
        } else {
            try (var reader = Files.newBufferedReader(Path.of(args[1]))) {
                for (int index = 0; index < literals.size(); index++) {
                    String line = reader.readLine();
                    if (line == null)
                        throw new IOException("Missing literal fixture " + index);
                    JsonNode actual = mapper.readTree(line);
                    RexLiteral expected = literals.get(index);
                    // Compare parsed JSON values, independent of Jackson Int/Long/BigInteger node classes.
                    if (!actual.equals(mapper.readTree(row(index, expected, typed, builder).toString())))
                        throw new IOException("Literal fixture payload changed: " + index);
                    var importedType = typed
                            ? S1ScalarType.decode(actual.path("type_table"), builder.getTypeFactory())
                            : expected.getType();
                    RexLiteral decoded = S1LiteralPayload.decode(actual.path("literal").path("value"),
                            importedType);
                    if (!decoded.equals(expected) || !decoded.getType().equals(expected.getType()))
                        throw new IOException("Imported literal value/type changed: " + index);
                }
                if (reader.readLine() != null)
                    throw new IOException("Unexpected extra literal fixture");
            }
        }
        ObjectNode complete = mapper.createObjectNode();
        complete.put("schema_version", 1);
        complete.put("complete", true);
        complete.put("cases", literals.size());
        complete.put("mode", args[0]);
        complete.put("SQL_types_imported", typed);
        complete.put("runtime_executed", false);
        Files.writeString(output.resolve("complete.json"), complete.toPrettyString(), StandardOpenOption.CREATE_NEW);
    }
}
