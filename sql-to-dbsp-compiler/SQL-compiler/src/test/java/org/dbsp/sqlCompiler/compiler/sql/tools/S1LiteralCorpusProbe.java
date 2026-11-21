package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Census and direct round trip of optimized-view literals. Missing payload
 * forms remain explicit; unexpected compiler/adapter failures abort the run. */
public final class S1LiteralCorpusProbe {
    private S1LiteralCorpusProbe() { }

    private static boolean supported(RexLiteral literal) {
        if (literal.isNull())
            return true;
        if (literal.getTypeName() == SqlTypeName.SARG)
            return false;
        if (literal.getType().getIntervalQualifier() != null)
            return true;
        return switch (literal.getType().getSqlTypeName()) {
            case BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, UTINYINT, USMALLINT, UINTEGER, UBIGINT,
                    DECIMAL, REAL, FLOAT, DOUBLE, CHAR, VARCHAR, BINARY, VARBINARY, DATE, TIME,
                    TIMESTAMP, TIMESTAMP_TZ, GEOMETRY, UUID, SYMBOL -> true;
            default -> false;
        };
    }

    private static ObjectNode audit(JsonNode source) throws IOException {
        var mapper = Utilities.deterministicObjectMapper();
        var options = mapper.treeToValue(source.get("options"), CompilerOptions.class);
        options.ioOptions.inputCircuit = true;
        var compiler = new DBSPCompiler(options);
        JsonNode submissions = source.get("submissions");
        if (submissions == null || !submissions.isArray() || submissions.isEmpty())
            throw new IOException("Missing ordered SQL submissions");
        for (JsonNode submission: submissions) {
            if (submission.path("batch").intValue() != 0)
                throw new IOException("Multiple compilation batches require explicit replay");
            if (submission.path("many").booleanValue())
                compiler.submitStatementsForCompilation(submission.path("sql").textValue());
            else
                compiler.submitStatementForCompilation(submission.path("sql").textValue());
        }
        if (compiler.getFinalCircuit(false) == null)
            throw new IOException("Corpus case produced no circuit: " + source.path("id"));
        compiler.throwIfErrorsOccurred();
        var capture = S1TypeCorpusProbe.literals(compiler);
        var literals = capture.literals();
        var result = mapper.createObjectNode();
        result.set("id", source.get("id"));
        result.put("expanded_search_calls", capture.expandedSearchCalls());
        result.set("type_table", S1TypeTable.encode(literals.stream().map(RexLiteral::getType).toList()));
        var imported = S1TypeTable.decode(mapper.readTree(result.path("type_table").toString()));
        var records = result.putArray("literals");
        for (int index = 0; index < literals.size(); index++) {
            RexLiteral literal = literals.get(index);
            ObjectNode record = records.addObject();
            record.put("index", index);
            record.put("rex_type", literal.getTypeName().name());
            record.put("sql_type", literal.getType().getFullTypeString());
            record.put("value_class", literal.getValue() == null ? "null" : literal.getValue().getClass().getName());
            ObjectNode literalNode = record.putObject("literal");
            literalNode.set("ty", result.at("/type_table/roots/" + index));
            literalNode.putObject("origin").put("kind", "absent");
            if (supported(literal)) {
                ObjectNode payload = S1LiteralPayload.encode(literal);
                RexLiteral decoded = S1LiteralPayload.decode(mapper.readTree(payload.toString()), imported.roots().get(index));
                if (!literal.equals(decoded) || !literal.getType().equals(decoded.getType())
                        || !mapper.readTree(payload.toString()).equals(mapper.readTree(S1LiteralPayload.encode(decoded).toString())))
                    throw new IOException("Literal/type round trip differs for " + source.path("id") + "/" + index);
                record.put("status", "supported");
                literalNode.set("value", payload);
            } else {
                record.put("status", "unsupported");
            }
        }
        return result;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2)
            throw new IllegalArgumentException("Usage: S1LiteralCorpusProbe INPUT.jsonl OUTPUT_DIR");
        Path output = Path.of(args[1]);
        Files.createDirectory(output);
        var mapper = Utilities.deterministicObjectMapper();
        mapper.enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        long cases = 0, total = 0, unsupported = 0, expanded = 0;
        try (var reader = Files.newBufferedReader(Path.of(args[0]));
             var writer = Files.newBufferedWriter(output.resolve("cases.jsonl"), StandardOpenOption.CREATE_NEW)) {
            String line;
            while ((line = reader.readLine()) != null) {
                ObjectNode record = audit(mapper.readTree(line));
                writer.write(record.toString());
                writer.newLine();
                writer.flush();
                cases++;
                expanded += record.path("expanded_search_calls").longValue();
                total += record.path("literals").size();
                for (JsonNode literal: record.path("literals"))
                    if (literal.path("status").textValue().equals("unsupported"))
                        unsupported++;
            }
        }
        var complete = mapper.createObjectNode();
        complete.put("schema_version", 1);
        complete.put("complete", true);
        complete.put("cases", cases);
        complete.put("literals", total);
        complete.put("unsupported_literals", unsupported);
        complete.put("expanded_search_calls", expanded);
        complete.put("runtime_executed", false);
        complete.put("S1_parity_proven", false);
        Files.writeString(output.resolve("complete.json"), complete.toPrettyString(), StandardOpenOption.CREATE_NEW);
    }
}
