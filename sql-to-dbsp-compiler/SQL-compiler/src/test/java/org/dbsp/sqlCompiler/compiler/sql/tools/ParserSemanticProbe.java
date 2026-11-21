package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.sql.SqlNode;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/** Compare independently parsed ASTs and bound schemas for original and AST-emitted SQL.
 * Source ranges are retained separately. This does not claim full S1 equivalence. */
public final class ParserSemanticProbe {
    private ParserSemanticProbe() { }

    private record Parsed(List<SqlNode> nodes, ArrayNode ranges) { }

    private static final class ComparisonFailure extends Exception {
        final String phase;
        ComparisonFailure(String phase, Exception cause) {
            super(phase, cause);
            this.phase = phase;
        }
    }

    @FunctionalInterface
    private interface CheckedOperation<T> { T run() throws Exception; }

    private static <T> T phase(String phase, CheckedOperation<T> operation) throws ComparisonFailure {
        try { return operation.run(); }
        catch (Exception error) { throw new ComparisonFailure(phase, error); }
    }

    private static Parsed parse(JsonNode input) throws Exception {
        var mapper = Utilities.deterministicObjectMapper();
        var compiler = new DBSPCompiler(mapper.treeToValue(input.path("options"), CompilerOptions.class));
        var nodes = new ArrayList<SqlNode>();
        var ranges = mapper.createArrayNode();
        for (var submission: input.path("submissions")) {
            List<SqlNode> parsed;
            if (submission.path("many").asBoolean() && submission.path("sql").asText().isEmpty()) {
                compiler.getSqlToRelCompiler().emptyStatement();
                parsed = List.of();
            } else if (submission.path("many").asBoolean()) {
                parsed = compiler.getSqlToRelCompiler().parseStatements(
                        submission.path("sql").textValue(), true, false).stream()
                        .map(statement -> statement.statement()).toList();
            } else {
                parsed = List.of(compiler.getSqlToRelCompiler().parse(
                        submission.path("sql").textValue(), true, false));
            }
            for (var node: parsed) {
                nodes.add(node);
                var range = node.getParserPosition();
                var position = ranges.addObject();
                position.put("line", range.getLineNum()).put("column", range.getColumnNum())
                        .put("end_line", range.getEndLineNum()).put("end_column", range.getEndColumnNum());
            }
        }
        return new Parsed(nodes, ranges);
    }

    private static ArrayNode diagnosticContent(DBSPCompiler compiler) {
        var result = Utilities.deterministicObjectMapper().createArrayNode();
        for (var message: compiler.messages.toJson(compiler.sources)) {
            var content = result.addObject();
            for (String field: List.of("warning", "error_type", "message"))
                content.set(field, message.get(field));
        }
        return result;
    }

    private record SchemaContent(JsonNode content, ArrayNode propertyRanges) { }

    /** Properties.asJson stores source ranges beside property values. Extract
     * only these two documented fields; retain full metadata in the result. */
    private static SchemaContent schemaContent(JsonNode schema) throws IOException {
        JsonNode content = schema.deepCopy();
        var ranges = Utilities.deterministicObjectMapper().createArrayNode();
        for (String group: List.of("inputs", "outputs")) {
            int table = 0;
            for (var relation: content.path(group)) {
                var properties = relation.path("properties");
                var entries = properties.fields();
                while (entries.hasNext()) {
                    var entry = entries.next();
                    if (!(entry.getValue() instanceof ObjectNode property))
                        throw new IOException("Expected structured schema property");
                    var record = ranges.addObject().put("group", group).put("relation", table)
                            .put("property", entry.getKey());
                    for (String field: List.of("key_position", "value_position")) {
                        var position = property.get(field);
                        if (position == null || !position.isObject() || position.size() != 4)
                            throw new IOException("Unexpected schema property range");
                        for (String coordinate: List.of("start_line_number", "start_column",
                                "end_line_number", "end_column"))
                            if (!position.path(coordinate).isIntegralNumber())
                                throw new IOException("Unexpected schema property coordinate");
                        record.set(field, property.remove(field));
                    }
                }
                table++;
            }
        }
        return new SchemaContent(content, ranges);
    }

    static void requireUserErrors(DBSPCompiler compiler) throws IOException {
        for (var message: compiler.messages.messages) {
            if (!message.warning && !List.of("Not supported", "Compilation error",
                    "Error in SQL statement", "Error parsing SQL").contains(message.errorType))
                throw new IOException("Unexpected compiler failure: " + message.errorType + ": " + message.message);
        }
    }

    private static DBSPCompiler bind(JsonNode input) throws IOException {
        var options = Utilities.deterministicObjectMapper().treeToValue(input.path("options"), CompilerOptions.class);
        options.ioOptions.inputCircuit = true;
        var compiler = new DBSPCompiler(options);
        for (var submission: input.path("submissions")) {
            if (submission.path("batch").asInt() != 0)
                throw new IOException("Semantic probe needs explicit support for multiple compilation batches");
            if (submission.path("many").asBoolean())
                compiler.submitStatementsForCompilation(submission.path("sql").textValue());
            else compiler.submitStatementForCompilation(submission.path("sql").textValue());
        }
        var circuit = compiler.getFinalCircuit(false);
        requireUserErrors(compiler);
        if (!compiler.hasErrors() && circuit == null) throw new IOException("Accepted program has no circuit");
        return compiler;
    }

    static ObjectNode compare(JsonNode input) throws Exception {
        return compare(input, false);
    }

    static ObjectNode compareAst(JsonNode input) throws Exception {
        return compare(input, true);
    }

    private static ObjectNode compare(JsonNode input, boolean astOnly) throws Exception {
        var mapper = Utilities.deterministicObjectMapper();
        ObjectNode original = input.path("original").deepCopy();
        ObjectNode emitted = input.path("emitted").deepCopy();
        if (!original.has("options")) original.set("options", mapper.valueToTree(new CompilerOptions()));
        if (!emitted.has("options")) emitted.set("options", mapper.valueToTree(new CompilerOptions()));
        if (!original.get("options").equals(emitted.get("options")))
            throw new IOException("Original and emitted compiler options differ");
        var before = phase("parse_original", () -> parse(original));
        var after = phase("parse_emitted", () -> parse(emitted));
        var record = mapper.createObjectNode();
        record.set("id", input.path("id"));
        var statements = record.putArray("statements");
        boolean astEqual = before.nodes().size() == after.nodes().size();
        for (int index = 0; index < Math.min(before.nodes().size(), after.nodes().size()); index++) {
            var left = before.nodes().get(index);
            var right = after.nodes().get(index);
            boolean equal = ParserAstEquality.equal(left, right);
            astEqual &= equal;
            var statement = statements.addObject();
            statement.put("index", index).put("ast_equal", equal)
                    .put("original_node", left.getClass().getSimpleName())
                    .put("emitted_node", right.getClass().getSimpleName());
            renderForDiagnostics(statement, "original_sql", left);
            renderForDiagnostics(statement, "emitted_sql", right);
        }
        record.put("original_statement_count", before.nodes().size())
                .put("emitted_statement_count", after.nodes().size());
        record.set("original_ranges", before.ranges());
        record.set("emitted_ranges", after.ranges());
        record.put("statement_ranges_equal", before.ranges().equals(after.ranges()));
        record.put("ast_equal", astEqual).put("binding_executed", !astOnly);
        if (astOnly) {
            record.put("equal", astEqual);
            return record;
        }
        // Separate compilers replay the complete original/emitted catalog history.
        var originalCompiler = phase("bind_original", () -> bind(original));
        var emittedCompiler = phase("bind_emitted", () -> bind(emitted));
        var originalSchema = originalCompiler.metadata.asJson();
        var emittedSchema = emittedCompiler.metadata.asJson();
        record.set("original_schema", originalSchema);
        record.set("emitted_schema", emittedSchema);
        record.set("original_diagnostics", originalCompiler.messages.toJson(originalCompiler.sources));
        record.set("emitted_diagnostics", emittedCompiler.messages.toJson(emittedCompiler.sources));
        boolean diagnosticsEqual = diagnosticContent(originalCompiler).equals(diagnosticContent(emittedCompiler));
        boolean originalAccepted = !originalCompiler.hasErrors();
        boolean emittedAccepted = !emittedCompiler.hasErrors();
        boolean expectedAccepted = switch (original.path("binding_expectation").asText("accepted")) {
            case "accepted" -> true;
            case "rejected" -> false;
            default -> throw new IOException("Unknown binding expectation");
        };
        boolean expected = originalAccepted == expectedAccepted;
        if (!expectedAccepted) {
            String kind = original.path("expected_error_type").asText();
            if (kind.isBlank()) throw new IOException("Expected binding rejection requires an error class");
            expected &= originalCompiler.messages.messages.stream()
                    .anyMatch(message -> !message.warning && message.errorType.equals(kind));
        }
        boolean schemaEqual = originalSchema.equals(emittedSchema);
        var originalContent = schemaContent(originalSchema);
        var emittedContent = schemaContent(emittedSchema);
        boolean schemaContentEqual = originalContent.content().equals(emittedContent.content());
        record.set("original_property_ranges", originalContent.propertyRanges());
        record.set("emitted_property_ranges", emittedContent.propertyRanges());
        record.put("property_ranges_equal", originalContent.propertyRanges().equals(emittedContent.propertyRanges()));
        record.put("original_accepted", originalAccepted).put("emitted_accepted", emittedAccepted)
                .put("binding_expectation_met", expected).put("ast_equal", astEqual)
                .put("diagnostic_content_equal", diagnosticsEqual);
        if (originalAccepted && emittedAccepted)
            record.put("schema_equal", schemaEqual).put("schema_content_equal", schemaContentEqual);
        else { record.putNull("schema_equal"); record.putNull("schema_content_equal"); }
        record.put("equal", expected && astEqual && originalAccepted == emittedAccepted
                && (!originalAccepted || schemaContentEqual) && diagnosticsEqual);
        return record;
    }

    public static void main(String[] args) throws Exception {
        boolean continueAfterFailure = false;
        boolean astOnly = false;
        if (args.length < 2) throw new IOException("usage: ParserSemanticProbe INPUT.jsonl OUTPUT_DIR [--continue-after-failure] [--ast-only]");
        for (int i = 2; i < args.length; i++) {
            if (args[i].equals("--continue-after-failure") && !continueAfterFailure) continueAfterFailure = true;
            else if (args[i].equals("--ast-only") && !astOnly) astOnly = true;
            else throw new IOException("Unknown or repeated argument: " + args[i]);
        }
        var mapper = Utilities.deterministicObjectMapper();
        var output = Path.of(args[1]); Files.createDirectory(output);
        var identities = new HashSet<String>();
        long mismatches = 0;
        long accepted = 0;
        long comparisonFailures = 0;
        try (var input = Files.newBufferedReader(Path.of(args[0]));
             var writer = Files.newBufferedWriter(output.resolve("cases.jsonl"), StandardOpenOption.CREATE_NEW)) {
            String line;
            while ((line = input.readLine()) != null) {
                var source = mapper.readTree(line);
                String id = source.path("id").asText();
                if (id.isBlank() || !identities.add(id)) throw new IOException("Missing or duplicate identity");
                ObjectNode record;
                try { record = compare(source, astOnly); }
                catch (Exception error) {
                    if (!continueAfterFailure)
                        throw new IOException("Semantic parser probe failed for " + id, error);
                    comparisonFailures++;
                    record = comparisonFailure(id, error);
                }
                if (!record.path("equal").asBoolean()) mismatches++;
                if (record.path("original_accepted").asBoolean()) accepted++;
                writer.write(record.toString()); writer.newLine(); writer.flush();
            }
        }
        if (identities.isEmpty()) throw new IOException("Empty semantic input");
        var complete = mapper.createObjectNode().put("schema_version", 1).put("complete", true)
                .put("cases", identities.size()).put("mismatches", mismatches)
                .put("comparison_failures", comparisonFailures)
                .put("binding_executed", !astOnly)
                .put("scope", astOnly ? "Java parsed AST equality only; grammar fixtures need not bind; source ranges separate"
                        : "Java parsed AST equality, bound schemas and diagnostic content; source ranges separate")
                .put("full_s1_equivalence_checked", false);
        if (!astOnly) complete.put("original_accepted", accepted)
                .put("original_rejected", identities.size() - accepted - comparisonFailures);
        Files.writeString(output.resolve("complete.json"), complete + "\n", StandardOpenOption.CREATE_NEW);
        if (mismatches != 0) throw new IOException("Semantic parser comparisons differ: " + mismatches);
    }

    /** A captured failure is always a mismatch, never a matching SQL rejection.
     * Continue mode inventories the remaining inputs; its process still fails. */
    /** Pretty printing is diagnostic evidence, independent of structural equality. */
    private static void renderForDiagnostics(ObjectNode output, String field, SqlNode node) {
        try {
            output.put(field, node.toString());
        } catch (RuntimeException error) {
            var failure = output.putObject(field + "_render_failure");
            failure.put("class", error.getClass().getName()).put("message", error.getMessage());
            var stack = new java.io.StringWriter();
            error.printStackTrace(new java.io.PrintWriter(stack));
            failure.put("stack", stack.toString());
        }
    }

    static ObjectNode comparisonFailure(String id, Exception error) {
        var record = Utilities.deterministicObjectMapper().createObjectNode();
        record.put("id", id).put("equal", false);
        var failure = record.putObject("comparison_failure");
        failure.put("phase", error instanceof ComparisonFailure known ? known.phase : "comparison");
        failure.put("class", error.getClass().getName()).put("message", error.getMessage());
        var stack = new java.io.StringWriter();
        error.printStackTrace(new java.io.PrintWriter(stack));
        failure.put("stack", stack.toString());
        return record;
    }
}
