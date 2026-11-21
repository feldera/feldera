package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;

/** Export contextual expression fragments from optimized-view relations.
 * The default measures projects; --all-fields also walks other local Rex roots.
 * Neither mode transports whole-program S1. Unknown operators/forms and all compiler/adapter failures abort the run;
 * no failing case is converted into a successful exclusion. */
public final class S1ExpressionCorpusProbe {
    private S1ExpressionCorpusProbe() { }

    static S1ExpressionSources.Source source(DBSPCompiler compiler) {
        var snapshot = compiler.sources.replaySnapshot();
        var lines = new ArrayList<String>();
        for (JsonNode line: snapshot.path("lines")) lines.add(line.textValue());
        String label = snapshot.path("file").isTextual() ? snapshot.path("file").textValue() : "(no input file)";
        return new S1ExpressionSources.Source(label, String.join("\n", lines));
    }

    static ObjectNode capture(JsonNode input, S1BuiltinOperatorRegistry registry, boolean allFields) throws IOException {
        var mapper = Utilities.deterministicObjectMapper();
        var compiler = compile(input);
        // Diagnostic coordinates refer to these lines. Raw program text and API
        // boundaries are separate whole-program requirements, not inferred here.
        var snapshot = compiler.sources.replaySnapshot();
        var lines = new ArrayList<String>();
        for (JsonNode line: snapshot.path("lines")) lines.add(line.textValue());
        String label = snapshot.path("file").isTextual() ? snapshot.path("file").textValue() : "(no input file)";
        var source = new S1ExpressionSources.Source(label, String.join("\n", lines));
        ObjectNode record = mapper.createObjectNode(); record.set("id", input.path("id"));
        record.put("scope", allFields ? "local_fields" : "optimized_projects");
        var fragments = record.putArray("fragments");
        var plans = compiler.getPlans(new IndentStream(new StringBuilder()));
        var queue = new ArrayDeque<RelNode>();
        plans.entrySet().stream().sorted(Comparator.comparingInt(java.util.Map.Entry::getValue))
                .forEach(entry -> queue.add(entry.getKey()));
        var visited = new IdentityHashMap<RelNode, Boolean>();
        int ordinal = 0;
        while (!queue.isEmpty()) {
            RelNode relation = queue.removeFirst();
            if (visited.put(relation, true) != null) continue;
            var local = new ArrayList<org.apache.calcite.rex.RexNode>();
            if (allFields) S1RelExpressions.forEachRoot(relation, local::add, ignored -> { });
            else if (relation.getClass() == LogicalProject.class)
                local.addAll(((LogicalProject) relation).getProjects());
            if (!local.isEmpty()) {
                var fragment = fragments.addObject(); fragment.put("relation_ordinal", ordinal);
                try {
                    var expressions = new ArrayList<org.apache.calcite.rex.RexNode>();
                    for (var expression: local)
                        expressions.add(org.apache.calcite.rex.RexUtil.expandSearch(
                                compiler.getSqlToRelCompiler().getRexBuilder(), null, expression));
                    fragment.set("table", S1ExpressionEncoder.encode(expressions,
                            inputContext(relation), registry, source));
                } catch (IOException error) {
                    throw new IOException("Relation " + ordinal + " (" + relation.getRelTypeName()
                            + ") of " + lines.size() + " diagnostic source lines: " + local, error);
                }
            }
            ordinal++;
            queue.addAll(relation.getInputs());
        }
        return record;
    }

    static DBSPCompiler compile(JsonNode input) throws IOException {
        return compile(input, ignored -> { });
    }

    static DBSPCompiler compile(JsonNode input, java.util.function.Consumer<DBSPCompiler> configure) throws IOException {
        return compileResult(input, configure).compiler();
    }

    record Compilation(DBSPCompiler compiler, org.dbsp.sqlCompiler.circuit.DBSPCircuit circuit) { }

    static Compilation compileResult(JsonNode input, java.util.function.Consumer<DBSPCompiler> configure) throws IOException {
        var mapper = Utilities.deterministicObjectMapper();
        var options = mapper.treeToValue(input.path("options"), CompilerOptions.class);
        options.ioOptions.inputCircuit = true;
        var compiler = new DBSPCompiler(options);
        configure.accept(compiler);
        JsonNode submissions = input.path("submissions");
        if (!submissions.isArray() || submissions.isEmpty()) throw new IOException("Missing SQL submissions");
        for (JsonNode submission: submissions) {
            if (submission.path("batch").intValue() != 0)
                throw new IOException("Multiple compilation batches require explicit replay");
            if (submission.path("many").booleanValue())
                compiler.submitStatementsForCompilation(submission.path("sql").textValue());
            else compiler.submitStatementForCompilation(submission.path("sql").textValue());
        }
        var circuit = compiler.getFinalCircuit(false);
        if (compiler.hasErrors()) throw new IOException("Corpus compilation failed: " + compiler.messages);
        if (circuit == null) throw new IOException("Corpus case produced no circuit");
        return new Compilation(compiler, circuit);
    }

    /** Ordered input slots, including Window's separate constant slots. */
    static org.apache.calcite.rel.type.RelDataType inputContext(RelNode relation) {
        if (relation.getInputs().size() == 1 && !(relation instanceof org.apache.calcite.rel.logical.LogicalWindow))
            return relation.getInput(0).getRowType();
        var fields = new ArrayList<org.apache.calcite.rel.type.RelDataTypeField>();
        for (RelNode input: relation.getInputs())
            for (var field: input.getRowType().getFieldList())
                fields.add(new org.apache.calcite.rel.type.RelDataTypeFieldImpl(field.getName(), fields.size(), field.getType()));
        if (relation instanceof org.apache.calcite.rel.logical.LogicalWindow window)
            for (var constant: window.constants)
                fields.add(new org.apache.calcite.rel.type.RelDataTypeFieldImpl("$constant" + fields.size(), fields.size(), constant.getType()));
        return new org.apache.calcite.rel.type.RelRecordType(
                org.apache.calcite.rel.type.StructKind.FULLY_QUALIFIED, fields, false);
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2 || args.length > 3 || (args.length == 3 && !args[2].equals("--all-fields")))
            throw new IOException("usage: S1ExpressionCorpusProbe INPUT.jsonl OUTPUT_DIR [--all-fields]");
        boolean allFields = args.length == 3;
        Path output = Path.of(args[1]); Files.createDirectory(output);
        var registry = new S1BuiltinOperatorRegistry();
        var mapper = Utilities.deterministicObjectMapper().copy();
        mapper.enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        long cases = 0, fragments = 0, expressions = 0, roots = 0;
        try (var reader = Files.newBufferedReader(Path.of(args[0]));
             var writer = Files.newBufferedWriter(output.resolve("cases.jsonl"), StandardOpenOption.CREATE_NEW)) {
            String line;
            while ((line = reader.readLine()) != null) {
                JsonNode input = mapper.readTree(line);
                ObjectNode record;
                try { record = capture(input, registry, allFields); }
                catch (RuntimeException | IOException error) {
                    throw new IOException("Expression fragment export failed for case " + input.path("id"), error);
                }
                writer.write(record.toString()); writer.newLine(); writer.flush(); cases++;
                fragments += record.path("fragments").size();
                for (JsonNode fragment: record.path("fragments")) {
                    expressions += fragment.path("table").path("expressions").size();
                    roots += fragment.path("table").path("roots").size();
                }
            }
        }
        ObjectNode complete = mapper.createObjectNode();
        complete.put("schema_version", 1).put("complete", true).put("cases", cases).put("fragments", fragments)
                .put("expressions", expressions).put("roots", roots).put("scope", allFields ? "local_fields" : "optimized_projects")
                .put("runtime_executed", false).put("S1_parity_proven", false);
        Files.writeString(output.resolve("complete.json"), complete.toPrettyString(), StandardOpenOption.CREATE_NEW);
    }
}
