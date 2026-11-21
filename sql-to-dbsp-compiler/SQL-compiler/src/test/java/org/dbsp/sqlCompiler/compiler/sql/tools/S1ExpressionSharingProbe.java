package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.ExternalFunction;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;

/** Observe original input-reference identities before designing a whole-program
 * evaluation environment. This census neither transports nor rewrites them. */
public final class S1ExpressionSharingProbe {
    private S1ExpressionSharingProbe() { }

    static final class Use {
        final RexInputRef reference;
        final LinkedHashSet<String> environments = new LinkedHashSet<>();
        Use(RexInputRef reference) { this.reference = reference; }
    }

    static final class References extends RexShuttle {
        final IdentityHashMap<RexInputRef, Use> ids = new IdentityHashMap<>();
        final List<Use> uses = new ArrayList<>();
        final IdentityHashMap<ExternalFunction, Boolean> seenFunctions = new IdentityHashMap<>();
        final List<ExternalFunction> functions = new ArrayList<>();
        final List<List<RexNode>> values = new ArrayList<>();
        List<org.apache.calcite.sql.SqlOperator> declarations = List.of();
        String environment = "";
        int relations;
        List<RelNode> relationNodes;
        List<Integer> roots;
        final IdentityHashMap<RelNode, String> relationEnvironments = new IdentityHashMap<>();
        final IdentityHashMap<RelNode, List<RexNode>> relationExpressions = new IdentityHashMap<>();
        final IdentityHashMap<ExternalFunction, RexNode> functionBodies = new IdentityHashMap<>();
        final java.util.LinkedHashMap<String, Environment> environments = new java.util.LinkedHashMap<>();

        @Override public RexNode visitInputRef(RexInputRef reference) {
            Use use = this.ids.get(reference);
            if (use == null) {
                use = new Use(reference); this.ids.put(reference, use); this.uses.add(use);
            }
            use.environments.add(this.environment);
            return reference;
        }

        @Override public RexNode visitCall(RexCall call) {
            if (call.getOperator() instanceof ExternalFunction function && function.body != null
                    && this.seenFunctions.put(function, true) == null)
                this.functions.add(function);
            return super.visitCall(call);
        }
    }

    record Environment(ObjectNode owner, org.apache.calcite.rel.type.RelDataType row) { }

    private static int relationId(IdentityHashMap<RelNode, Integer> ids, RelNode relation) {
        Integer id = ids.get(relation);
        if (id == null) { id = ids.size(); ids.put(relation, id); }
        return id;
    }

    static References collect(List<RelNode> roots) {
        return collect(roots, java.util.function.UnaryOperator.identity());
    }

    static References collect(List<RelNode> roots, java.util.function.UnaryOperator<RexNode> normalize) {
        return collect(roots, normalize, List.of());
    }

    static References collect(List<RelNode> roots, java.util.function.UnaryOperator<RexNode> normalize,
                              List<List<RexNode>> values) {
        return collect(roots, normalize, values, List.of());
    }

    static References collect(List<RelNode> roots, java.util.function.UnaryOperator<RexNode> normalize,
                              List<List<RexNode>> values, List<? extends org.apache.calcite.sql.SqlOperator> declarations) {
        var references = new References();
        references.declarations = List.copyOf(declarations);
        for (var declaration: declarations)
            if (declaration instanceof ExternalFunction function && function.body != null && references.seenFunctions.put(function, true) == null)
                references.functions.add(function);
        var ids = new IdentityHashMap<RelNode, Integer>();
        var visited = new IdentityHashMap<RelNode, Boolean>();
        var queue = new ArrayDeque<>(roots);
        int relations = 0;
        while (!queue.isEmpty()) {
            RelNode relation = queue.removeFirst();
            if (visited.put(relation, true) != null) continue;
            int id = relationId(ids, relation);
            var inputs = new ArrayList<Integer>();
            for (RelNode input: relation.getInputs()) inputs.add(relationId(ids, input));
            // Unary consumers of the same producer share the same input row.
            // Window's appended constant slots belong to its own input environment.
            references.environment = "inputs:" + inputs
                    + (relation instanceof LogicalWindow ? "/window:" + id : "");
            ObjectNode owner = Utilities.deterministicObjectMapper().createObjectNode();
            owner.put("kind", relation instanceof LogicalWindow ? "window" : "relations");
            if (relation instanceof LogicalWindow) owner.put("relation", id);
            var inputIds = owner.putArray("inputs"); inputs.forEach(inputIds::add);
            references.environments.putIfAbsent(references.environment,
                    new Environment(owner, S1ExpressionCorpusProbe.inputContext(relation)));
            references.relationEnvironments.put(relation, references.environment);
            var local = new ArrayList<RexNode>();
            S1RelExpressions.forEachRoot(relation, expression -> local.add(normalize.apply(expression)), ignored -> { });
            references.relationExpressions.put(relation, List.copyOf(local));
            local.forEach(expression -> expression.accept(references));
            relations++; queue.addAll(relation.getInputs());
        }
        if (!values.isEmpty()) {
            references.environment = "constant";
            references.environments.put("constant", new Environment(
                    Utilities.deterministicObjectMapper().createObjectNode().put("kind", "constant"),
                    new org.apache.calcite.rel.type.RelRecordType(List.of())));
            for (var group: values) {
                var normalized = group.stream().map(normalize).toList();
                references.values.add(normalized);
                normalized.forEach(expression -> expression.accept(references));
            }
        }
        // Grow the queue as nested function bodies reveal additional functions.
        for (int index = 0; index < references.functions.size(); index++) {
            references.environment = "function:" + index;
            var function = references.functions.get(index);
            var owner = Utilities.deterministicObjectMapper().createObjectNode()
                    .put("kind", "function").put("function", index);
            var row = new org.apache.calcite.rel.type.RelRecordType(
                    org.apache.calcite.rel.type.StructKind.FULLY_QUALIFIED, function.parameterList, false);
            references.environments.put(references.environment, new Environment(owner, row));
            RexNode body = normalize.apply(function.body);
            references.functionBodies.put(function, body);
            body.accept(references);
        }
        references.relations = relations;
        var ordered = new ArrayList<RelNode>(java.util.Collections.nCopies(relations, null));
        ids.forEach((relation, id) -> ordered.set(id, relation));
        references.relationNodes = List.copyOf(ordered);
        references.roots = roots.stream().map(ids::get).toList();
        return references;
    }

    static ObjectNode audit(List<RelNode> roots) {
        var references = collect(roots);
        var result = Utilities.deterministicObjectMapper().createObjectNode();
        result.put("relations", references.relations).put("input_reference_objects", references.uses.size())
                .put("sql_function_objects", references.functions.size());
        var shared = result.putArray("shared_input_references");
        for (int index = 0; index < references.uses.size(); index++) {
            Use use = references.uses.get(index);
            if (use.environments.size() < 2) continue;
            var entry = shared.addObject().put("object", index).put("field", use.reference.getIndex())
                    .put("type", use.reference.getType().getFullTypeString());
            var environments = entry.putArray("environments");
            use.environments.forEach(environments::add);
        }
        result.put("shared_across_distinct_inputs", shared.size());
        return result;
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2 || args.length > 3 || (args.length == 3 && !java.util.Set.of("--transport", "--objects").contains(args[2])))
            throw new IOException("usage: S1ExpressionSharingProbe INPUT.jsonl OUTPUT_DIR [--transport|--objects]");
        boolean transport = args.length == 3;
        boolean objects = transport && args[2].equals("--objects");
        var output = Path.of(args[1]); Files.createDirectory(output);
        var mapper = Utilities.deterministicObjectMapper();
        var registry = new S1BuiltinOperatorRegistry();
        long cases = 0, inputReferences = 0, shared = 0, affected = 0;
        try (var reader = Files.newBufferedReader(Path.of(args[0]));
             var writer = Files.newBufferedWriter(output.resolve("cases.jsonl"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                var input = mapper.readTree(line);
                var compiler = S1ExpressionCorpusProbe.compile(input);
                var plans = compiler.getPlans(new org.dbsp.util.IndentStream(new StringBuilder()));
                var roots = plans.entrySet().stream().sorted(Comparator.comparingInt(java.util.Map.Entry::getValue))
                        .map(java.util.Map.Entry::getKey).toList();
                ObjectNode result = audit(roots);
                if (transport) {
                    var record = mapper.createObjectNode(); record.set("id", input.path("id"));
                    var captured = objects ? collect(roots, new S1ExpressionNormalizer(compiler.getSqlToRelCompiler().getRexBuilder())) : collect(roots);
                    var graph = objects ? S1ExpressionObjectsEncoder.encode(captured, registry, S1ExpressionCorpusProbe.source(compiler)) : null;
                    var table = graph == null ? S1InputUsesTable.encode(captured) : graph.inputs();
                    record.set("table", table.table());
                    record.set("producers", S1InputProducers.encode(captured, table));
                    record.set("properties", S1RelationProperties.encode(captured, table, registry));
                    if (graph != null) record.set("expressions", graph.expressions());
                    writer.write(record.toString()); writer.newLine(); writer.flush();
                }
                result.set("id", input.path("id"));
                if (!transport) { writer.write(result.toString()); writer.newLine(); writer.flush(); }
                cases++; inputReferences += result.path("input_reference_objects").longValue();
                long sharedCount = result.path("shared_across_distinct_inputs").longValue();
                shared += sharedCount; if (sharedCount > 0) affected++;
            }
        }
        if (cases == 0) throw new IOException("Empty sharing census");
        var complete = mapper.createObjectNode().put("complete", true).put("cases", cases)
                .put("input_reference_objects", inputReferences).put("shared_across_distinct_inputs", shared)
                .put("affected_cases", affected).put("scope", "optimized-view relations and SQL function bodies")
                .put("S1_parity_proven", false);
        Files.writeString(output.resolve("complete.json"), complete.toPrettyString());
    }
}
