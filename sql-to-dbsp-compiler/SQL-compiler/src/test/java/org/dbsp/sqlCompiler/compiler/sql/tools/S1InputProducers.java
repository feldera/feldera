package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.dbsp.util.Utilities;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/** Ordered producer topology and exact row layouts. Predicates, grouping,
 * frames, traits and catalog bindings still require the full relation codec. */
final class S1InputProducers {
    private S1InputProducers() { }

    static ObjectNode encode(S1ExpressionSharingProbe.References captured, S1InputUsesTable.Encoded encoded) throws IOException {
        var result = Utilities.deterministicObjectMapper().createObjectNode().put("schema_version", "input-producers-v0");
        var ids = new IdentityHashMap<RelNode, Integer>();
        for (int i = 0; i < captured.relationNodes.size(); i++) ids.put(captured.relationNodes.get(i), i);
        var types = encoded.types();
        var relations = result.putArray("relations");
        for (int i = 0; i < captured.relationNodes.size(); i++) {
            RelNode node = captured.relationNodes.get(i);
            var entry = relations.addObject().put("form", node.getClass().getSimpleName())
                    .put("row", types.id(node.getRowType()));
            var inputs = entry.putArray("inputs");
            node.getInputs().forEach(input -> inputs.add(ids.get(input)));
            var constants = entry.putArray("constants");
            if (node instanceof LogicalWindow window)
                for (var constant: window.constants) constants.add(types.id(constant.getType()));
        }
        var roots = result.putArray("roots"); captured.roots.forEach(roots::add);
        var functions = result.putArray("function_rows");
        for (int i = 0; i < captured.functions.size(); i++)
            functions.add(types.id(captured.environments.get("function:" + i).row()));
        validate(result, encoded.table());
        return result;
    }

    static void validate(JsonNode graph, JsonNode table) throws IOException {
        fields(graph, "schema_version", "relations", "roots", "function_rows");
        check(graph.path("schema_version").asText().equals("input-producers-v0"), "Unknown producer version");
        var relations = graph.path("relations");
        array(relations); array(graph.path("roots")); array(graph.path("function_rows"));
        check(relations.size() == table.path("relations").intValue()
                && graph.path("function_rows").size() == table.path("functions").intValue(), "Producer arena counts differ");
        var edges = new ArrayList<List<Integer>>();
        var ordinary = new HashSet<JsonNode>();
        long references = graph.path("roots").size() + graph.path("function_rows").size();
        for (var producer: relations) {
            fields(producer, "form", "row", "inputs", "constants");
            row(table, producer.path("row")); array(producer.path("inputs")); array(producer.path("constants"));
            int count = producer.path("inputs").size();
            String form = producer.path("form").asText();
            boolean arity = switch (form) {
                case "LogicalTableScan", "JdbcTableScan", "LogicalValues" -> count == 0;
                case "LogicalJoin", "LogicalAsofJoin", "LogicalCorrelate" -> count == 2;
                case "LogicalUnion", "LogicalIntersect", "LogicalMinus" -> count > 0;
                case "LogicalTableFunctionScan" -> true;
                case "LogicalProject", "LogicalFilter", "LogicalAggregate", "LogicalWindow",
                        "LogicalSort", "Uncollect", "Collect" -> count == 1;
                default -> throw new IOException("Unknown producer relation form");
            };
            check(arity, "Producer input arity differs");
            var children = new ArrayList<Integer>();
            for (var input: producer.path("inputs")) children.add(index(input, relations.size()));
            edges.add(children);
            for (var constant: producer.path("constants")) index(constant, table.at("/type_table/types").size());
            if (!form.equals("LogicalWindow")) {
                check(producer.path("constants").isEmpty(), "Only windows have constant slots");
                ordinary.add(producer.path("inputs"));
            }
            references += 1L + count + producer.path("constants").size();
            check(references <= 1_000_000, "Producer references exceed limit");
        }
        check(references <= 1_000_000, "Producer references exceed limit");
        for (var root: graph.path("roots")) index(root, relations.size());
        S1ArenaGraph.order(edges);
        for (var function: graph.path("function_rows")) row(table, function);
        long checkedSlots = 0;
        for (var environment: table.path("environments")) {
            var expected = row(table, environment.path("row"));
            checkedSlots += expected.size();
            check(checkedSlots <= 1_000_000, "Environment slot comparisons exceed limit");
            var owner = environment.path("owner");
            JsonNode constants = Utilities.deterministicObjectMapper().createArrayNode();
            switch (owner.path("kind").asText()) {
                case "constant" -> check(expected.isEmpty(), "Constant environment has fields");
                case "relations" -> check(ordinary.contains(owner.path("inputs")), "Environment has no consumer");
                case "window" -> {
                    var producer = relations.get(index(owner.path("relation"), relations.size()));
                    check(producer.path("form").asText().equals("LogicalWindow")
                            && producer.path("inputs").equals(owner.path("inputs")), "Window owner differs");
                    constants = producer.path("constants");
                }
                case "function" -> {
                    var function = graph.path("function_rows").get(index(owner.path("function"), graph.path("function_rows").size()));
                    check(expected.equals(row(table, function)), "Function environment differs");
                    continue;
                }
                default -> throw new IOException("Unknown input owner");
            }
            int offset = 0;
            for (var input: owner.path("inputs")) {
                var actual = row(table, relations.get(index(input, relations.size())).path("row"));
                for (var field: actual) {
                    check(offset < expected.size() && field.equals(expected.get(offset)), "Ordered producer fields differ");
                    offset++;
                }
            }
            for (var constant: constants) {
                check(offset < expected.size() && expected.get(offset).path("ty").equals(constant), "Window constant type differs");
                offset++;
            }
            check(offset == expected.size(), "Extra environment fields");
        }
    }

    private static JsonNode row(JsonNode table, JsonNode id) throws IOException {
        var types = table.at("/type_table/types");
        var type = types.get(index(id, types.size())).path("kind");
        check(type.path("kind").asText().equals("row"), "Producer requires row type");
        return type.path("fields");
    }
    private static int index(JsonNode value, int size) throws IOException {
        check(value.isIntegralNumber() && value.canConvertToInt() && value.intValue() >= 0
                && value.intValue() < size, "Producer reference outside arena"); return value.intValue();
    }
    private static void array(JsonNode value) throws IOException {
        check(value.isArray() && value.size() <= 1_000_000, "Invalid producer array");
    }
    private static void fields(JsonNode value, String... names) throws IOException {
        check(value.isObject(), "Missing producer object");
        var actual = new HashSet<String>(); value.fieldNames().forEachRemaining(actual::add);
        check(actual.equals(Set.of(names)), "Unknown or missing producer fields");
    }
    private static void check(boolean condition, String message) throws IOException {
        if (!condition) throw new IOException(message);
    }
}
