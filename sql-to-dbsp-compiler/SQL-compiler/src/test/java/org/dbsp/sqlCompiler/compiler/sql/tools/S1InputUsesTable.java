package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Exact input objects with separately validated evaluation uses. Producer IDs
 * are opaque here; a relation adapter must establish the environments' rows. */
final class S1InputUsesTable {
    private S1InputUsesTable() { }
    record Decoded(List<RexInputRef> objects, List<RexInputRef> uses) { }

    record Encoded(ObjectNode table, S1TypeTable.Encoded types) { }

    static Encoded encode(S1ExpressionSharingProbe.References captured) throws IOException {
        return encode(captured, List.of());
    }

    static Encoded encode(S1ExpressionSharingProbe.References captured, List<RelDataType> additionalTypes) throws IOException {
        var roots = new ArrayList<RelDataType>();
        captured.uses.forEach(use -> roots.add(use.reference.getType()));
        captured.environments.values().forEach(environment -> roots.add(environment.row()));
        captured.relationNodes.forEach(relation -> roots.add(relation.getRowType()));
        for (var relation: captured.relationNodes)
            if (relation instanceof org.apache.calcite.rel.logical.LogicalWindow window)
                window.constants.forEach(constant -> roots.add(constant.getType()));
        roots.addAll(S1RelationProperties.additionalTypes(captured.relationNodes));
        roots.addAll(additionalTypes);
        var arena = S1TypeTable.encodeWithIds(roots);
        ObjectNode types = arena.table();
        var wire = Utilities.deterministicObjectMapper().createObjectNode();
        wire.put("schema_version", "input-uses-v1").put("relations", captured.relations)
                .put("functions", captured.functions.size());
        wire.set("type_table", types);
        var objects = wire.putArray("objects");
        for (var use: captured.uses)
            objects.addObject().put("ty", arena.id(use.reference.getType())).put("field", use.reference.getIndex());
        var environments = wire.putArray("environments");
        var ids = new java.util.LinkedHashMap<String, Integer>();
        for (var entry: captured.environments.entrySet()) {
            ids.put(entry.getKey(), ids.size());
            var environment = environments.addObject();
            environment.set("owner", entry.getValue().owner());
            environment.put("row", arena.id(entry.getValue().row()));
        }
        var uses = wire.putArray("uses");
        for (int object = 0; object < captured.uses.size(); object++)
            for (String environment: captured.uses.get(object).environments)
                uses.addObject().put("object", object).put("environment", ids.get(environment));
        Decoded decoded = decode(wire);
        var distinct = new java.util.IdentityHashMap<RexInputRef, Boolean>();
        for (int index = 0; index < captured.uses.size(); index++) {
            RexInputRef original = captured.uses.get(index).reference, imported = decoded.objects().get(index);
            check(distinct.put(imported, true) == null, "Distinct input objects were interned");
            check(original.getType().equals(imported.getType()) && original.getIndex() == imported.getIndex(),
                    "Input object changed");
        }
        return new Encoded(wire, arena);
    }

    static Decoded decode(JsonNode wire) throws IOException {
        fields(wire, "schema_version", "type_table", "relations", "functions", "objects", "environments", "uses");
        check(wire.path("schema_version").asText().equals("input-uses-v1"), "Unknown input-use version");
        int relations = integer(wire.path("relations")), functions = integer(wire.path("functions"));
        check(relations <= 100_000 && functions <= 100_000, "Producer arena exceeds limit");
        array(wire.path("objects"), 100_000); array(wire.path("environments"), 100_000);
        array(wire.path("uses"), 1_000_000);
        var types = S1TypeTable.decodeWithSources(wire.path("type_table")).types();
        var objects = new ArrayList<RexInputRef>();
        for (var object: wire.path("objects")) {
            fields(object, "ty", "field");
            objects.add(new RexInputRef(integer(object.path("field")), types.get(reference(object.path("ty"), types.size()))));
        }
        var rows = new ArrayList<Integer>();
        var owners = new HashSet<JsonNode>();
        long references = 2L * wire.path("uses").size() + objects.size() + wire.path("environments").size();
        for (var environment: wire.path("environments")) {
            fields(environment, "owner", "row");
            JsonNode owner = environment.path("owner");
            switch (owner.path("kind").asText()) {
                case "constant" -> fields(owner, "kind");
                case "relations" -> fields(owner, "kind", "inputs");
                case "window" -> {
                    fields(owner, "kind", "inputs", "relation"); reference(owner.path("relation"), relations); references++;
                }
                case "function" -> {
                    fields(owner, "kind", "function"); reference(owner.path("function"), functions); references++;
                }
                default -> throw new IOException("Unknown input owner kind");
            }
            if (owner.has("inputs")) {
                array(owner.path("inputs"), 1_000_000);
                references += owner.path("inputs").size();
                for (var input: owner.path("inputs")) reference(input, relations);
            }
            check(references <= 1_000_000, "Input references exceed limit");
            check(owners.add(owner), "Duplicate input environment owner");
            int row = reference(environment.path("row"), types.size());
            check(types.get(row).isStruct(), "Input environment requires row type"); rows.add(row);
            if (owner.path("kind").asText().equals("constant"))
                check(types.get(row).getFieldCount() == 0, "Constant environment must have no fields");
        }
        check(references <= 1_000_000, "Input references exceed limit");
        var uses = new ArrayList<RexInputRef>();
        var states = new HashSet<Long>();
        var used = new boolean[objects.size()];
        for (var use: wire.path("uses")) {
            fields(use, "object", "environment");
            int object = reference(use.path("object"), objects.size()), environment = reference(use.path("environment"), rows.size());
            check(states.add(((long) object << 32) | environment), "Duplicate input evaluation state");
            used[object] = true;
            var input = objects.get(object);
            JsonNode fields = wire.path("type_table").path("types").get(rows.get(environment)).path("kind").path("fields");
            check(input.getIndex() < fields.size(), "Input field is outside environment");
            int declared = integer(fields.get(input.getIndex()).path("ty"));
            int result = integer(wire.path("objects").get(object).path("ty"));
            JsonNode source = wire.path("type_table").path("types").get(declared);
            JsonNode target = wire.path("type_table").path("types").get(result);
            check(declared == result || (!source.path("kind").path("kind").asText().equals("named")
                    && source.path("kind").equals(target.path("kind"))
                    && (!source.path("nullable").booleanValue() || target.path("nullable").booleanValue())),
                    "Input type differs from evaluation environment");
            uses.add(input);
        }
        for (boolean present: used) check(present, "Input object has no evaluation use");
        return new Decoded(List.copyOf(objects), List.copyOf(uses));
    }

    private static int reference(JsonNode node, int count) throws IOException {
        int value = integer(node); check(value < count, "Input-use reference outside arena"); return value;
    }
    private static int integer(JsonNode node) throws IOException {
        check(node.isIntegralNumber() && node.canConvertToInt() && node.intValue() >= 0, "Invalid unsigned input-use index");
        return node.intValue();
    }
    private static void array(JsonNode node, int limit) throws IOException {
        check(node.isArray() && node.size() <= limit, "Invalid or oversized input-use array");
    }
    private static void fields(JsonNode node, String... names) throws IOException {
        check(node.isObject(), "Missing input-use object");
        var actual = new HashSet<String>(); node.fieldNames().forEachRemaining(actual::add);
        check(actual.equals(Set.of(names)), "Unknown or missing input-use fields");
    }
    private static void check(boolean condition, String message) throws IOException {
        if (!condition) throw new IOException(message);
    }
}
