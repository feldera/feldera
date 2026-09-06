package org.dbsp.sqlCompiler.compiler.visitors.outer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** Serializes the circuit to JSON and decodes the JSON back; the result is the decoded copy.
 * The copy must serialize to the same JSON as the original, up to node ids, node sharing,
 * source positions, and the program metadata, whose Calcite-level column properties the
 * decoder cannot rebuild. */
public class TestSerialize implements CircuitTransform {
    final DBSPCompiler compiler;

    public TestSerialize(DBSPCompiler compiler) {
        this.compiler = compiler;
    }

    @Override
    public String getName() {
        return "TestSerialize";
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        String str = this.toJson(circuit);
        try {
            JsonNode node = Utilities.deterministicObjectMapper().readTree(str);
            JsonDecoder decoder = new JsonDecoder(this.compiler.sqlToRelCompiler.typeFactory);
            DBSPCircuit result = decoder.decodeOuter(node, DBSPCircuit.class);
            this.checkSameJson(circuit, result);
            return result;
        } catch (JsonProcessingException ex) {
            System.out.println(str);
            throw new RuntimeException(ex);
        }
    }

    String toJson(DBSPCircuit circuit) {
        ToJsonOuterVisitor visitor = ToJsonOuterVisitor.create(this.compiler, 1);
        visitor.apply(circuit);
        return visitor.getJsonString();
    }

    /** Fails if the decoded circuit serializes differently from the original. */
    void checkSameJson(DBSPCircuit original, DBSPCircuit decoded) throws JsonProcessingException {
        Tree expected = new Tree(this.toJson(original));
        Tree actual = new Tree(this.toJson(decoded));
        String difference = new Comparison(expected, actual).firstDifference(expected.root, actual.root, "", true);
        Utilities.enforce(difference == null,
                () -> "The decoded circuit differs from the original at " + difference);
    }

    /** True if {@code node} is a serialized source position range. */
    static boolean isPosition(@Nullable JsonNode node) {
        return node != null && node.isObject() && node.has("start_line_number");
    }

    /** True if the node under {@code property} of {@code parent} is an operator or a
     * declaration rather than an inner node.  The two kinds have separate id spaces. */
    static boolean isOuter(JsonNode parent, String property) {
        if (property.equals("declarations") || property.equals("allOperators"))
            return true;
        // An OutputPort refers to its operator
        return property.equals("operator") && parent.has("outputNumber");
    }

    /** A parsed JSON circuit without its program metadata, with the node definitions indexed
     * by id.  The writer defines a node in full at its first occurrence and writes
     * {@code {"node": id}} afterwards. */
    static class Tree {
        final ObjectNode root;
        final Map<Long, JsonNode> innerDefinitions = new HashMap<>();
        final Map<Long, JsonNode> outerDefinitions = new HashMap<>();

        Tree(String json) throws JsonProcessingException {
            this.root = (ObjectNode) Utilities.deterministicObjectMapper().readTree(json);
            this.root.remove("metadata");
            this.collect(this.root, true);
        }

        void collect(JsonNode node, boolean outer) {
            if (node.isObject()) {
                if (node.has("id") && node.has("class"))
                    (outer ? this.outerDefinitions : this.innerDefinitions).put(node.get("id").asLong(), node);
                for (var field: node.properties()) {
                    // Annotations are plain JSON; some carry a "class" and an "id" of their own
                    if (field.getKey().equals("annotations"))
                        continue;
                    this.collect(field.getValue(), isOuter(node, field.getKey()));
                }
            } else if (node.isArray()) {
                for (JsonNode element: node)
                    this.collect(element, outer);
            }
        }

        /** The definition of a node reference, or the node itself if it is not a reference. */
        JsonNode resolve(JsonNode node, boolean outer) {
            if (!node.isObject() || node.size() != 1 || !node.has("node"))
                return node;
            long id = node.get("node").asLong();
            JsonNode definition = (outer ? this.outerDefinitions : this.innerDefinitions).get(id);
            Utilities.enforce(definition != null, () -> "Reference to undefined node " + id);
            return definition;
        }
    }

    /** Compares two circuit trees structurally: references are followed, ids are ignored, and a
     * pair of definitions is compared once, so sharing may differ between the trees. */
    static class Comparison {
        final Tree expected;
        final Tree actual;
        /** Pairs of definitions (expected id, actual id, kind) already compared */
        final Set<String> compared = new HashSet<>();

        Comparison(Tree expected, Tree actual) {
            this.expected = expected;
            this.actual = actual;
        }

        /** The path of the first difference between the two subtrees, or null if they are equal. */
        @Nullable
        String firstDifference(JsonNode e, JsonNode a, String path, boolean outer) {
            e = this.expected.resolve(e, outer);
            a = this.actual.resolve(a, outer);
            if (e.isObject() && a.isObject()) {
                if (e.has("id") && a.has("id")) {
                    String pair = (outer ? "outer " : "inner ") + e.get("id").asLong() + ":" + a.get("id").asLong();
                    if (!this.compared.add(pair))
                        return null;
                }
                Set<String> names = new TreeSet<>();
                e.fieldNames().forEachRemaining(names::add);
                a.fieldNames().forEachRemaining(names::add);
                names.remove("id");
                for (String name: names) {
                    JsonNode eChild = e.get(name);
                    JsonNode aChild = a.get(name);
                    // Source positions are not compared: the decoder gives none to shared
                    // nodes, and a node built from a child inherits the child's position
                    if (isPosition(eChild) || isPosition(aChild))
                        continue;
                    if (name.equals("annotations")) {
                        if (!eChild.equals(aChild))
                            return path + "/" + name + ": original " + eChild + ", decoded " + aChild;
                        continue;
                    }
                    String childPath = path + "/" + name;
                    if (eChild != null && eChild.has("class"))
                        childPath += "<" + eChild.get("class").asText() + ">";
                    if (eChild == null)
                        return childPath + ": only in the decoded circuit";
                    if (aChild == null)
                        return childPath + ": only in the original circuit";
                    String difference = this.firstDifference(eChild, aChild, childPath, isOuter(e, name));
                    if (difference != null)
                        return difference;
                }
                return null;
            }
            if (e.isArray() && a.isArray()) {
                if (e.size() != a.size())
                    return path + ": " + e.size() + " elements in the original, " +
                            a.size() + " in the decoded circuit";
                for (int i = 0; i < e.size(); i++) {
                    String elementPath = path + "[" + i + "]";
                    if (e.get(i).has("class"))
                        elementPath += "<" + e.get(i).get("class").asText() + ">";
                    String difference = this.firstDifference(e.get(i), a.get(i), elementPath, outer);
                    if (difference != null)
                        return difference;
                }
                return null;
            }
            if (e.equals(a))
                return null;
            return path + ": original " + Utilities.toDepth(e, 1) + ", decoded " + Utilities.toDepth(a, 1);
        }
    }
}
