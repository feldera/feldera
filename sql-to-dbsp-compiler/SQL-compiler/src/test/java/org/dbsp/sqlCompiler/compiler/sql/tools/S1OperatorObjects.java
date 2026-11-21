package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.FelderaS1Identifiers;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlTypeConstructorFunction;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.dbsp.util.Utilities;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.ExternalFunction;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.SqlUserDefinedAggregationFunction;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Exact object arena for builtin operators and declarative type constructors.
 * The wire selects fixed adapters, never a Java class to instantiate.
 * Instantiated call signatures live in the expression table separately. */
final class S1OperatorObjects {
    record Decoded(List<SqlOperator> objects, long references, long identifierBytes) {
        Decoded { objects = java.util.Collections.unmodifiableList(new ArrayList<>(objects)); }
    }

    private final S1BuiltinOperatorRegistry registry;
    private final S1ExpressionSources sources;
    private final List<SqlOperator> objects = new ArrayList<>();
    private final List<Integer> objectSources = new ArrayList<>();
    private final IdentityHashMap<SqlOperator, Integer> ids = new IdentityHashMap<>();

    S1OperatorObjects(S1BuiltinOperatorRegistry registry, S1ExpressionSources sources) {
        this.registry = registry; this.sources = sources;
    }

    int add(SqlOperator operator) throws IOException { return this.add(operator, 0); }

    int add(SqlOperator operator, int source) throws IOException {
        Integer previous = this.ids.get(operator);
        if (previous != null) return previous;
        if (this.registry.contains(operator)) {
            // Prefer the trusted singleton even when its class also has a
            // declarative adapter for separately constructed objects.
        } else if (operator.getClass() == SqlTypeConstructorFunction.class) {
            S1SqlIdentifier.encode(((SqlTypeConstructorFunction) operator).getSqlIdentifier(), this.sources, source);
        } else if (operator.getClass() != SqlRowOperator.class && operator.getClass() != ExternalFunction.class
                && operator.getClass() != SqlUserDefinedAggregationFunction.class
                && operator.getClass() != org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction.class) this.registry.identify(operator);
        int id = this.objects.size(); this.ids.put(operator, id); this.objects.add(operator); this.objectSources.add(source);
        return id;
    }

    Map<SqlOperator, Integer> identities() {
        return java.util.Collections.unmodifiableMap(new IdentityHashMap<>(this.ids));
    }

    int size() { return this.objects.size(); }
    ExternalFunction function(int index) {
        return this.objects.get(index).getClass() == ExternalFunction.class ? (ExternalFunction) this.objects.get(index) : null;
    }

    List<RelDataType> declaredTypes() {
        var result = new ArrayList<RelDataType>();
        for (SqlOperator operator: this.objects)
            if (operator.getClass() == SqlTypeConstructorFunction.class) result.add(constructorType(operator));
            else if (operator.getClass() == SqlUserDefinedAggregationFunction.class) {
                var description = ((SqlUserDefinedAggregationFunction) operator).description;
                result.add(description.returnType);
                for (var parameter: description.parameterList) result.add(parameter.getType());
            } else if (operator.getClass() == ExternalFunction.class) {
                var function = (ExternalFunction) operator; result.add(function.returnType);
                for (var parameter: function.parameterList) result.add(parameter.getType());
            }
        return result;
    }

    ArrayNode encode(Map<RelDataType, Integer> types, Map<ExternalFunction, S1FunctionObjects.Body> bodies) throws IOException {
        var result = Utilities.deterministicObjectMapper().createArrayNode();
        for (int index = 0; index < this.objects.size(); index++) {
            SqlOperator operator = this.objects.get(index);
            ObjectNode entry = result.addObject();
            if (this.registry.contains(operator)) {
                var identity = this.registry.identify(operator);
                entry.put("kind", "builtin").put("registry", identity.registry())
                        .put("registry_version", identity.version()).put("key", identity.key());
            } else if (operator.getClass() == org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction.class) {
                entry.put("kind", "sum_zero");
            } else if (operator.getClass() == SqlTypeConstructorFunction.class) {
                entry.put("kind", "type_constructor");
                entry.set("name", S1SqlIdentifier.encode(((SqlTypeConstructorFunction) operator).getSqlIdentifier(), this.sources, this.objectSources.get(index)));
                Integer type = types.get(constructorType(operator));
                check(type != null, "Constructor type is outside its arena"); entry.put("declared_type", type);
            } else if (operator.getClass() == SqlRowOperator.class) {
                entry.put("kind", "row_constructor").put("name", operator.getName());
                var fields = entry.putObject("fields");
                List<String> names = ((SqlRowOperator) operator).getFieldNames();
                if (names == null) fields.put("kind", "absent");
                else {
                    fields.put("kind", "present"); var items = fields.putArray("names");
                    for (String field: names) { if (field == null) items.addNull(); else items.add(field); }
                }
            } else if (operator.getClass() == SqlUserDefinedAggregationFunction.class) {
                result.set(result.size() - 1, S1AggregateObjects.encode((SqlUserDefinedAggregationFunction) operator,
                        types, this.sources, this.objectSources.get(index)));
            } else if (operator.getClass() == ExternalFunction.class) {
                result.set(result.size() - 1, S1FunctionObjects.encode((ExternalFunction) operator, types, this.sources, bodies.get(operator)));
            } else {
                throw new IOException("Unsupported operator object class");
            }
        }
        return result;
    }

    void verify(List<SqlOperator> imported, Map<RelDataType, Integer> originalTypes,
                List<RelDataType> decodedTypes, Map<ExternalFunction, S1FunctionObjects.Body> bodies,
                List<org.apache.calcite.rex.RexNode> nodes) throws IOException {
        check(imported.size() == this.objects.size(), "Operator object count changed");
        var reverse = new IdentityHashMap<SqlOperator, Boolean>();
        var decodedIds = new IdentityHashMap<RelDataType, Integer>();
        for (int index = 0; index < decodedTypes.size(); index++) decodedIds.put(decodedTypes.get(index), index);
        var reconstructed = new S1OperatorObjects(this.registry, this.sources);
        var decodedBodies = new IdentityHashMap<ExternalFunction, S1FunctionObjects.Body>();
        for (int index = 0; index < imported.size(); index++) {
            SqlOperator original = this.objects.get(index), decoded = imported.get(index);
            check(reverse.put(decoded, true) == null, "Distinct operator objects were interned");
            check(original.getClass() == decoded.getClass(), "Operator class changed");
            if (this.registry.contains(original))
                check(original == decoded, "Builtin operator identity changed");
            if (original instanceof ExternalFunction function) {
                var body = bodies.get(function);
                var importedFunction = (ExternalFunction) decoded;
                if (body != null) {
                    check(importedFunction.body == nodes.get(body.root()), "Function body sharing changed");
                    decodedBodies.put(importedFunction, body);
                }
                check(importedFunction.structReturnType == null, "Function return cache was not reset");
            }
            reconstructed.add(decoded, this.objectSources.get(index));
        }
        check(this.encode(originalTypes, bodies).equals(reconstructed.encode(decodedIds, decodedBodies)), "Operator declarations changed");
    }

    static Decoded decode(JsonNode wire, List<RelDataType> types, S1ExpressionSources sources,
                          S1BuiltinOperatorRegistry registry) throws IOException {
        check(wire.isArray() && wire.size() <= 100_000, "Invalid operator object arena");
        var objects = new ArrayList<SqlOperator>();
        var identities = new IdentityHashMap<SqlOperator, Boolean>();
        long references = 0, identifierBytes = 0;
        for (JsonNode entry: wire) {
            String tag = text(entry, "kind");
            if (tag.equals("builtin")) {
                fields(entry, "kind", "registry", "registry_version", "key");
                String registryName = text(entry, "registry"), version = text(entry, "registry_version"), key = text(entry, "key");
                check(!registryName.isEmpty() && !version.isEmpty() && !key.isEmpty(), "Empty builtin identity");
                for (String value: List.of(registryName, version, key)) identifierBytes += bytes(value);
                SqlOperator operator = registry.resolve(new S1BuiltinOperatorRegistry.Identity(registryName, version, key));
                objects.add(operator);
            } else if (tag.equals("sum_zero")) {
                fields(entry, "kind");
                objects.add(new org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction());
            } else if (tag.equals("type_constructor")) {
                fields(entry, "kind", "name", "declared_type");
                int type = unsigned(entry.path("declared_type"));
                check(type < types.size(), "Constructor type is outside its arena"); references++;
                SqlIdentifier name = S1SqlIdentifier.decode(entry.path("name"), sources);
                references += name.names.size() + 1;
                for (String part: name.names) identifierBytes += bytes(part);
                var components = FelderaS1Identifiers.components(name);
                if (components != null) references += components.size();
                objects.add(new SqlTypeConstructorFunction(name, types.get(type)));
            } else if (tag.equals("row_constructor")) {
                fields(entry, "kind", "name", "fields"); String name = text(entry, "name");
                check(!name.isEmpty(), "Empty row constructor name"); identifierBytes += bytes(name);
                JsonNode fields = entry.path("fields"); List<String> names = null;
                if (text(fields, "kind").equals("absent")) fields(fields, "kind");
                else {
                    fields(fields, "kind", "names");
                    check(text(fields, "kind").equals("present") && fields.path("names").isArray(), "Invalid row constructor field names");
                    check(fields.path("names").size() <= 1_000_000, "Row field names exceed limit");
                    names = new ArrayList<>(); references += fields.path("names").size();
                    for (JsonNode field: fields.path("names")) {
                        check(field.isNull() || field.isTextual(), "Invalid row field name");
                        names.add(field.isNull() ? null : field.textValue());
                        if (field.isTextual()) identifierBytes += bytes(field.textValue());
                    }
                }
                objects.add(new SqlRowOperator(name, names));
            } else if (tag.equals("aggregate_function")) {
                var aggregate = S1AggregateObjects.decode(entry, types, sources);
                var usage = S1AggregateObjects.usage(aggregate);
                references += usage.references(); identifierBytes += usage.bytes(); objects.add(aggregate);
            } else if (tag.equals("function")) {
                var usage = S1FunctionObjects.validate(entry, types, sources);
                references += usage.references(); identifierBytes += usage.bytes(); objects.add(null);
            } else throw new IOException("Unsupported operator object kind: " + tag);
            if (objects.get(objects.size() - 1) != null)
                check(identities.put(objects.get(objects.size() - 1), true) == null, "Duplicate builtin operator object identity");
            check(identifierBytes <= 1024 * 1024 && references <= 1_000_000, "Operator objects exceed limits");
        }
        return new Decoded(objects, references, identifierBytes);
    }

    private static RelDataType constructorType(SqlOperator operator) {
        // The exact trusted class returns its stored type without consulting a
        // binding. No reflection or SQL re-inference is involved.
        return ((SqlTypeConstructorFunction) operator).inferReturnType((SqlOperatorBinding) null);
    }

    private static int bytes(String text) { return text.getBytes(StandardCharsets.UTF_8).length; }
    private static int unsigned(JsonNode node) throws IOException {
        check(node.isIntegralNumber() && node.canConvertToInt() && node.intValue() >= 0, "Invalid operator object reference"); return node.intValue();
    }
    private static String text(JsonNode node, String field) throws IOException {
        check(node.path(field).isTextual(), "Missing operator object string: " + field); return node.path(field).textValue();
    }
    private static void fields(JsonNode node, String... names) throws IOException {
        check(node.isObject(), "Missing operator object"); var actual = new HashSet<String>(); node.fieldNames().forEachRemaining(actual::add);
        check(actual.equals(Set.of(names)), "Unknown or missing operator object fields");
    }
    private static void check(boolean condition, String message) throws IOException {
        if (!condition) throw new IOException(message);
    }
}
