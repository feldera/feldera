package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.MultisetSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.SqlToRelCompiler;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Test-only ordered scalar/row/collection arena. Statement-level named types,
 * source origins and planner-only types still require separate S1 contracts. */
final class S1TypeTable {
    private static final int MAX_TYPES = 100_000;
    private static final int MAX_REFERENCES = 1_000_000;
    private static final int MAX_DEPTH = 256;

    private S1TypeTable() { }

    /** Calcite's default caches are global. Bypass both interning entry points
     * so equal but distinct wire entries remain separate Java objects. */
    static final class Factory extends SqlTypeFactoryImpl {
        Factory() { super(SqlToRelCompiler.TYPE_SYSTEM); }

        @Override public Charset getDefaultCharset() { return StandardCharsets.UTF_8; }
        @Override protected RelDataType canonize(RelDataType type) { return type; }
        @Override protected RelDataType canonize(StructKind kind, List<String> names,
                                                List<RelDataType> types, boolean nullable) {
            List<RelDataTypeField> fields = new ArrayList<>();
            for (int index = 0; index < names.size(); index++)
                fields.add(new RelDataTypeFieldImpl(names.get(index), index, types.get(index)));
            return new RelRecordType(kind, fields, nullable);
        }
    }

    record Decoded(List<RelDataType> types, List<RelDataType> roots) {
        Decoded { types = List.copyOf(types); roots = List.copyOf(roots); }
    }

    /** Identity lookup belongs to the arena that assigned IDs. Adapters must
     * not recover IDs by relying on another adapter's root-list offsets. */
    record Encoded(ObjectNode table, Map<RelDataType, Integer> ids) {
        Encoded { ids = java.util.Collections.unmodifiableMap(new IdentityHashMap<>(ids)); }
        int id(RelDataType type) throws IOException { return S1TypeTable.id(this.ids, type); }
    }

    private static ObjectNode object() { return Utilities.deterministicObjectMapper().createObjectNode(); }

    static ObjectNode encode(List<RelDataType> roots) throws IOException {
        return encodeWithIds(roots).table();
    }

    static Encoded encodeWithIds(List<RelDataType> roots) throws IOException {
        List<RelDataType> arena = new ArrayList<>();
        var ids = new IdentityHashMap<RelDataType, Integer>();
        for (RelDataType root: roots)
            collect(root, arena, ids, new IdentityHashMap<>(), 1);
        ObjectNode table = encodeArena(arena, roots);
        decode(Utilities.deterministicObjectMapper().readTree(table.toString()));
        return new Encoded(table, ids);
    }

    private static void collect(RelDataType type, List<RelDataType> arena,
                                IdentityHashMap<RelDataType, Integer> ids,
                                IdentityHashMap<RelDataType, Boolean> active, int depth) throws IOException {
        if (depth > MAX_DEPTH || active.containsKey(type))
            throw new IOException("Cyclic or overdeep type graph");
        if (ids.containsKey(type))
            return;
        if (arena.size() >= MAX_TYPES)
            throw new IOException("Type count exceeds the validation limit");
        ids.put(type, arena.size());
        arena.add(type);
        active.put(type, true);
        for (RelDataType child: children(type))
            collect(child, arena, ids, active, depth + 1);
        active.remove(type);
    }

    private static List<RelDataType> children(RelDataType type) {
        if (type instanceof RelRecordType)
            return type.getFieldList().stream().map(RelDataTypeField::getType).toList();
        if (type instanceof ArraySqlType || type instanceof MultisetSqlType)
            return List.of(type.getComponentType());
        if (type instanceof MapSqlType)
            return List.of(type.getKeyType(), type.getValueType());
        return List.of();
    }

    static ObjectNode encodeArena(List<RelDataType> arena, List<RelDataType> roots) throws IOException {
        if (arena.size() > MAX_TYPES || roots.size() > MAX_REFERENCES)
            throw new IOException("Type arena exceeds the validation limit");
        var ids = new IdentityHashMap<RelDataType, Integer>();
        for (int index = 0; index < arena.size(); index++) {
            if (ids.put(arena.get(index), index) != null)
                throw new IOException("Distinct arena entries share one Java type object");
        }
        ObjectNode table = object();
        table.put("schema_version", "types-v0");
        table.putArray("sources");
        ArrayNode entries = table.putArray("types");
        long references = roots.size();
        for (RelDataType type: arena) {
            references += children(type).size();
            if (references > MAX_REFERENCES)
                throw new IOException("Type references exceed the validation limit");
            entries.add(encodeEntry(type, ids));
        }
        ArrayNode rootIds = table.putArray("roots");
        for (RelDataType root: roots)
            rootIds.add(id(ids, root));
        return table;
    }

    private static int id(Map<RelDataType, Integer> ids, RelDataType type) throws IOException {
        Integer result = ids.get(type);
        if (result == null)
            throw new IOException("Type reference is outside its arena");
        return result;
    }

    private static ObjectNode encodeEntry(RelDataType type, Map<RelDataType, Integer> ids) throws IOException {
        ObjectNode entry = object();
        entry.put("nullable", type.isNullable());
        entry.putObject("origin").put("kind", "absent");
        ObjectNode kind = entry.putObject("kind");
        if (type instanceof RelRecordType) {
            kind.put("kind", "row");
            if (type.getStructKind() == StructKind.NONE)
                throw new IOException("Non-struct row kind");
            kind.put("struct_kind", type.getStructKind().name().toLowerCase(Locale.ROOT));
            ArrayNode fields = kind.putArray("fields");
            for (RelDataTypeField field: type.getFieldList()) {
                ObjectNode value = fields.addObject();
                ObjectNode name = value.putObject("name");
                // RelDataType has resolved field names, not SQL token quoting.
                name.put("name", field.getName());
                name.put("quoted", true);
                name.putObject("origin").put("kind", "absent");
                value.put("ty", id(ids, field.getType()));
            }
        } else if (type instanceof ArraySqlType || type instanceof MultisetSqlType) {
            kind.put("kind", type instanceof ArraySqlType ? "array" : "multiset");
            kind.put("element", id(ids, type.getComponentType()));
            kind.putObject("cardinality").put("kind", "unbounded");
        } else if (type instanceof MapSqlType) {
            kind.put("kind", "map");
            kind.put("key", id(ids, type.getKeyType()));
            kind.put("value", id(ids, type.getValueType()));
        } else {
            return (ObjectNode) S1ScalarType.exportType(type, new Factory()).path("types").path(0);
        }
        return entry;
    }

    private static int reference(JsonNode value, int count) throws IOException {
        if (value == null || !value.isIntegralNumber() || !value.canConvertToInt()
                || value.intValue() < 0 || value.intValue() >= count)
            throw new IOException("Type reference is outside its arena");
        return value.intValue();
    }

    private static List<Integer> references(JsonNode entry, int count) throws IOException {
        JsonNode kind = entry.path("kind");
        List<Integer> result = new ArrayList<>();
        switch (kind.path("kind").asText()) {
            case "row" -> {
                if (!kind.path("fields").isArray())
                    throw new IOException("Row fields must be an array");
                for (JsonNode field: kind.path("fields"))
                    result.add(reference(field.get("ty"), count));
            }
            case "array", "multiset" -> result.add(reference(kind.get("element"), count));
            case "map" -> {
                result.add(reference(kind.get("key"), count));
                result.add(reference(kind.get("value"), count));
            }
            default -> { /* Scalar decoder rejects unknown variants. */ }
        }
        return result;
    }

    /** Validate shared source records before passing the source-free type
     * projection to this reader. Type/field origins still must be absent. */
    static Decoded decodeWithSources(JsonNode table) throws IOException {
        S1ExpressionSources.decode(table.path("sources"));
        ObjectNode projection = table.deepCopy();
        projection.putArray("sources");
        return decode(projection);
    }

    static Decoded decode(JsonNode table) throws IOException {
        if (!table.path("types").isArray() || !table.path("roots").isArray()
                || table.path("types").size() > MAX_TYPES || table.path("roots").size() > MAX_REFERENCES)
            throw new IOException("Invalid or overlarge type arena");
        JsonNode entries = table.path("types");
        int count = entries.size();
        List<List<Integer>> edges = new ArrayList<>();
        long references = table.path("roots").size();
        for (JsonNode entry: entries) {
            List<Integer> children = references(entry, count);
            references += children.size();
            if (references > MAX_REFERENCES)
                throw new IOException("Type references exceed the validation limit");
            edges.add(children);
        }
        RelDataType[] types = new RelDataType[count];
        byte[] state = new byte[count];
        int[] heights = new int[count];
        var factory = new Factory();
        // Iterative postorder checks unreachable entries and shared-subtree depth.
        for (int root = 0; root < count; root++) {
            if (state[root] == 2)
                continue;
            List<int[]> stack = new ArrayList<>();
            stack.add(new int[]{root, 0});
            state[root] = 1;
            while (!stack.isEmpty()) {
                int[] frame = stack.getLast();
                int node = frame[0];
                if (frame[1] < edges.get(node).size()) {
                    int child = edges.get(node).get(frame[1]++);
                    if (state[child] == 1)
                        throw new IOException("Cyclic type reference");
                    if (state[child] == 0) {
                        if (stack.size() >= MAX_DEPTH)
                            throw new IOException("Type depth exceeds the validation limit");
                        state[child] = 1;
                        stack.add(new int[]{child, 0});
                    }
                } else {
                    int height = 1;
                    for (int child: edges.get(node))
                        height = Math.max(height, heights[child] + 1);
                    if (height > MAX_DEPTH)
                        throw new IOException("Type depth exceeds the validation limit");
                    heights[node] = height;
                    types[node] = decodeEntry(entries.get(node), types, factory);
                    state[node] = 2;
                    stack.removeLast();
                }
            }
        }
        List<RelDataType> roots = new ArrayList<>();
        for (JsonNode root: table.path("roots"))
            roots.add(types[reference(root, count)]);
        var decoded = new Decoded(List.of(types), roots);
        // A full re-encoding checks strict fields, variants, origins, identities,
        // cardinalities and type-factory conversions at every arena entry.
        if (!Utilities.deterministicObjectMapper().readTree(encodeArena(decoded.types(), decoded.roots()).toString()).equals(table))
            throw new IOException("Type arena is malformed, unsupported, or lossy");
        return decoded;
    }

    private static RelDataType decodeEntry(JsonNode entry, RelDataType[] types, Factory factory) throws IOException {
        JsonNode kind = entry.path("kind");
        if (!entry.path("nullable").isBoolean())
            throw new IOException("Type nullability must be Boolean");
        boolean nullable = entry.path("nullable").booleanValue();
        try {
            return switch (kind.path("kind").asText()) {
                case "row" -> {
                    List<RelDataTypeField> fields = new ArrayList<>();
                    for (JsonNode field: kind.path("fields")) {
                        JsonNode name = field.path("name").path("name");
                        if (!name.isTextual())
                            throw new IOException("Resolved row field name must be text");
                        fields.add(new RelDataTypeFieldImpl(name.textValue(), fields.size(), types[reference(field.get("ty"), types.length)]));
                    }
                    yield new RelRecordType(StructKind.valueOf(kind.path("struct_kind").asText().toUpperCase(Locale.ROOT)), fields, nullable);
                }
                case "array" -> new ArraySqlType(types[reference(kind.get("element"), types.length)], nullable);
                case "multiset" -> new MultisetSqlType(types[reference(kind.get("element"), types.length)], nullable);
                case "map" -> new MapSqlType(types[reference(kind.get("key"), types.length)], types[reference(kind.get("value"), types.length)], nullable);
                default -> {
                    ObjectNode scalar = object();
                    scalar.put("schema_version", "types-v0");
                    scalar.putArray("sources");
                    scalar.putArray("types").add(entry);
                    scalar.putArray("roots").add(0);
                    yield S1ScalarType.decode(scalar, factory);
                }
            };
        } catch (IllegalArgumentException | AssertionError ex) {
            throw new IOException("Invalid compound type", ex);
        }
    }
}
