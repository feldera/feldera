package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableIntList;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Common metadata has fixed constructors and a round-trip check. Unknown trait
 * definitions or conventions require an adapter instead of display-name JSON. */
final class S1RelationMetadata {
    record Decoded(RelTraitSet traits, List<RelHint> hints) { }
    private S1RelationMetadata() { }

    static ObjectNode encode(RelNode relation) throws IOException {
        var hints = relation instanceof Hintable hintable ? hintable.getHints() : List.<RelHint>of();
        var wire = encode(relation.getTraitSet(), hints);
        var decoded = decode(wire, relation.getRowType().getFieldCount());
        check(decoded.traits().equals(relation.getTraitSet()), "Trait reconstruction changed metadata");
        check(decoded.hints().size() == hints.size(), "Hint count changed");
        for (int index = 0; index < hints.size(); index++)
            check(hints.get(index).identical(decoded.hints().get(index)), "Hint reconstruction changed metadata");
        return wire;
    }

    static ObjectNode encode(RelTraitSet traits, List<RelHint> hints) throws IOException {
        var result = Utilities.deterministicObjectMapper().createObjectNode();
        var entries = result.putArray("traits");
        for (int index = 0; index < traits.size(); index++) {
            var trait = traits.get(index);
            check(trait != null, "Null trait requires an explicit adapter");
            var entry = entries.addObject();
            if (trait.getTraitDef() == ConventionTraitDef.INSTANCE) {
                check(trait == Convention.NONE, "Non-logical convention requires a catalog adapter");
                entry.put("kind", "convention_none");
            } else if (trait.getTraitDef() == RelCollationTraitDef.INSTANCE) {
                entry.put("kind", "collation"); var values = entry.putArray("values");
                for (var value: traits.<RelCollation>getTraits(index)) values.add(collation(value));
            } else if (trait.getTraitDef() == RelDistributionTraitDef.INSTANCE) {
                entry.put("kind", "distribution"); var values = entry.putArray("values");
                for (var value: traits.<RelDistribution>getTraits(index)) {
                    var item = values.addObject().put("distribution", value.getType().name());
                    var keys = item.putArray("keys"); value.getKeys().forEach(keys::add);
                }
            } else throw new IOException("Unknown trait definition: " + trait.getTraitDef().getClass().getName());
        }
        result.set("hints", hints(hints));
        return result;
    }

    static Decoded decode(JsonNode wire, int outputFields) throws IOException {
        fields(wire, "traits", "hints"); array(wire.path("traits")); array(wire.path("hints"));
        var traits = RelTraitSet.createEmpty(); var definitions = new HashSet<String>();
        for (var entry: wire.path("traits")) {
            String kind = text(entry, "kind"); check(definitions.add(kind), "Duplicate trait definition");
            switch (kind) {
                case "convention_none" -> { fields(entry, "kind"); traits = traits.plus(Convention.NONE); }
                case "collation" -> {
                    fields(entry, "kind", "values"); array(entry.path("values"));
                    var values = new ArrayList<RelCollation>();
                    for (var value: entry.path("values")) values.add(collation(value, outputFields));
                    ordered(values);
                    traits = traits.plus(RelCollations.EMPTY).replace(RelCollationTraitDef.INSTANCE, values);
                }
                case "distribution" -> {
                    fields(entry, "kind", "values"); array(entry.path("values"));
                    var values = new ArrayList<RelDistribution>();
                    for (var value: entry.path("values")) {
                        fields(value, "distribution", "keys");
                        var kindValue = enumValue(RelDistribution.Type.class, text(value, "distribution"));
                        var keys = integers(value.path("keys"));
                        for (int key: keys) check(key < outputFields, "Distribution key outside output row");
                        check(kindValue == RelDistribution.Type.HASH_DISTRIBUTED || kindValue == RelDistribution.Type.RANGE_DISTRIBUTED
                                || keys.isEmpty(), "Distribution does not accept keys");
                        if (kindValue == RelDistribution.Type.HASH_DISTRIBUTED)
                            for (int i = 1; i < keys.size(); i++) check(keys.get(i - 1) <= keys.get(i), "Unordered hash keys");
                        values.add(RelDistributions.of(kindValue, ImmutableIntList.copyOf(keys)));
                    }
                    ordered(values);
                    traits = traits.plus(RelDistributions.ANY).replace(RelDistributionTraitDef.INSTANCE, values);
                }
                default -> throw new IOException("Unknown trait kind");
            }
        }
        var hints = hints(wire.path("hints"));
        // Constructors may canonicalize lists. Reject wire that loses order or
        // fields instead of silently accepting a different descriptor.
        check(wire.equals(encode(traits, hints)), "Metadata changed during reconstruction");
        return new Decoded(traits, List.copyOf(hints));
    }

    static ArrayNode hints(List<RelHint> hints) {
        var hintEntries = Utilities.deterministicObjectMapper().createArrayNode();
        for (var hint: hints) {
            var entry = hintEntries.addObject().put("name", hint.hintName);
            entry.set("position", position(hint.pos));
            var path = entry.putArray("inherit_path"); hint.inheritPath.forEach(path::add);
            var list = entry.putArray("list"); hint.listOptions.forEach(list::add);
            var options = entry.putArray("options");
            var keys = new ArrayList<>(hint.kvOptions.keySet());
            // Match Rust's UTF-8 ordering even for supplementary characters.
            keys.sort((left, right) -> java.util.Arrays.compareUnsigned(
                    left.getBytes(StandardCharsets.UTF_8), right.getBytes(StandardCharsets.UTF_8)));
            for (var key: keys) options.addObject().put("key", key).put("value", hint.kvOptions.get(key));
        }
        return hintEntries;
    }

    static List<RelHint> hints(JsonNode wire) throws IOException {
        array(wire);
        var hints = new ArrayList<RelHint>();
        for (var entry: wire) {
            fields(entry, "name", "position", "inherit_path", "list", "options");
            String name = text(entry, "name"); check(!name.isEmpty(), "Empty hint name");
            var builder = RelHint.builder(name).position(position(entry.path("position")))
                    .inheritPath(integers(entry.path("inherit_path")));
            array(entry.path("list")); array(entry.path("options"));
            check(entry.path("list").isEmpty() || entry.path("options").isEmpty(), "Mixed hint options");
            for (var option: entry.path("list")) { check(option.isTextual(), "Invalid hint option"); builder.hintOption(option.textValue()); }
            var keys = new HashSet<String>();
            for (var option: entry.path("options")) {
                fields(option, "key", "value"); String key = text(option, "key");
                check(keys.add(key), "Duplicate hint option"); builder.hintOption(key, text(option, "value"));
            }
            hints.add(builder.build());
        }
        check(wire.equals(hints(hints)), "Hints changed during reconstruction");
        return List.copyOf(hints);
    }

    static ArrayNode collation(RelCollation collation) {
        var result = Utilities.deterministicObjectMapper().createArrayNode();
        for (var field: collation.getFieldCollations()) result.addObject().put("field", field.getFieldIndex())
                .put("direction", field.direction.name()).put("null_direction", field.nullDirection.name());
        return result;
    }
    static ObjectNode position(SqlParserPos position) {
        return Utilities.deterministicObjectMapper().createObjectNode().put("line", position.getLineNum()).put("column", position.getColumnNum())
                .put("end_line", position.getEndLineNum()).put("end_column", position.getEndColumnNum())
                .put("quoted", position.isQuoted());
    }

    private static void ordered(List<? extends org.apache.calcite.plan.RelMultipleTrait> values) throws IOException {
        check(!values.isEmpty(), "Empty trait alternatives");
        for (int index = 1; index < values.size(); index++)
            check(values.get(index - 1).compareTo(values.get(index)) < 0, "Trait alternatives are not strictly ordered");
    }

    static RelCollation collation(JsonNode wire, int outputFields) throws IOException {
        array(wire); var fields = new ArrayList<RelFieldCollation>(); var seen = new HashSet<Integer>();
        for (var field: wire) {
            fields(field, "field", "direction", "null_direction"); int index = unsigned(field.path("field"));
            check(index < outputFields && seen.add(index), "Invalid collation field");
            fields.add(new RelFieldCollation(index,
                    enumValue(RelFieldCollation.Direction.class, text(field, "direction")),
                    enumValue(RelFieldCollation.NullDirection.class, text(field, "null_direction"))));
        }
        return RelCollations.of(fields);
    }
    static SqlParserPos position(JsonNode wire) throws IOException {
        fields(wire, "line", "column", "end_line", "end_column", "quoted");
        check(wire.path("quoted").isBoolean(), "Invalid position quoting");
        return new SqlParserPos(unsigned(wire.path("line")), unsigned(wire.path("column")),
                unsigned(wire.path("end_line")), unsigned(wire.path("end_column"))).withQuoting(wire.path("quoted").booleanValue());
    }
    private static <T extends Enum<T>> T enumValue(Class<T> type, String value) throws IOException {
        try { return Enum.valueOf(type, value); }
        catch (IllegalArgumentException exception) { throw new IOException("Unknown " + type.getSimpleName() + ": " + value, exception); }
    }
    private static List<Integer> integers(JsonNode wire) throws IOException {
        array(wire); var values = new ArrayList<Integer>();
        for (var value: wire) values.add(unsigned(value)); return values;
    }
    private static int unsigned(JsonNode value) throws IOException {
        check(value.isIntegralNumber() && value.canConvertToInt() && value.intValue() >= 0, "Invalid metadata integer"); return value.intValue();
    }
    private static String text(JsonNode wire, String name) throws IOException {
        check(wire.path(name).isTextual(), "Invalid metadata string"); return wire.path(name).textValue();
    }
    private static void array(JsonNode wire) throws IOException { check(wire.isArray() && wire.size() <= 1_000_000, "Invalid metadata array"); }
    private static void fields(JsonNode wire, String... names) throws IOException {
        check(wire.isObject(), "Invalid metadata object"); var fields = new HashSet<String>(); wire.fieldNames().forEachRemaining(fields::add);
        check(fields.equals(Set.of(names)), "Unknown or missing metadata fields");
    }
    private static void check(boolean condition, String message) throws IOException { if (!condition) throw new IOException(message); }
}
