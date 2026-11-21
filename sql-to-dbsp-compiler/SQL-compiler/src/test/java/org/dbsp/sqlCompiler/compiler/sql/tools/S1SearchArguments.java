package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.FelderaS1LiteralFactory;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sarg;
import org.dbsp.util.Utilities;

import java.io.IOException;

import static org.dbsp.sqlCompiler.compiler.sql.tools.S1MetadataWire.*;

/** SEARCH ranges remain declarative input to the unchanged lowering expansion.
 * Expanding and flattening them during transport changes observable S2 trees. */
final class S1SearchArguments {
    private S1SearchArguments() { }

    static ObjectNode encode(RexLiteral literal) throws IOException {
        check(literal.getTypeName() == SqlTypeName.SARG, "Expected a search argument literal");
        var value = literal.getValueAs(Sarg.class);
        check(value != null, "Search argument cannot be null");
        var wire = project(literal, 0);
        var restored = decode(wire, literal.getType(), 0);
        check(value.getClass() == restored.getValue().getClass() && value.equals(restored.getValue()),
                "Search argument storage changed");
        return wire;
    }

    private static ObjectNode project(RexLiteral literal, int typeId) throws IOException {
        var value = literal.getValueAs(Sarg.class);
        var wire = Utilities.deterministicObjectMapper().createObjectNode()
                .put("kind", "search_argument").put("null_as", value.nullAs.name());
        var ranges = wire.putArray("ranges");
        for (Object item: value.rangeSet.asRanges()) {
            var range = (Range<?>) item;
            var entry = ranges.addObject();
            entry.set("lower", bound(range.hasLowerBound() ? range.lowerEndpoint() : null,
                    range.hasLowerBound() && range.lowerBoundType() == BoundType.CLOSED, literal.getType(), typeId));
            entry.set("upper", bound(range.hasUpperBound() ? range.upperEndpoint() : null,
                    range.hasUpperBound() && range.upperBoundType() == BoundType.CLOSED, literal.getType(), typeId));
        }
        return wire;
    }

    private static ObjectNode bound(Comparable<?> value, boolean closed, RelDataType type, int typeId) throws IOException {
        var wire = Utilities.deterministicObjectMapper().createObjectNode();
        if (value == null) return wire.put("kind", "unbounded");
        wire.put("kind", "bounded").put("closed", closed);
        var literal = wire.putObject("literal").put("ty", typeId);
        literal.putObject("origin").put("kind", "absent");
        literal.set("value", S1LiteralPayload.encodeValue(value, type));
        return wire;
    }

    private static Comparable<?> bound(JsonNode wire, RelDataType type, int typeId) throws IOException {
        if (text(wire.path("kind")).equals("unbounded")) { fields(wire, "kind"); return null; }
        fields(wire, "kind", "closed", "literal");
        fields(wire.path("literal"), "ty", "origin", "value");
        check(unsigned(wire.at("/literal/ty")) == typeId, "Search bound type differs");
        fields(wire.at("/literal/origin"), "kind");
        check(text(wire.at("/literal/origin/kind")).equals("absent"), "Search bound origin must be absent");
        check(text(wire.path("kind")).equals("bounded"), "Unknown search bound"); bool(wire.path("closed"));
        var literal = S1LiteralPayload.decode(wire.at("/literal/value"), type);
        check(!literal.isNull(), "Search endpoint cannot be null");
        // getValue() converts temporal storage to Calendar; ranges require the
        // original DateString/TimeString/TimestampString comparison semantics.
        return literal.getValueAs(Comparable.class);
    }

    /** Calcite's Sarg uses a self-comparable generic type. The payload decoder
     * validates every endpoint against the same resolved SQL type before this
     * cast; Guava then validates interval ordering. No wire class is loaded. */
    @SuppressWarnings({"rawtypes", "unchecked"})
    static RexLiteral decode(JsonNode wire, RelDataType type, int typeId) throws IOException {
        fields(wire, "kind", "null_as", "ranges");
        check(text(wire.path("kind")).equals("search_argument"), "Unknown search argument kind");
        RexUnknownAs nullAs;
        try { nullAs = RexUnknownAs.valueOf(text(wire.path("null_as"))); }
        catch (IllegalArgumentException error) { throw new IOException("Unknown SEARCH null policy", error); }
        array(wire.path("ranges")); var ranges = ImmutableRangeSet.<Comparable>builder();
        try {
            for (var entry: wire.path("ranges")) {
                fields(entry, "lower", "upper");
                Comparable lower = bound(entry.path("lower"), type, typeId), upper = bound(entry.path("upper"), type, typeId);
                BoundType lowerType = entry.at("/lower/closed").asBoolean() ? BoundType.CLOSED : BoundType.OPEN;
                BoundType upperType = entry.at("/upper/closed").asBoolean() ? BoundType.CLOSED : BoundType.OPEN;
                Range<Comparable> range = lower == null
                        ? upper == null ? Range.all() : Range.upTo(upper, upperType)
                        : upper == null ? Range.downTo(lower, lowerType) : Range.range(lower, lowerType, upper, upperType);
                check(!range.isEmpty(), "Empty SEARCH range"); ranges.add(range);
            }
            var result = Sarg.of(nullAs, ranges.build());
            var literal = FelderaS1LiteralFactory.create(result, type, SqlTypeName.SARG);
            // A parsed small nanosecond count is an IntNode even though the
            // temporal encoder writes a LongNode. Compare JSON integer values,
            // preserving array order and every field, rather than JVM widths.
            var mapper = Utilities.deterministicObjectMapper();
            check(mapper.readTree(project(literal, typeId).toString()).equals(mapper.readTree(wire.toString())),
                    "SEARCH range ordering or bounds changed");
            return literal;
        } catch (IllegalArgumentException | ClassCastException error) {
            throw new IOException("Invalid SEARCH range ordering or endpoint type", error);
        }
    }
}
