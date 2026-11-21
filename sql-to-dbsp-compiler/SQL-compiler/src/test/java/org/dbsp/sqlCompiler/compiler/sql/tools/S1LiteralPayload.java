package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.FelderaS1LiteralFactory;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.TimestampWithTimeZoneString;
import org.apache.calcite.util.UuidValue;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/** Test-only scalar payload projection. The caller owns the resolved SQL type.
 * This is not the complete S1 exporter or a type-table importer. */
final class S1LiteralPayload {
    private S1LiteralPayload() { }

    private static String kind(RelDataType type) throws IOException {
        if (type.getIntervalQualifier() != null)
            return "interval";
        return switch (type.getSqlTypeName()) {
            case BOOLEAN -> "boolean";
            case TINYINT, SMALLINT, INTEGER, BIGINT, UTINYINT, USMALLINT, UINTEGER, UBIGINT -> "integer";
            case DECIMAL -> "decimal";
            case REAL -> "float32";
            case FLOAT, DOUBLE -> "float64";
            case CHAR, VARCHAR -> "character";
            case BINARY, VARBINARY -> "binary";
            case DATE -> "date";
            case GEOMETRY -> "geometry_point";
            case TIME -> "time";
            case TIMESTAMP -> "timestamp";
            case TIMESTAMP_TZ -> "local_timestamp_with_zone";
            case UUID -> "uuid";
            case SYMBOL -> "symbol";
            default -> throw new IOException("Unsupported draft literal SQL type: " + type.getSqlTypeName());
        };
    }

    static ObjectNode encode(RexLiteral literal) throws IOException {
        if (literal.getTypeName() == SqlTypeName.SARG)
            throw new IOException("SARG must be expanded before S1 literal transport");
        ObjectNode result = Utilities.deterministicObjectMapper().createObjectNode();
        if (literal.isNull()) {
            result.put("kind", "null");
            verifyExactStorage(literal, result);
            return result;
        }
        RelDataType type = literal.getType();
        String kind = kind(type);
        result.put("kind", kind);
        switch (kind) {
            case "boolean" -> result.put("value", literal.getValueAs(Boolean.class));
            case "integer" -> result.put("value", Objects.requireNonNull(literal.getValueAs(BigDecimal.class))
                    .toBigIntegerExact().toString());
            case "decimal", "interval" -> {
                BigDecimal value = Objects.requireNonNull(literal.getValueAs(BigDecimal.class));
                ObjectNode decimal = result.putObject("value");
                decimal.put("coefficient", value.unscaledValue().toString());
                decimal.put("scale", value.scale());
                if (kind.equals("interval"))
                    result.put("unit", type.getIntervalQualifier().isYearMonth() ? "months" : "milliseconds");
            }
            case "float32" -> result.put("bits", Integer.toUnsignedLong(
                    Float.floatToRawIntBits(Objects.requireNonNull(literal.getValueAs(Float.class)))));
            case "float64" -> result.put("bits", new BigInteger(Long.toUnsignedString(
                    Double.doubleToRawLongBits(Objects.requireNonNull(literal.getValueAs(Double.class))))));
            case "character" -> result.put("value", literal.getValueAs(String.class));
            case "binary" -> {
                var bytes = result.putArray("value");
                for (byte value: Objects.requireNonNull(literal.getValueAs(ByteString.class)).getBytes())
                    bytes.add(Byte.toUnsignedInt(value));
            }
            case "symbol" -> {
                Object value = literal.getValue();
                if (value instanceof TimeUnitRange unit) {
                    result.put("domain", "time_unit_range");
                    result.put("name", unit.name());
                } else if (value instanceof SqlTrimFunction.Flag flag) {
                    result.put("domain", "trim_mode");
                    result.put("name", flag.name());
                } else {
                    throw new IOException("Unsupported symbol domain");
                }
            }
            case "uuid" -> {
                var value = Objects.requireNonNull(literal.getValueAs(UuidValue.class));
                byte[] bytes = ByteBuffer.allocate(16).putLong(value.getMostSignificantBits())
                        .putLong(value.getLeastSignificantBits()).array();
                var array = result.putArray("bytes");
                for (byte valueByte: bytes)
                    array.add(Byte.toUnsignedInt(valueByte));
            }
            case "local_timestamp_with_zone" -> {
                var value = Objects.requireNonNull(literal.getValueAs(TimestampWithTimeZoneString.class));
                var local = LocalDateTime.parse(value.getLocalTimestampString().toString().replace(' ', 'T'));
                result.put("epoch_days", local.toLocalDate().toEpochDay());
                result.put("nanoseconds_since_midnight", local.toLocalTime().toNanoOfDay());
                String text = value.toString();
                result.put("zone", text.substring(text.lastIndexOf(' ') + 1));
            }
            case "geometry_point" -> result = S1PointPayload.encode(literal);
            case "date" -> result.put("epoch_days", Objects.requireNonNull(
                    literal.getValueAs(DateString.class)).getDaysSinceEpoch());
            case "time" -> result.put("nanoseconds_since_midnight", LocalTime.parse(Objects.requireNonNull(
                    literal.getValueAs(TimeString.class)).toString()).toNanoOfDay());
            case "timestamp" -> {
                LocalDateTime value = LocalDateTime.parse(Objects.requireNonNull(
                        literal.getValueAs(TimestampString.class)).toString().replace(' ', 'T'));
                result.put("epoch_days", value.toLocalDate().toEpochDay());
                result.put("nanoseconds_since_midnight", value.toLocalTime().toNanoOfDay());
            }
            default -> throw new IOException("Unsupported draft literal kind: " + kind);
        }
        verifyExactStorage(literal, result);
        return result;
    }

    /** RexLiteral/Double equality canonicalizes NaNs. Compare storage directly
     * so an unsupported REAL payload cannot pass after losing DOUBLE bits. */
    private static void verifyExactStorage(RexLiteral original, ObjectNode payload) throws IOException {
        RexLiteral restored = decode(payload, original.getType());
        if (restored.getType() != original.getType() || restored.getTypeName() != original.getTypeName()
                || !sameStorageValue(original.getValue(), restored.getValue()))
            throw new IOException("Literal payload would lose exact Calcite storage identity");
    }

    private static boolean sameStorageValue(Object original, Object restored) {
        if (original == null || restored == null)
            return original == restored;
        if (original.getClass() != restored.getClass())
            return false;
        if (original instanceof Double value)
            return Double.doubleToRawLongBits(value) == Double.doubleToRawLongBits((Double) restored);
        if (original instanceof Float value)
            return Float.floatToRawIntBits(value) == Float.floatToRawIntBits((Float) restored);
        return original.equals(restored);
    }

    private static void fields(JsonNode node, String... names) throws IOException {
        Set<String> actual = new HashSet<>();
        node.fieldNames().forEachRemaining(actual::add);
        if (!node.isObject() || !actual.equals(Set.of(names)))
            throw new IOException("Unexpected or missing literal payload fields: " + actual);
    }

    private static String text(JsonNode node, String name) throws IOException {
        JsonNode value = node.get(name);
        if (value == null || !value.isTextual())
            throw new IOException("Literal field requires text: " + name);
        return value.asText();
    }

    private static BigInteger integer(JsonNode node, String name, int bits) throws IOException {
        JsonNode value = node.get(name);
        if (value == null || !value.isIntegralNumber() || value.bigIntegerValue().signum() < 0
                || value.bigIntegerValue().bitLength() > bits)
            throw new IOException("Literal field requires unsigned " + bits + "-bit integer: " + name);
        return value.bigIntegerValue();
    }

    private static BigDecimal decimal(JsonNode node) throws IOException {
        fields(node, "coefficient", "scale");
        String coefficient = text(node, "coefficient");
        if (coefficient.length() > 10000 || !coefficient.matches("[+-]?[0-9]+")
                || !node.path("scale").isIntegralNumber() || !node.path("scale").canConvertToInt()
                || Math.abs(node.path("scale").asLong()) > 10000)
            throw new IOException("Invalid exact decimal payload");
        return new BigDecimal(new BigInteger(coefficient), node.get("scale").asInt());
    }

    static RexLiteral decode(JsonNode payload, RelDataType type) throws IOException {
        String kind = text(payload, "kind");
        if (kind.equals("null")) {
            fields(payload, "kind");
            if (!type.isNullable())
                throw new IOException("NULL literal requires a nullable SQL type");
            return FelderaS1LiteralFactory.create(null, type, SqlTypeName.NULL);
        }
        if (!kind.equals(kind(type)))
            throw new IOException("Literal payload kind does not match resolved SQL type");
        Object value;
        switch (kind) {
            case "boolean" -> {
                fields(payload, "kind", "value");
                if (!payload.path("value").isBoolean())
                    throw new IOException("Boolean literal requires a boolean value");
                value = payload.get("value").asBoolean();
            }
            case "integer" -> {
                fields(payload, "kind", "value");
                String coefficient = text(payload, "value");
                if (coefficient.length() > 10000 || !coefficient.matches("[+-]?[0-9]+"))
                    throw new IOException("Invalid integer coefficient");
                BigInteger number = new BigInteger(coefficient);
                int width = switch (type.getSqlTypeName()) {
                    case TINYINT, UTINYINT -> 8;
                    case SMALLINT, USMALLINT -> 16;
                    case INTEGER, UINTEGER -> 32;
                    case BIGINT, UBIGINT -> 64;
                    default -> throw new IOException("Unsupported integer type");
                };
                boolean unsigned = type.getSqlTypeName().name().startsWith("U");
                BigInteger bound = BigInteger.ONE.shiftLeft(unsigned ? width : width - 1);
                BigInteger minimum = unsigned ? BigInteger.ZERO : bound.negate();
                if (number.compareTo(minimum) < 0 || number.compareTo(bound.subtract(BigInteger.ONE)) > 0)
                    throw new IOException("Integer payload is outside the resolved SQL type");
                value = new BigDecimal(number);
            }
            case "decimal", "interval" -> {
                if (kind.equals("interval")) {
                    fields(payload, "kind", "value", "unit");
                    String expected = type.getIntervalQualifier().isYearMonth() ? "months" : "milliseconds";
                    if (!text(payload, "unit").equals(expected))
                        throw new IOException("Interval storage unit disagrees with SQL qualifier");
                } else {
                    fields(payload, "kind", "value");
                }
                value = decimal(payload.get("value"));
            }
            case "float32" -> {
                fields(payload, "kind", "bits");
                value = Float.intBitsToFloat(integer(payload, "bits", 32).intValue());
            }
            case "float64" -> {
                fields(payload, "kind", "bits");
                value = Double.longBitsToDouble(integer(payload, "bits", 64).longValue());
            }
            case "character" -> {
                fields(payload, "kind", "value");
                value = new NlsString(text(payload, "value"), Objects.requireNonNull(type.getCharset()).name(),
                        type.getCollation());
            }
            case "binary" -> {
                fields(payload, "kind", "value");
                JsonNode bytes = payload.get("value");
                if (!bytes.isArray())
                    throw new IOException("Binary literal requires byte array");
                byte[] decoded = new byte[bytes.size()];
                for (int index = 0; index < decoded.length; index++) {
                    JsonNode item = bytes.get(index);
                    if (!item.isIntegralNumber() || !item.canConvertToInt() || item.asInt() < 0 || item.asInt() > 255)
                        throw new IOException("Binary literal byte is outside 0..255");
                    decoded[index] = (byte) item.asInt();
                }
                value = new ByteString(decoded);
            }
            case "symbol" -> {
                fields(payload, "kind", "domain", "name");
                try {
                    value = switch (text(payload, "domain")) {
                        case "time_unit_range" -> TimeUnitRange.valueOf(text(payload, "name"));
                        case "trim_mode" -> SqlTrimFunction.Flag.valueOf(text(payload, "name"));
                        default -> throw new IOException("Unsupported symbol domain");
                    };
                } catch (IllegalArgumentException ex) {
                    throw new IOException("Unknown symbol name", ex);
                }
            }
            case "uuid" -> {
                fields(payload, "kind", "bytes");
                JsonNode bytes = payload.get("bytes");
                if (!bytes.isArray() || bytes.size() != 16)
                    throw new IOException("UUID requires sixteen bytes");
                var buffer = ByteBuffer.allocate(16);
                for (JsonNode item: bytes) {
                    if (!item.isIntegralNumber() || !item.canConvertToInt() || item.intValue() < 0 || item.intValue() > 255)
                        throw new IOException("UUID byte is outside 0..255");
                    buffer.put((byte) item.intValue());
                }
                buffer.flip();
                value = new UuidValue(new UUID(buffer.getLong(), buffer.getLong()));
            }
            case "local_timestamp_with_zone" -> {
                fields(payload, "kind", "epoch_days", "nanoseconds_since_midnight", "zone");
                JsonNode days = payload.get("epoch_days");
                if (!days.isIntegralNumber() || !days.canConvertToInt())
                    throw new IOException("Local zoned timestamp requires signed day count");
                long nanos = integer(payload, "nanoseconds_since_midnight", 47).longValue();
                String zone = text(payload, "zone");
                if (nanos >= 86400000000000L || zone.isEmpty())
                    throw new IOException("Invalid local zoned timestamp fields");
                String clock = LocalTime.ofNanoOfDay(nanos).format(java.time.format.DateTimeFormatter.ISO_LOCAL_TIME);
                try {
                    String local = LocalDate.ofEpochDay(days.intValue()) + " " + clock;
                    value = new TimestampWithTimeZoneString(local + " " + zone);
                } catch (IllegalArgumentException | CalciteException ex) {
                    throw new IOException("Invalid local zoned timestamp", ex);
                }
            }
            case "geometry_point" -> value = S1PointPayload.decode(payload);
            case "date" -> {
                fields(payload, "kind", "epoch_days");
                JsonNode days = payload.get("epoch_days");
                if (!days.isIntegralNumber() || !days.canConvertToInt())
                    throw new IOException("Date literal requires signed day count");
                value = new DateString(LocalDate.ofEpochDay(days.asInt()).toString());
            }
            case "time", "timestamp" -> {
                if (kind.equals("time")) fields(payload, "kind", "nanoseconds_since_midnight");
                else fields(payload, "kind", "epoch_days", "nanoseconds_since_midnight");
                long nanos = integer(payload, "nanoseconds_since_midnight", 47).longValue();
                if (nanos >= 86400000000000L)
                    throw new IOException("Time literal is outside the day");
                LocalTime time = LocalTime.ofNanoOfDay(nanos);
                String clock = time.format(java.time.format.DateTimeFormatter.ISO_LOCAL_TIME);
                if (kind.equals("time")) value = new TimeString(clock);
                else {
                    JsonNode days = payload.get("epoch_days");
                    if (!days.isIntegralNumber() || !days.canConvertToInt())
                        throw new IOException("Timestamp literal requires signed day count");
                    value = new TimestampString(LocalDate.ofEpochDay(days.asInt()) + " " + clock);
                }
            }
            default -> throw new IOException("Unsupported draft literal kind: " + kind);
        }
        var storage = storageType(kind, type);
        if (kind.equals("float32")) value = ((Float) value).doubleValue();
        RexLiteral result = FelderaS1LiteralFactory.create((Comparable<?>) value, type, storage);
        if (!result.getType().equals(type))
            throw new IOException("Literal builder changed SQL type from " + type.getFullTypeString()
                    + " to " + result.getType().getFullTypeString());
        return result;
    }

    static ObjectNode encodeValue(Comparable<?> value, RelDataType type) throws IOException {
        return encode(FelderaS1LiteralFactory.create(value, type, storageType(kind(type), type)));
    }

    private static SqlTypeName storageType(String kind, RelDataType type) {
        return switch (kind) {
            case "integer", "decimal" -> SqlTypeName.DECIMAL;
            case "float32", "float64" -> SqlTypeName.DOUBLE;
            case "character" -> SqlTypeName.CHAR;
            case "binary" -> SqlTypeName.BINARY;
            default -> RexLiteral.strictTypeName(type);
        };
    }
}
