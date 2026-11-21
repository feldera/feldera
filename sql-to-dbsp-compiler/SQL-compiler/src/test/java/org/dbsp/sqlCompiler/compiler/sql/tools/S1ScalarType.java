package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.SqlToRelCompiler;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Objects;

/** Test-only one-entry scalar type table. Compound/named types, source origins,
 * and unspecified/default precision identity still need the full S1 adapter. */
final class S1ScalarType {
    private S1ScalarType() { }

    private static ObjectNode object() {
        return Utilities.deterministicObjectMapper().createObjectNode();
    }

    private static ObjectNode precision(int precision) {
        ObjectNode result = object();
        result.put("kind", precision < 0 ? "unspecified" : "specified");
        if (precision >= 0)
            result.put("value", precision);
        return result;
    }

    static ObjectNode encode(RelDataType type) throws IOException {
        ObjectNode result = object();
        result.put("schema_version", "types-v0");
        result.putArray("sources");
        result.putArray("roots").add(0);
        ObjectNode entry = result.putArray("types").addObject();
        entry.put("nullable", type.isNullable());
        entry.putObject("origin").put("kind", "absent");
        ObjectNode kind = entry.putObject("kind");
        if (type.getIntervalQualifier() != null) {
            var qualifier = type.getIntervalQualifier();
            if (qualifier.timeFrameName != null)
                throw new IOException("Named time frames are outside the scalar type experiment");
            kind.put("kind", "interval");
            kind.put("start", qualifier.getStartUnit().name().toLowerCase(Locale.ROOT));
            ObjectNode end = kind.putObject("end");
            end.put("kind", qualifier.getEndUnit() == null ? "single" : "through");
            if (qualifier.getEndUnit() != null)
                end.put("value", qualifier.getEndUnit().name().toLowerCase(Locale.ROOT));
            kind.set("leading_precision", precision(qualifier.getStartPrecisionPreservingDefault()));
            kind.set("fractional_precision", precision(qualifier.getFractionalSecondPrecisionPreservingDefault()));
            return result;
        }
        SqlTypeName name = type.getSqlTypeName();
        switch (name) {
            case BOOLEAN -> kind.put("kind", "boolean");
            case NULL -> kind.put("kind", "null");
            case SYMBOL, VARIANT, UUID, ANY, UNKNOWN, COLUMN_LIST, CURSOR -> kind.put("kind", name.name().toLowerCase(Locale.ROOT));
            case TINYINT, SMALLINT, INTEGER, BIGINT, UTINYINT, USMALLINT, UINTEGER, UBIGINT -> {
                kind.put("kind", "integer");
                kind.put("signed", !name.name().startsWith("U"));
                kind.put("width", switch (name) {
                    case TINYINT, UTINYINT -> "i8";
                    case SMALLINT, USMALLINT -> "i16";
                    case INTEGER, UINTEGER -> "i32";
                    default -> "i64";
                });
            }
            case DECIMAL -> {
                kind.put("kind", "decimal");
                kind.put("precision", type.getPrecision());
                kind.put("scale", type.getScale());
                var factory = new SqlTypeFactoryImpl(SqlToRelCompiler.TYPE_SYSTEM);
                kind.put("explicit_scale", !sameType(type, new org.apache.calcite.sql.type.BasicSqlType(factory.getTypeSystem(), name, type.getPrecision()), factory));
            }
            case REAL, FLOAT, DOUBLE -> {
                kind.put("kind", "float");
                kind.put("flavor", name.name().toLowerCase(Locale.ROOT));
            }
            case CHAR, VARCHAR -> {
                kind.put("kind", "character");
                kind.put("fixed", name == SqlTypeName.CHAR);
                var factory = new SqlTypeFactoryImpl(SqlToRelCompiler.TYPE_SYSTEM);
                kind.set("length", precision(sameType(type, factory.createSqlType(name, RelDataType.PRECISION_NOT_SPECIFIED), factory)
                        ? RelDataType.PRECISION_NOT_SPECIFIED : type.getPrecision()));
                ObjectNode metadata = kind.putObject("metadata");
                if (type.getCharset() == null && type.getCollation() == null) {
                    metadata.put("kind", "absent");
                } else {
                    if (type.getCharset() == null || type.getCollation() == null)
                        throw new IOException("Partial character metadata");
                    metadata.put("kind", "present");
                    metadata.put("charset", type.getCharset().name());
                    metadata.put("collation", type.getCollation().getCollationName());
                    metadata.put("identity", type.getCollation() == SqlCollation.IMPLICIT ? "builtin_implicit"
                            : type.getCollation() == SqlCollation.COERCIBLE ? "builtin_coercible" : "named");
                    metadata.put("coercibility", type.getCollation().getCoercibility().name().toLowerCase(Locale.ROOT));
                }
            }
            case BINARY, VARBINARY -> {
                kind.put("kind", "binary");
                kind.put("fixed", name == SqlTypeName.BINARY);
                var factory = new SqlTypeFactoryImpl(SqlToRelCompiler.TYPE_SYSTEM);
                kind.set("length", precision(sameType(type, factory.createSqlType(name, RelDataType.PRECISION_NOT_SPECIFIED), factory)
                        ? RelDataType.PRECISION_NOT_SPECIFIED : type.getPrecision()));
            }
            case DATE -> kind.put("kind", "date");
            case GEOMETRY -> {
                kind.put("kind", "geometry");
                if (type instanceof RelDataTypeFactoryImpl.JavaType host) {
                    if (host.getJavaClass() != org.locationtech.jts.geom.Geometry.class)
                        throw new IOException("Unsupported host geometry type");
                    kind.put("representation", "host");
                } else {
                    kind.put("representation", "sql");
                }
            }
            case TIME, TIME_TZ, TIME_WITH_LOCAL_TIME_ZONE,
                    TIMESTAMP, TIMESTAMP_TZ, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> {
                kind.put("kind", switch (name) {
                    case TIME, TIME_TZ, TIME_WITH_LOCAL_TIME_ZONE -> "time";
                    default -> "timestamp";
                });
                kind.set("precision", precision(type.getPrecision()));
                kind.put("zone", switch (name) {
                    case TIME_TZ, TIMESTAMP_TZ -> "with_time_zone";
                    case TIME_WITH_LOCAL_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> "local";
                    default -> "none";
                });
            }
            default -> throw new IOException("Unsupported scalar type: " + name);
        }
        return result;
    }

    private static boolean sameType(RelDataType type, RelDataType candidate, RelDataTypeFactory factory) {
        if (type.getCharset() != null && type.getCollation() != null)
            candidate = factory.createTypeWithCharsetAndCollation(candidate, type.getCharset(), type.getCollation());
        candidate = factory.createTypeWithNullability(candidate, type.isNullable());
        return type.equals(candidate) && type.getFullTypeString().equals(candidate.getFullTypeString());
    }

    /** Fail before export if the draft wire cannot reconstruct the exact type.
     * This includes explicit FLOAT precision and some default precision forms. */
    static ObjectNode exportType(RelDataType type, RelDataTypeFactory factory) throws IOException {
        ObjectNode encoded = encode(type);
        RelDataType decoded;
        try {
            decoded = decode(Utilities.deterministicObjectMapper().readTree(encoded.toString()), factory);
        } catch (IOException ex) {
            throw new IOException("Cannot transport " + type.getFullTypeString() + ": " + ex.getMessage(), ex);
        }
        if (!type.equals(decoded) || !type.getFullTypeString().equals(decoded.getFullTypeString()))
            throw new IOException("Scalar type export would lose type identity: " + type.getFullTypeString());
        return encoded;
    }

    private static String text(JsonNode node, String field) throws IOException {
        JsonNode value = node.get(field);
        if (value == null || !value.isTextual())
            throw new IOException("Expected text field: " + field);
        return value.textValue();
    }

    private static boolean bool(JsonNode node, String field) throws IOException {
        JsonNode value = node.get(field);
        if (value == null || !value.isBoolean())
            throw new IOException("Expected Boolean field: " + field);
        return value.booleanValue();
    }

    private static int nonnegative(JsonNode node, String field) throws IOException {
        JsonNode value = node.get(field);
        if (value == null || !value.isIntegralNumber() || !value.canConvertToInt() || value.intValue() < 0)
            throw new IOException("Expected nonnegative int field: " + field);
        return value.intValue();
    }

    private static int precision(JsonNode node) throws IOException {
        return switch (text(node, "kind")) {
            case "unspecified" -> RelDataType.PRECISION_NOT_SPECIFIED;
            case "specified" -> nonnegative(node, "value");
            default -> throw new IOException("Unknown precision variant");
        };
    }

    private static TimeUnit intervalUnit(String name) throws IOException {
        return switch (name) {
            case "year" -> TimeUnit.YEAR;
            case "quarter" -> TimeUnit.QUARTER;
            case "month" -> TimeUnit.MONTH;
            case "week" -> TimeUnit.WEEK;
            case "day" -> TimeUnit.DAY;
            case "hour" -> TimeUnit.HOUR;
            case "minute" -> TimeUnit.MINUTE;
            case "second" -> TimeUnit.SECOND;
            default -> throw new IOException("Unsupported interval unit: " + name);
        };
    }

    private static int intervalRank(TimeUnit unit) {
        return switch (unit) {
            case YEAR -> 0;
            case QUARTER -> 1;
            case MONTH -> 2;
            case WEEK -> 3;
            case DAY -> 4;
            case HOUR -> 5;
            case MINUTE -> 6;
            case SECOND -> 7;
            default -> throw new IllegalArgumentException("Unsupported interval unit");
        };
    }

    static RelDataType decode(JsonNode table, RelDataTypeFactory factory) throws IOException {
        try {
            RelDataType result = decodeEntry(table.path("types").path(0), factory);
            // Exact re-encoding rejects unknown/missing fields, extra entries,
            // roots, unsupported origins, and any lossy type-factory conversion.
            var mapper = Utilities.deterministicObjectMapper();
            if (!mapper.readTree(encode(result).toString()).equals(table))
                throw new IOException("Scalar type table is unsupported, malformed, or lossy");
            return result;
        } catch (IllegalArgumentException | NullPointerException | AssertionError ex) {
            throw new IOException("Invalid scalar type table", ex);
        }
    }

    private static RelDataType decodeEntry(JsonNode entry, RelDataTypeFactory factory) throws IOException {
        JsonNode kind = entry.path("kind");
        String tag = text(kind, "kind");
        RelDataType result;
        switch (tag) {
            case "boolean", "null", "date", "symbol", "variant", "uuid", "any", "unknown", "column_list", "cursor" -> result = factory.createSqlType(SqlTypeName.valueOf(tag.toUpperCase(Locale.ROOT)));
            case "integer" -> {
                boolean signed = bool(kind, "signed");
                String name = switch (text(kind, "width")) {
                    case "i8" -> "TINYINT";
                    case "i16" -> "SMALLINT";
                    case "i32" -> "INTEGER";
                    case "i64" -> "BIGINT";
                    default -> throw new IOException("Unknown integer width");
                };
                result = factory.createSqlType(SqlTypeName.valueOf((signed ? "" : "U") + name));
            }
            case "decimal" -> {
                int precision = nonnegative(kind, "precision");
                int scale = nonnegative(kind, "scale");
                if (precision == 0 || scale > precision)
                    throw new IOException("Decimal needs positive precision and scale no greater than precision");
                result = bool(kind, "explicit_scale") ? factory.createSqlType(SqlTypeName.DECIMAL, precision, scale)
                        : new org.apache.calcite.sql.type.BasicSqlType(factory.getTypeSystem(), SqlTypeName.DECIMAL, precision);
            }
            case "geometry" -> result = switch (text(kind, "representation")) {
                case "sql" -> factory.createSqlType(SqlTypeName.GEOMETRY);
                case "host" -> factory.createJavaType(org.locationtech.jts.geom.Geometry.class);
                default -> throw new IOException("Unknown geometry representation");
            };
            case "float" -> {
                SqlTypeName flavor = switch (text(kind, "flavor")) {
                    case "real" -> SqlTypeName.REAL;
                    case "float" -> SqlTypeName.FLOAT;
                    case "double" -> SqlTypeName.DOUBLE;
                    default -> throw new IOException("Unknown float flavor");
                };
                result = factory.createSqlType(flavor);
            }
            case "character", "binary" -> {
                boolean fixed = bool(kind, "fixed");
                SqlTypeName name = tag.equals("character")
                        ? (fixed ? SqlTypeName.CHAR : SqlTypeName.VARCHAR)
                        : (fixed ? SqlTypeName.BINARY : SqlTypeName.VARBINARY);
                result = factory.createSqlType(name, precision(kind.path("length")));
                if (tag.equals("character") && text(kind.path("metadata"), "kind").equals("present")) {
                    JsonNode metadata = kind.path("metadata");
                    var collation = switch (text(metadata, "identity")) {
                        case "builtin_implicit" -> SqlCollation.IMPLICIT;
                        case "builtin_coercible" -> SqlCollation.COERCIBLE;
                        case "named" -> new SqlCollation(text(metadata, "collation"),
                                SqlCollation.Coercibility.valueOf(text(metadata, "coercibility").toUpperCase(Locale.ROOT)));
                        default -> throw new IOException("Unknown collation identity");
                    };
                    result = factory.createTypeWithCharsetAndCollation(result,
                            Charset.forName(text(metadata, "charset")), collation);
                }
            }
            case "time", "timestamp" -> {
                String suffix = switch (text(kind, "zone")) {
                    case "none" -> "";
                    case "local" -> "_WITH_LOCAL_TIME_ZONE";
                    case "with_time_zone" -> "_TZ";
                    default -> throw new IOException("Unknown temporal zone flavor");
                };
                result = factory.createSqlType(SqlTypeName.valueOf(tag.toUpperCase(Locale.ROOT) + suffix), precision(kind.path("precision")));
            }
            case "interval" -> {
                TimeUnit start = intervalUnit(text(kind, "start"));
                JsonNode end = kind.path("end");
                TimeUnit endUnit = switch (text(end, "kind")) {
                    case "single" -> null;
                    case "through" -> intervalUnit(text(end, "value"));
                    default -> throw new IOException("Unknown interval end variant");
                };
                int leading = precision(kind.path("leading_precision"));
                if (leading == 0 || (endUnit != null && (start.yearMonth != endUnit.yearMonth
                        || intervalRank(start) >= intervalRank(endUnit))))
                    throw new IOException("Invalid interval qualifier bounds or leading precision");
                result = factory.createSqlIntervalType(new SqlIntervalQualifier(start,
                        leading, endUnit, precision(kind.path("fractional_precision")), SqlParserPos.ZERO));
            }
            default -> throw new IOException("Unsupported scalar type kind: " + tag);
        }
        return factory.createTypeWithNullability(Objects.requireNonNull(result), bool(entry, "nullable"));
    }
}
