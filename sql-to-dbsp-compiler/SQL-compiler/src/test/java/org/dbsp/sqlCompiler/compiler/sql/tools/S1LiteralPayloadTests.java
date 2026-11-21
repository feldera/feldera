package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.SqlToRelCompiler;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class S1LiteralPayloadTests {
    @Test public void searchArgumentsRequireExpansionBeforeLiteralTransport() {
        var factory = new S1TypeTable.Factory();
        var builder = new RexBuilder(factory);
        var type = factory.createSqlType(SqlTypeName.INTEGER);
        var ranges = com.google.common.collect.ImmutableRangeSet.of(
                com.google.common.collect.Range.singleton(BigDecimal.ONE));
        var search = org.apache.calcite.util.Sarg.of(org.apache.calcite.rex.RexUnknownAs.UNKNOWN, ranges);
        var literal = builder.makeSearchArgumentLiteral(search, type);
        var error = Assert.assertThrows(IOException.class, () -> S1LiteralPayload.encode(literal));
        Assert.assertTrue(error.getMessage().contains("expanded"));
    }
    static SqlTypeFactoryImpl factory() {
        return new SqlTypeFactoryImpl(SqlToRelCompiler.TYPE_SYSTEM) {
            @Override
            public java.nio.charset.Charset getDefaultCharset() {
                return java.nio.charset.StandardCharsets.UTF_8;
            }
        };
    }

    static List<RexLiteral> fixtures(RexBuilder builder) {
        var factory = builder.getTypeFactory();
        List<RexLiteral> result = new ArrayList<>();
        result.add(builder.makeLiteral(true));
        result.add(builder.makeLiteral(false));
        for (SqlTypeName type: List.of(SqlTypeName.TINYINT, SqlTypeName.SMALLINT, SqlTypeName.INTEGER,
                SqlTypeName.BIGINT, SqlTypeName.UTINYINT, SqlTypeName.USMALLINT, SqlTypeName.UINTEGER,
                SqlTypeName.UBIGINT)) {
            RelDataType sqlType = factory.createSqlType(type);
            result.add(builder.makeExactLiteral(BigDecimal.ZERO, sqlType));
            result.add(builder.makeExactLiteral(BigDecimal.ONE, sqlType));
            result.add(builder.makeNullLiteral(sqlType));
        }
        result.add(builder.makeExactLiteral(new BigDecimal("18446744073709551615"),
                factory.createSqlType(SqlTypeName.UBIGINT)));
        result.add(builder.makeExactLiteral(new BigDecimal("-9223372036854775808"),
                factory.createSqlType(SqlTypeName.BIGINT)));
        for (BigDecimal value: List.of(new BigDecimal("1234567890123456789012345678.1234567890"),
                new BigDecimal("0.0000000001"), new BigDecimal("-0.0000000001")))
            result.add(builder.makeExactLiteral(value, factory.createSqlType(SqlTypeName.DECIMAL, 38, 10)));
        result.add(builder.makeExactLiteral(new BigDecimal("1E+9"), factory.createSqlType(SqlTypeName.DECIMAL, 38, 0)));
        for (SqlTypeName type: List.of(SqlTypeName.REAL, SqlTypeName.FLOAT, SqlTypeName.DOUBLE)) {
            RelDataType sqlType = factory.createSqlType(type);
            for (double value: new double[]{0.0, -0.0, 1.25, -1.25, 1e-30, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.longBitsToDouble(0x7ff8000000000042L)})
                result.add(builder.makeApproxLiteral(value, sqlType));
        }
        result.add(builder.makeLiteral("a  ", factory.createSqlType(SqlTypeName.CHAR, 3)));
        result.add(builder.makeLiteral("🦀", factory.createSqlType(SqlTypeName.VARCHAR, 5)));
        result.add(builder.makeLiteral(new ByteString(new byte[]{0, -1, 42, 0}),
                factory.createSqlType(SqlTypeName.BINARY, 4)));
        result.add(builder.makeDateLiteral(new DateString("1969-12-31")));
        result.add(builder.makeDateLiteral(new DateString("2024-02-29")));
        result.add(builder.makeTimeLiteral(new TimeString("00:00:00"), 6));
        result.add(builder.makeTimeLiteral(new TimeString("23:59:59.123456"), 6));
        result.add(builder.makeTimestampLiteral(new TimestampString("1969-12-31 23:59:59.123456"), 6));
        result.add(builder.makeTimestampLiteral(new TimestampString("2024-02-29 00:00:00"), 6));
        TimeUnit[][] ranges = {{TimeUnit.YEAR, null}, {TimeUnit.YEAR, TimeUnit.MONTH}, {TimeUnit.MONTH, null},
                {TimeUnit.DAY, null}, {TimeUnit.DAY, TimeUnit.HOUR}, {TimeUnit.DAY, TimeUnit.MINUTE},
                {TimeUnit.DAY, TimeUnit.SECOND}, {TimeUnit.HOUR, null}, {TimeUnit.HOUR, TimeUnit.MINUTE},
                {TimeUnit.HOUR, TimeUnit.SECOND}, {TimeUnit.MINUTE, null}, {TimeUnit.MINUTE, TimeUnit.SECOND},
                {TimeUnit.SECOND, null}};
        for (TimeUnit[] range: ranges) {
            var qualifier = new SqlIntervalQualifier(range[0], range[1], SqlParserPos.ZERO);
            result.add(builder.makeIntervalLiteral(new BigDecimal("-12"), qualifier));
            result.add(builder.makeNullLiteral(factory.createSqlIntervalType(qualifier)));
        }
        RelDataType geometryType = factory.createSqlType(SqlTypeName.GEOMETRY);
        var sequences = org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory.DOUBLE_FACTORY;
        for (int[] layout: new int[][]{{2, 0}, {3, 0}, {3, 1}, {4, 1}}) {
            var geometryFactory = new org.locationtech.jts.geom.GeometryFactory(
                    new org.locationtech.jts.geom.PrecisionModel(), 4326, sequences);
            for (boolean empty: new boolean[]{false, true}) {
                var sequence = sequences.create(empty ? 0 : 1, layout[0], layout[1]);
                if (!empty) {
                    sequence.setOrdinate(0, 0, -0.0);
                    sequence.setOrdinate(0, 1, 1.25);
                    for (int ordinate = 2; ordinate < layout[0]; ordinate++)
                        sequence.setOrdinate(0, ordinate, Double.longBitsToDouble(0x7ff8000000000042L));
                }
                result.add(builder.makeLiteral(geometryFactory.createPoint(sequence), geometryType));
            }
        }
        result.add(builder.makeLiteral(new org.locationtech.jts.geom.GeometryFactory().createPoint(
                new org.locationtech.jts.geom.Coordinate(1.25, -2.5, 3.75)), geometryType));
        result.add(builder.makeLiteral(new org.locationtech.jts.geom.GeometryFactory().createPoint(
                new org.locationtech.jts.geom.Coordinate(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)), geometryType));
        result.add(builder.makeNullLiteral(geometryType));
        for (var unit: org.apache.calcite.avatica.util.TimeUnitRange.values())
            result.add(builder.makeFlag(unit));
        for (var flag: org.apache.calcite.sql.fun.SqlTrimFunction.Flag.values())
            result.add(builder.makeFlag(flag));
        for (String uuid: List.of("00000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff",
                "550e8400-e29b-41d4-a716-446655440000"))
            result.add(builder.makeUuidLiteral(java.util.UUID.fromString(uuid)));
        result.add(builder.makeNullLiteral(factory.createSqlType(SqlTypeName.UUID)));
        for (String timestamp: List.of("2000-01-01 00:00:00 UTC", "1969-12-31 23:59:59.123456 UTC",
                "2024-03-10 01:30:00 America/New_York", "2024-03-10 03:30:00 America/New_York", "2024-11-03 01:30:00 America/New_York",
                "2024-01-01 00:00:00 GMT+05:30", "2024-01-01 00:00:00 Etc/GMT+1"))
            result.add(builder.makeTimestampTzLiteral(new org.apache.calcite.util.TimestampWithTimeZoneString(
                    new TimestampString(timestamp.substring(0, timestamp.lastIndexOf(' '))),
                    java.util.TimeZone.getTimeZone(timestamp.substring(timestamp.lastIndexOf(' ') + 1))), 6));
        result.add(builder.makeNullLiteral(factory.createSqlType(SqlTypeName.TIMESTAMP_TZ, 6)));
        return result;
    }

    @Test
    public void scalarPayloadsRoundTripWithExactTypesAndValues() throws IOException {
        var factory = factory();
        var builder = new RexBuilder(factory);
        for (RexLiteral original: fixtures(builder)) {
            var mapper = Utilities.deterministicObjectMapper();
            var payload = mapper.readTree(S1LiteralPayload.encode(original).toString());
            RexLiteral decoded = S1LiteralPayload.decode(payload, original.getType());
            Assert.assertEquals(original.toString(), original.getType(), decoded.getType());
            Assert.assertEquals(original.toString(), payload, mapper.readTree(S1LiteralPayload.encode(decoded).toString()));
            Assert.assertEquals(original.toString(), original, decoded);
        }
    }

    @Test
    public void resolvedVarcharAndRawDecimalTypesAreNotReinferredOnImport() throws IOException {
        var factory = new S1TypeTable.Factory();
        var builder = new RexBuilder(factory);
        var mapper = Utilities.deterministicObjectMapper();
        for (var type: List.of(factory.createSqlType(SqlTypeName.VARCHAR, -1),
                new org.apache.calcite.sql.type.BasicSqlType(factory.getTypeSystem(), SqlTypeName.DECIMAL, 38))) {
            ObjectNode payload = mapper.createObjectNode();
            if (type.getSqlTypeName() == SqlTypeName.VARCHAR) {
                payload.put("kind", "character");
                payload.put("value", "abc");
            } else {
                payload.put("kind", "decimal");
                payload.putObject("value").put("coefficient", "42").put("scale", 0);
            }
            var restored = S1LiteralPayload.decode(payload, type);
            Assert.assertEquals(type, restored.getType());
            Assert.assertEquals(type.getFullTypeString(), restored.getType().getFullTypeString());
            Assert.assertEquals(mapper.readTree(payload.toString()), mapper.readTree(S1LiteralPayload.encode(restored).toString()));
        }
    }

    @Test
    public void symbolUuidAndZonedPayloadsRejectMalformedFields() throws IOException {
        var factory = new S1TypeTable.Factory();
        var builder = new RexBuilder(factory);
        var mapper = Utilities.deterministicObjectMapper();
        ObjectNode symbol = S1LiteralPayload.encode(builder.makeFlag(org.apache.calcite.avatica.util.TimeUnitRange.DAY));
        symbol.put("domain", "arbitrary.class.name");
        Assert.assertThrows(IOException.class, () -> S1LiteralPayload.decode(symbol, factory.createSqlType(SqlTypeName.SYMBOL)));
        symbol.put("domain", "time_unit_range");
        symbol.put("name", "NOT_A_UNIT");
        Assert.assertThrows(IOException.class, () -> S1LiteralPayload.decode(symbol, factory.createSqlType(SqlTypeName.SYMBOL)));
        var uuid = S1LiteralPayload.encode(builder.makeUuidLiteral(new java.util.UUID(0, 0)));
        uuid.withArray("bytes").remove(0);
        Assert.assertThrows(IOException.class, () -> S1LiteralPayload.decode(uuid, factory.createSqlType(SqlTypeName.UUID)));
        var zoned = S1LiteralPayload.encode(builder.makeTimestampTzLiteral(
                new org.apache.calcite.util.TimestampWithTimeZoneString(new TimestampString("2024-03-10 01:30:00"),
                        java.util.TimeZone.getTimeZone("America/New_York")), 6));
        var gap = zoned.deepCopy();
        gap.put("nanoseconds_since_midnight", 9_000_000_000_000L);
        Assert.assertThrows(IOException.class, () -> S1LiteralPayload.decode(gap, factory.createSqlType(SqlTypeName.TIMESTAMP_TZ, 6)));
        zoned.put("zone", "");
        Assert.assertThrows(IOException.class, () -> S1LiteralPayload.decode(zoned, factory.createSqlType(SqlTypeName.TIMESTAMP_TZ, 6)));
    }

    @Test
    public void s1LiteralCollectionExpandsSearchArgumentsLikeTheLowering() {
        var compiler = new org.dbsp.sqlCompiler.compiler.DBSPCompiler(new org.dbsp.sqlCompiler.compiler.CompilerOptions());
        compiler.submitStatementsForCompilation("CREATE TABLE t(x INT, y INT, z INT); CREATE VIEW v AS SELECT * FROM t WHERE x IN (1,3,5,7,9) AND y BETWEEN 2 AND 8 AND z > 0;");
        Assert.assertNotNull(compiler.getFinalCircuit(false));
        compiler.throwIfErrorsOccurred();
        var capture = S1TypeCorpusProbe.literals(compiler);
        Assert.assertTrue(capture.expandedSearchCalls() > 0);
        Assert.assertTrue(capture.literals().stream().noneMatch(literal -> literal.getTypeName() == SqlTypeName.SARG));
    }

    @Test
    public void nullCollectionsKeepTheirExactArenaTypeObject() throws IOException {
        var factory = new S1TypeTable.Factory();
        var builder = new RexBuilder(factory);
        var integer = factory.createSqlType(SqlTypeName.INTEGER);
        var payload = Utilities.deterministicObjectMapper().createObjectNode().put("kind", "null");
        for (var type: List.of(new org.apache.calcite.sql.type.ArraySqlType(integer, true),
                new org.apache.calcite.sql.type.MultisetSqlType(integer, true),
                new org.apache.calcite.sql.type.MapSqlType(integer, integer, true))) {
            var restored = S1LiteralPayload.decode(payload, type);
            Assert.assertTrue(restored.isNull());
            Assert.assertSame(type, restored.getType());
        }
    }

    @Test
    public void integerOverflowAndUnboundedDecimalScalesAreRejected() {
        var factory = factory();
        var builder = new RexBuilder(factory);
        var mapper = Utilities.deterministicObjectMapper();
        ObjectNode payload = mapper.createObjectNode();
        payload.put("kind", "integer");
        payload.put("value", "256");
        Assert.assertThrows(IOException.class, () -> S1LiteralPayload.decode(payload,
                factory.createSqlType(SqlTypeName.UTINYINT)));
        payload.put("value", "-1");
        Assert.assertThrows(IOException.class, () -> S1LiteralPayload.decode(payload,
                factory.createSqlType(SqlTypeName.UBIGINT)));
        ObjectNode decimal = mapper.createObjectNode();
        decimal.put("kind", "decimal");
        var value = decimal.putObject("value");
        value.put("coefficient", "1");
        value.put("scale", Integer.MIN_VALUE);
        Assert.assertThrows(IOException.class, () -> S1LiteralPayload.decode(decimal,
                factory.createSqlType(SqlTypeName.DECIMAL, 38, 0)));
    }

    @Test
    public void malformedPayloadsAndWrongResolvedTypesFail() throws IOException {
        var factory = factory();
        var builder = new RexBuilder(factory);
        ObjectNode integer = S1LiteralPayload.encode(builder.makeExactLiteral(BigDecimal.ONE,
                factory.createSqlType(SqlTypeName.INTEGER)));
        Assert.assertThrows(IOException.class, () -> S1LiteralPayload.decode(integer,
                factory.createSqlType(SqlTypeName.BOOLEAN)));
        integer.put("extra", true);
        Assert.assertThrows(IOException.class, () -> S1LiteralPayload.decode(integer,
                factory.createSqlType(SqlTypeName.INTEGER)));
        ObjectNode real = S1LiteralPayload.encode(builder.makeApproxLiteral(1.25,
                factory.createSqlType(SqlTypeName.REAL)));
        real.put("bits", -1);
        Assert.assertThrows(IOException.class, () -> S1LiteralPayload.decode(real,
                factory.createSqlType(SqlTypeName.REAL)));
        ObjectNode nullValue = Utilities.deterministicObjectMapper().createObjectNode();
        nullValue.put("kind", "null");
        Assert.assertThrows(IOException.class, () -> S1LiteralPayload.decode(nullValue,
                factory.createSqlType(SqlTypeName.INTEGER)));
    }
    @Test
    public void lossyStorageMustFailEvenWhenRexEqualityCanonicalizesNaNs() throws IOException {
        var factory = factory();
        var real = factory.createSqlType(SqlTypeName.REAL);
        var original = org.apache.calcite.rex.FelderaS1LiteralFactory.create(
                Double.longBitsToDouble(0x7ff8000000000042L), real, SqlTypeName.DOUBLE);
        var narrowed = Utilities.deterministicObjectMapper().createObjectNode();
        narrowed.put("kind", "float32").put("bits", 0x7fc00000);
        var restored = S1LiteralPayload.decode(narrowed, real);
        Assert.assertEquals(original, restored); // Double.equals canonicalizes NaNs.
        Assert.assertNotEquals(Double.doubleToRawLongBits(original.getValueAs(Double.class)),
                Double.doubleToRawLongBits(restored.getValueAs(Double.class)));
        Assert.assertThrows(IOException.class, () -> S1LiteralPayload.encode(original));

        var overprecise = org.apache.calcite.rex.FelderaS1LiteralFactory.create(
                Math.nextUp(1.0), real, SqlTypeName.DOUBLE);
        Assert.assertThrows(IOException.class, () -> S1LiteralPayload.encode(overprecise));
        var scaledInteger = org.apache.calcite.rex.FelderaS1LiteralFactory.create(
                new BigDecimal("1.0"), factory.createSqlType(SqlTypeName.INTEGER), SqlTypeName.DECIMAL);
        Assert.assertThrows(IOException.class, () -> S1LiteralPayload.encode(scaledInteger));
    }

    @Test
    public void representableRealAndDoubleStorageRetainsRawBits() throws IOException {
        var factory = factory();
        var real = factory.createSqlType(SqlTypeName.REAL);
        for (int bits: new int[]{0, 0x80000000, 1, 0x7fc12345}) {
            double value = Float.intBitsToFloat(bits);
            var original = org.apache.calcite.rex.FelderaS1LiteralFactory.create(value, real, SqlTypeName.DOUBLE);
            var payload = S1LiteralPayload.encode(original);
            Assert.assertEquals(Integer.toUnsignedLong(bits), payload.path("bits").longValue());
            var restored = S1LiteralPayload.decode(payload, real);
            Assert.assertEquals(Double.doubleToRawLongBits(value),
                    Double.doubleToRawLongBits(restored.getValueAs(Double.class)));
        }
        var type = factory.createSqlType(SqlTypeName.DOUBLE);
        double value = Double.longBitsToDouble(0x7ff8000000000042L);
        var original = org.apache.calcite.rex.FelderaS1LiteralFactory.create(value, type, SqlTypeName.DOUBLE);
        var restored = S1LiteralPayload.decode(S1LiteralPayload.encode(original), type);
        Assert.assertEquals(Double.doubleToRawLongBits(value),
                Double.doubleToRawLongBits(restored.getValueAs(Double.class)));
    }

    @Test
    public void literalCollectionIncludesValuesTuples() {
        var options = new org.dbsp.sqlCompiler.compiler.CompilerOptions();
        options.ioOptions.inputCircuit = true;
        var compiler = new org.dbsp.sqlCompiler.compiler.DBSPCompiler(options);
        compiler.submitStatementsForCompilation("CREATE VIEW v AS SELECT 1234567 AS x;");
        Assert.assertNotNull(compiler.getFinalCircuit(false));
        compiler.throwIfErrorsOccurred();
        var capture = S1TypeCorpusProbe.literals(compiler);
        Assert.assertTrue(capture.literals().stream().anyMatch(literal ->
                literal.getType().getSqlTypeName() == SqlTypeName.INTEGER
                        && new BigDecimal("1234567").equals(literal.getValueAs(BigDecimal.class))));
    }

    @Test
    public void literalCollectionIncludesWindowConstantsAndBounds() {
        var options = new org.dbsp.sqlCompiler.compiler.CompilerOptions();
        options.ioOptions.inputCircuit = true;
        var compiler = new org.dbsp.sqlCompiler.compiler.DBSPCompiler(options);
        compiler.submitStatementsForCompilation("""
                CREATE TABLE t(x INT, y INT);
                CREATE VIEW v AS SELECT SUM(x) OVER (
                    ORDER BY y ROWS BETWEEN 123 PRECEDING AND CURRENT ROW) AS z FROM t;
                """);
        Assert.assertNotNull(compiler.getFinalCircuit(false));
        compiler.throwIfErrorsOccurred();
        var capture = S1TypeCorpusProbe.literals(compiler);
        Assert.assertTrue(capture.literals().stream().anyMatch(literal ->
                !literal.isNull()
                        && List.of(SqlTypeName.INTEGER, SqlTypeName.BIGINT, SqlTypeName.DECIMAL)
                        .contains(literal.getType().getSqlTypeName())
                        && new BigDecimal("123").compareTo(literal.getValueAs(BigDecimal.class)) == 0));
    }

}
