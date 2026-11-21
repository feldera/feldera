package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.avatica.util.TimeUnit;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class S1ScalarTypeTests {
    @Test
    public void fixtureTypesRoundTripWithoutTheOriginalType() throws IOException {
        var factory = S1LiteralPayloadTests.factory();
        var builder = new RexBuilder(factory);
        var mapper = Utilities.deterministicObjectMapper();
        for (var literal: S1LiteralPayloadTests.fixtures(builder)) {
            var encoded = mapper.readTree(S1ScalarType.encode(literal.getType()).toString());
            var decoded = S1ScalarType.decode(encoded, factory);
            Assert.assertEquals(literal.getType().getFullTypeString() + " versus " + decoded.getFullTypeString(), literal.getType(), decoded);
            Assert.assertEquals(literal.getType().getFullTypeString(), decoded.getFullTypeString());
        }
        var qualifier = new SqlIntervalQualifier(TimeUnit.DAY, 4, TimeUnit.SECOND, 6, SqlParserPos.ZERO);
        var type = factory.createSqlIntervalType(qualifier);
        Assert.assertEquals(type, S1ScalarType.decode(mapper.readTree(S1ScalarType.encode(type).toString()), factory));
    }

    @Test
    public void unknownFieldsLossyPrecisionAndUnsupportedShapesFail() throws IOException {
        var factory = S1LiteralPayloadTests.factory();
        for (String pointer: List.of("", "/types/0", "/types/0/kind", "/types/0/origin")) {
            var table = S1ScalarType.encode(factory.createSqlType(SqlTypeName.INTEGER));
            ((com.fasterxml.jackson.databind.node.ObjectNode) table.at(pointer)).put("extra", 1);
            Assert.assertThrows(IOException.class, () -> S1ScalarType.decode(table, factory));
        }
        var decimal = S1ScalarType.encode(factory.createSqlType(SqlTypeName.DECIMAL, 38, 2));
        ((com.fasterxml.jackson.databind.node.ObjectNode) decimal.at("/types/0/kind")).put("precision", 1000);
        Assert.assertThrows(IOException.class, () -> S1ScalarType.decode(decimal, factory));
        var invalidScale = S1ScalarType.encode(factory.createSqlType(SqlTypeName.DECIMAL, 38, 2));
        ((com.fasterxml.jackson.databind.node.ObjectNode) invalidScale.at("/types/0/kind")).put("precision", 1);
        Assert.assertThrows(IOException.class, () -> S1ScalarType.decode(invalidScale, factory));
        var interval = S1ScalarType.encode(factory.createSqlIntervalType(
                new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO)));
        ((com.fasterxml.jackson.databind.node.ObjectNode) interval.at("/types/0/kind/leading_precision"))
                .put("kind", "specified").put("value", 0);
        Assert.assertThrows(IOException.class, () -> S1ScalarType.decode(interval, factory));
        var roots = S1ScalarType.encode(factory.createSqlType(SqlTypeName.INTEGER));
        roots.withArray("roots").add(0);
        Assert.assertThrows(IOException.class, () -> S1ScalarType.decode(roots, factory));
        var badFlag = S1ScalarType.encode(factory.createSqlType(SqlTypeName.INTEGER));
        ((com.fasterxml.jackson.databind.node.ObjectNode) badFlag.at("/types/0")).put("nullable", "false");
        Assert.assertThrows(IOException.class, () -> S1ScalarType.decode(badFlag, factory));
        Assert.assertThrows(IOException.class, () -> S1ScalarType.decode(Utilities.deterministicObjectMapper().createObjectNode(), factory));
    }

    @Test
    public void builtInAndEqualNamedCollationsRetainDistinctTypeDigests() throws IOException {
        var factory = S1LiteralPayloadTests.factory();
        var mapper = Utilities.deterministicObjectMapper();
        var base = factory.createSqlType(SqlTypeName.CHAR, 3);
        for (var builtin: List.of(org.apache.calcite.sql.SqlCollation.IMPLICIT,
                org.apache.calcite.sql.SqlCollation.COERCIBLE)) {
            var named = new org.apache.calcite.sql.SqlCollation(builtin.getCollationName(), builtin.getCoercibility());
            var builtInType = factory.createTypeWithCharsetAndCollation(base, base.getCharset(), builtin);
            var namedType = factory.createTypeWithCharsetAndCollation(base, base.getCharset(), named);
            Assert.assertNotEquals(builtInType.getFullTypeString(), namedType.getFullTypeString());
            for (var type: List.of(builtInType, namedType)) {
                var decoded = S1ScalarType.decode(mapper.readTree(S1ScalarType.encode(type).toString()), factory);
                Assert.assertEquals(type, decoded);
                Assert.assertEquals(type.getFullTypeString(), decoded.getFullTypeString());
            }
        }
    }

    @Test
    public void exporterRefusesUnrepresentedPrecisionBeforeWritingAType() throws IOException {
        var factory = S1LiteralPayloadTests.factory();
        var explicitFloat = factory.createSqlType(SqlTypeName.FLOAT, 24);
        Assert.assertThrows(IOException.class, () -> S1ScalarType.exportType(explicitFloat, factory));
        var defaultTime = factory.createSqlType(SqlTypeName.TIME);
        var exportedTime = S1ScalarType.exportType(defaultTime, factory);
        Assert.assertEquals(defaultTime, S1ScalarType.decode(
                Utilities.deterministicObjectMapper().readTree(exportedTime.toString()), factory));
        for (var literal: S1LiteralPayloadTests.fixtures(new RexBuilder(factory)))
            S1ScalarType.exportType(literal.getType(), factory);
    }

    @Test
    public void rawBinaryPrecisionAndExplicitDefaultRemainDistinct() throws IOException {
        var factory = new S1TypeTable.Factory();
        for (var name: List.of(SqlTypeName.BINARY, SqlTypeName.CHAR)) {
            var unspecified = factory.createSqlType(name, -1);
            var specified = factory.createSqlType(name, 1);
            Assert.assertNotEquals(unspecified.getFullTypeString(), specified.getFullTypeString());
            for (var type: List.of(unspecified, specified, factory.createSqlType(name))) {
                for (boolean nullable: List.of(false, true)) {
                    var original = factory.createTypeWithNullability(type, nullable);
                    var encoded = S1ScalarType.exportType(original, factory);
                    var decoded = S1ScalarType.decode(Utilities.deterministicObjectMapper().readTree(encoded.toString()), factory);
                    Assert.assertEquals(original, decoded);
                    Assert.assertEquals(original.getFullTypeString(), decoded.getFullTypeString());
                }
            }
        }
    }

    @Test
    public void rawDecimalScaleAndExtendedIntervalUnitsSurvive() throws IOException {
        var factory = new S1TypeTable.Factory();
        var raw = new org.apache.calcite.sql.type.BasicSqlType(factory.getTypeSystem(), SqlTypeName.DECIMAL, 38);
        var explicit = factory.createSqlType(SqlTypeName.DECIMAL, 38, 0);
        Assert.assertNotEquals(raw.getFullTypeString(), explicit.getFullTypeString());
        for (var type: List.of(raw, explicit,
                factory.createSqlIntervalType(new SqlIntervalQualifier(TimeUnit.QUARTER, null, SqlParserPos.ZERO)),
                factory.createSqlIntervalType(new SqlIntervalQualifier(TimeUnit.WEEK, null, SqlParserPos.ZERO)))) {
            for (boolean nullable: List.of(false, true)) {
                var original = factory.createTypeWithNullability(type, nullable);
                var encoded = S1ScalarType.exportType(original, factory);
                var decoded = S1ScalarType.decode(Utilities.deterministicObjectMapper().readTree(encoded.toString()), factory);
                Assert.assertEquals(original, decoded);
                Assert.assertEquals(original.getFullTypeString(), decoded.getFullTypeString());
            }
        }
    }

    @Test
    public void inferredStringPrecisionUsesUtf16Units() {
        var builder = new RexBuilder(S1LiteralPayloadTests.factory());
        Assert.assertEquals(2, builder.makeLiteral("🦀").getType().getPrecision());
    }
}
