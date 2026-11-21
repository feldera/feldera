package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;

public class JaninoFoldEligibilityTests {
    @Test
    public void temporalPassRequiresCanonicalStringsAtTheDeclaredPrecision() {
        var builder = new RexBuilder(new JavaTypeFactoryImpl());
        for (var name: List.of(SqlTypeName.TIME, SqlTypeName.TIMESTAMP)) {
            var type = builder.getTypeFactory().createSqlType(name, 0);
            String prefix = name == SqlTypeName.TIMESTAMP ? "2020-01-02 " : "";
            for (String clock: List.of("01:02:03", "1:2:3", "01:02:03.1", "01:02:03 ")) {
                var cast = (org.apache.calcite.rex.RexCall) builder.makeAbstractCast(type, builder.makeLiteral(prefix + clock), false);
                Assert.assertEquals(clock.equals("01:02:03"), RustFoldExecutor.canonicalTemporalCast(cast));
            }
        }
    }

    @Test
    public void temporalPassChecksPrecisionBeforeLiteralConversion() {
        var whole = new org.apache.calcite.util.TimestampString("2020-02-29 23:59:58");
        var fractional = new org.apache.calcite.util.TimestampString("2020-02-29 23:59:58.5");
        Assert.assertTrue(RustFoldExecutor.timestampFits(whole, 0));
        Assert.assertFalse(RustFoldExecutor.timestampFits(whole, -1));
        Assert.assertFalse(RustFoldExecutor.timestampFits(fractional, 0));
        Assert.assertTrue(RustFoldExecutor.timestampFits(fractional, 1));
    }

    public static org.locationtech.jts.geom.Geometry ST_Point(BigDecimal x, BigDecimal y) {
        throw new AssertionError("User function must never execute during fold eligibility");
    }

    @Test
    public void spatialIdentityUsesTheBackingMethodInsteadOfTheSqlName() throws Exception {
        var builder = new RexBuilder(new JavaTypeFactoryImpl());
        for (Class<?> owner: List.of(org.apache.calcite.runtime.SpatialTypeFunctions.class,
                JaninoFoldEligibilityTests.class)) {
            var function = org.apache.calcite.schema.impl.ScalarFunctionImpl.create(
                    owner.getMethod("ST_Point", BigDecimal.class, BigDecimal.class));
            var operator = new org.apache.calcite.sql.validate.SqlUserDefinedFunction(
                    new org.apache.calcite.sql.SqlIdentifier("ST_POINT", org.apache.calcite.sql.parser.SqlParserPos.ZERO),
                    org.apache.calcite.sql.SqlKind.ST_POINT,
                    org.apache.calcite.sql.type.ReturnTypes.explicit(SqlTypeName.GEOMETRY),
                    null, null, function);
            var zero = builder.makeExactLiteral(BigDecimal.ZERO);
            var call = builder.makeCall(operator, zero, zero);
            boolean builtin = owner == org.apache.calcite.runtime.SpatialTypeFunctions.class;
            Assert.assertEquals(builtin, JaninoFoldEligibility.builtin(operator) != null);
            Assert.assertEquals(builtin, JaninoFoldEligibility.accepts(List.of(call)));
        }
    }

    @Test
    public void emptySplitFailsAtLiteralConversionAndAbandonsTheBatch() {
        var factory = new JavaTypeFactoryImpl();
        var builder = new RexBuilder(factory);
        var split = builder.makeCall(SqlLibraryOperators.SPLIT,
                builder.makeLiteral(""), builder.makeLiteral(""));
        var one = builder.makeExactLiteral(BigDecimal.ONE);
        var sum = builder.makeCall(SqlStdOperatorTable.PLUS, one, one);
        var values = org.apache.calcite.runtime.SqlFunctions.split("", "");
        Assert.assertTrue(values.isEmpty());
        Assert.assertThrows(RuntimeException.class,
                () -> builder.makeLiteral(values, split.getType(), true));
        var executor = new org.apache.calcite.rex.RexExecutorImpl(org.apache.calcite.DataContexts.EMPTY);
        var output = new java.util.ArrayList<org.apache.calcite.rex.RexNode>();
        executor.reduce(builder, List.of(sum, split), output);
        Assert.assertEquals(List.of(sum, split), output);
    }

    @Test
    public void pinnedCalcitePreservesNonfiniteApproximateLiterals() {
        var builder = new RexBuilder(new JavaTypeFactoryImpl());
        for (var kind: List.of(SqlTypeName.REAL, SqlTypeName.DOUBLE)) {
            var type = builder.getTypeFactory().createSqlType(kind);
            for (double value: new double[]{Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
                var literal = (RexLiteral) builder.makeLiteral(value, type, true);
                Assert.assertEquals(Double.doubleToRawLongBits(value),
                        Double.doubleToRawLongBits(literal.getValueAs(Double.class)));
            }
        }
    }

    @Test
    public void unavailableCallPreventsTheWholeBatch() {
        var builder = new RexBuilder(new JavaTypeFactoryImpl());
        var one = builder.makeExactLiteral(BigDecimal.ONE);
        var sum = builder.makeCall(SqlStdOperatorTable.PLUS, one, one);
        Assert.assertTrue(JaninoFoldEligibility.accepts(List.of(sum, one)));
        for (var operator: List.of(SqlStdOperatorTable.NULLIF,
                SqlLibraryOperators.LEAST, SqlLibraryOperators.GREATEST)) {
            var unavailable = builder.makeCall(operator, one, one);
            Assert.assertFalse(JaninoFoldEligibility.accepts(List.of(unavailable)));
            Assert.assertFalse(JaninoFoldEligibility.accepts(List.of(sum, unavailable)));
            Assert.assertFalse(JaninoFoldEligibility.accepts(List.of(
                    builder.makeCall(SqlStdOperatorTable.PLUS, unavailable, one))));
        }
    }

    @Test
    public void specialCaseTranslationStillChecksUnreachableBranches() {
        var builder = new RexBuilder(new JavaTypeFactoryImpl());
        var one = builder.makeExactLiteral(BigDecimal.ONE);
        var valid = builder.makeCall(SqlStdOperatorTable.CASE, builder.makeLiteral(true), one, one);
        Assert.assertTrue(JaninoFoldEligibility.accepts(List.of(valid)));
        var unavailable = builder.makeCall(SqlStdOperatorTable.NULLIF, one, one);
        var invalid = builder.makeCall(SqlStdOperatorTable.CASE,
                builder.makeLiteral(true), one, unavailable);
        Assert.assertFalse(JaninoFoldEligibility.accepts(List.of(invalid)));
        Assert.assertFalse(JaninoFoldEligibility.accepts(List.of(
                builder.makeInputRef(builder.getTypeFactory().createSqlType(SqlTypeName.INTEGER), 0))));
    }
}
