package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPRealLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeISize;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeReal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUSize;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

/** Unit tests for {@link Simplify}. */
public class SimplifyTests extends BaseSQLTests {
    static final List<DBSPTypeCode> INTEGER_CODES = List.of(
            DBSPTypeCode.INT8, DBSPTypeCode.INT16, DBSPTypeCode.INT32, DBSPTypeCode.INT64, DBSPTypeCode.INT128,
            DBSPTypeCode.UINT8, DBSPTypeCode.UINT16, DBSPTypeCode.UINT32, DBSPTypeCode.UINT64, DBSPTypeCode.UINT128);

    static DBSPTypeInteger integerType(DBSPTypeCode code) {
        return DBSPTypeInteger.getType(CalciteObject.EMPTY, code, false);
    }

    static DBSPLiteral integer(DBSPTypeInteger type, BigInteger value) {
        return Objects.requireNonNull(type.getLiteral(CalciteObject.EMPTY, value));
    }

    static DBSPLiteral integer(DBSPTypeInteger type, long value) {
        return integer(type, BigInteger.valueOf(value));
    }

    /** The values of {@code type} at the ends of its range and around zero. */
    static TreeSet<BigInteger> extremeValues(DBSPTypeInteger type) {
        TreeSet<BigInteger> result = new TreeSet<>();
        BigInteger min = type.minimum();
        BigInteger max = type.maximum();
        for (BigInteger value : List.of(min, min.add(BigInteger.ONE), BigInteger.ONE.negate(),
                BigInteger.ZERO, BigInteger.ONE, max.subtract(BigInteger.ONE), max)) {
            if (value.compareTo(min) >= 0 && value.compareTo(max) <= 0)
                result.add(value);
        }
        return result;
    }

    /** {@code source} cast to {@code type}, after {@link Simplify}. */
    DBSPExpression simplifyCast(DBSPCompiler compiler, DBSPExpression source, DBSPType type) {
        DBSPExpression cast = new DBSPCastExpression(
                CalciteObject.EMPTY, source, type, DBSPCastExpression.CastType.SqlUnsafe);
        return new Simplify(compiler).apply(cast).to(DBSPExpression.class);
    }

    @Test
    public void testIntegerRange() {
        for (DBSPTypeCode code : INTEGER_CODES) {
            DBSPTypeInteger type = integerType(code);
            int valueBits = type.signed ? type.getWidth() - 1 : type.getWidth();
            BigInteger range = BigInteger.ONE.shiftLeft(valueBits);
            BigInteger min = type.signed ? range.negate() : BigInteger.ZERO;
            Assert.assertEquals(code.toString(), min, type.minimum());
            Assert.assertEquals(code.toString(), range.subtract(BigInteger.ONE), type.maximum());
            Assert.assertEquals(code.toString(), type.minimum(),
                    type.getMinValue().to(DBSPIntLiteral.class).getValue());
            Assert.assertEquals(code.toString(), type.maximum(),
                    type.getMaxValue().to(DBSPIntLiteral.class).getValue());
            Assert.assertNull(type.getLiteral(CalciteObject.EMPTY, type.minimum().subtract(BigInteger.ONE)));
            Assert.assertNull(type.getLiteral(CalciteObject.EMPTY, type.maximum().add(BigInteger.ONE)));
        }
    }

    @Test
    public void testIntegerToInteger() {
        DBSPCompiler compiler = this.testCompiler();
        for (DBSPTypeCode sourceCode : INTEGER_CODES) {
            DBSPTypeInteger sourceType = integerType(sourceCode);
            for (DBSPTypeCode targetCode : INTEGER_CODES) {
                DBSPTypeInteger targetType = integerType(targetCode);
                for (BigInteger value : extremeValues(sourceType)) {
                    String message = "CAST(" + value + ": " + sourceCode + " AS " + targetCode + ")";
                    DBSPExpression result = simplifyCast(compiler, integer(sourceType, value), targetType);
                    boolean fits = value.compareTo(targetType.minimum()) >= 0
                            && value.compareTo(targetType.maximum()) <= 0;
                    if (fits) {
                        Assert.assertTrue(message, result.getType().sameType(targetType));
                        Assert.assertEquals(message, value, result.to(DBSPIntLiteral.class).getValue());
                    } else {
                        // The runtime reports the overflow
                        Assert.assertTrue(message, result.is(DBSPCastExpression.class));
                    }
                }
            }
        }
    }

    @Test
    public void testIntegerToNullableInteger() {
        DBSPCompiler compiler = this.testCompiler();
        DBSPTypeInteger nullableInt = DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, true);
        DBSPExpression result = simplifyCast(compiler, integer(integerType(DBSPTypeCode.INT8), 7), nullableInt);
        Assert.assertTrue(result.getType().sameType(nullableInt));
        Assert.assertEquals(BigInteger.valueOf(7), result.to(DBSPIntLiteral.class).getValue());
    }

    @Test
    public void testIntegerToDecimal() {
        DBSPCompiler compiler = this.testCompiler();
        DBSPTypeInteger bigint = integerType(DBSPTypeCode.INT64);
        DBSPTypeDecimal decimal32 = new DBSPTypeDecimal(CalciteObject.EMPTY, 3, 2, false);
        DBSPExpression result = simplifyCast(compiler, integer(bigint, -5), decimal32);
        Assert.assertEquals(new BigDecimal("-5.00"), result.to(DBSPDecimalLiteral.class).value);

        // 10.00 needs 4 digits
        result = simplifyCast(compiler, integer(bigint, 10), decimal32);
        Assert.assertTrue(result.is(DBSPCastExpression.class));

        DBSPTypeDecimal decimal20 = new DBSPTypeDecimal(CalciteObject.EMPTY, 2, 0, false);
        result = simplifyCast(compiler, integer(integerType(DBSPTypeCode.INT32), 99), decimal20);
        Assert.assertEquals(new BigDecimal("99"), result.to(DBSPDecimalLiteral.class).value);
        result = simplifyCast(compiler, integer(integerType(DBSPTypeCode.INT32), 1000), decimal20);
        Assert.assertTrue(result.is(DBSPCastExpression.class));
    }

    @Test
    public void testIntegerToFloatingPoint() {
        DBSPCompiler compiler = this.testCompiler();
        DBSPTypeInteger bigint = integerType(DBSPTypeCode.INT64);
        BigInteger doubleMantissa = BigInteger.ONE.shiftLeft(53);
        DBSPExpression result = simplifyCast(compiler, integer(bigint, doubleMantissa), DBSPTypeDouble.INSTANCE);
        Assert.assertEquals(doubleMantissa.doubleValue(), result.to(DBSPDoubleLiteral.class).value, 0);
        // Rounds to 2^53
        result = simplifyCast(compiler, integer(bigint, doubleMantissa.add(BigInteger.ONE)), DBSPTypeDouble.INSTANCE);
        Assert.assertTrue(result.is(DBSPCastExpression.class));

        BigInteger realMantissa = BigInteger.ONE.shiftLeft(24);
        result = simplifyCast(compiler, integer(bigint, realMantissa.negate()), DBSPTypeReal.INSTANCE);
        Assert.assertEquals(-realMantissa.floatValue(), result.to(DBSPRealLiteral.class).value, 0);
        // Rounds to 2^24
        result = simplifyCast(compiler, integer(bigint, realMantissa.add(BigInteger.ONE)), DBSPTypeReal.INSTANCE);
        Assert.assertTrue(result.is(DBSPCastExpression.class));
    }

    @Test
    public void testIntegerToSize() {
        DBSPCompiler compiler = this.testCompiler();
        DBSPTypeInteger u128 = integerType(DBSPTypeCode.UINT128);
        BigInteger u64Max = integerType(DBSPTypeCode.UINT64).maximum();
        DBSPExpression result = simplifyCast(compiler, integer(u128, u64Max), DBSPTypeUSize.INSTANCE);
        Assert.assertEquals(u64Max, result.to(DBSPUSizeLiteral.class).value);
        result = simplifyCast(compiler, integer(u128, u64Max.add(BigInteger.ONE)), DBSPTypeUSize.INSTANCE);
        Assert.assertTrue(result.is(DBSPCastExpression.class));
        result = simplifyCast(compiler, integer(integerType(DBSPTypeCode.INT8), -1), DBSPTypeUSize.INSTANCE);
        Assert.assertTrue(result.is(DBSPCastExpression.class));

        BigInteger i64Max = integerType(DBSPTypeCode.INT64).maximum();
        result = simplifyCast(compiler, integer(u128, i64Max), DBSPTypeISize.INSTANCE);
        Assert.assertFalse(result.is(DBSPCastExpression.class));
        result = simplifyCast(compiler, integer(u128, i64Max.add(BigInteger.ONE)), DBSPTypeISize.INSTANCE);
        Assert.assertTrue(result.is(DBSPCastExpression.class));
    }
}
