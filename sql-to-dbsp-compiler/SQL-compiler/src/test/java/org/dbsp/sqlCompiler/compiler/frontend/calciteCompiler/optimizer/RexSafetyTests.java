package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.apache.calcite.sql.type.SqlTypeName.*;

/** Unit tests for {@link RexSafety}. */
public class RexSafetyTests {
    static final RelDataTypeFactory TYPES = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    static final RexBuilder REX = new RexBuilder(TYPES);

    static RelDataType type(SqlTypeName name, boolean nullable) {
        return TYPES.createTypeWithNullability(TYPES.createSqlType(name), nullable);
    }

    static RexNode column(SqlTypeName name, int index) {
        return REX.makeInputRef(type(name, true), index);
    }

    static RexNode literal(int value) {
        return REX.makeExactLiteral(BigDecimal.valueOf(value));
    }

    record Cast(SqlTypeName from, SqlTypeName to, boolean neverThrows) {}

    @Test
    public void integerCasts() {
        List<Cast> casts = List.of(
                new Cast(INTEGER, INTEGER, true),
                new Cast(TINYINT, BIGINT, true),
                new Cast(BIGINT, INTEGER, false),
                new Cast(UTINYINT, USMALLINT, true),
                new Cast(UBIGINT, UINTEGER, false),
                // Unsigned to signed: the signed type must be strictly wider
                new Cast(UTINYINT, SMALLINT, true),
                new Cast(UINTEGER, BIGINT, true),
                new Cast(UTINYINT, TINYINT, false),
                new Cast(UBIGINT, BIGINT, false),
                // Signed to unsigned: negative values never fit
                new Cast(TINYINT, UTINYINT, false),
                new Cast(SMALLINT, UBIGINT, false));
        for (Cast cast : casts) {
            Assert.assertEquals(cast.toString(), cast.neverThrows(),
                    RexSafety.castNeverThrows(type(cast.from(), true), type(cast.to(), true)));
        }
    }

    @Test
    public void castNullability() {
        Assert.assertTrue(RexSafety.castNeverThrows(type(INTEGER, false), type(BIGINT, true)));
        // A NULL value does not fit a NOT NULL type
        Assert.assertFalse(RexSafety.castNeverThrows(type(INTEGER, true), type(BIGINT, false)));
        Assert.assertFalse(RexSafety.castNeverThrows(type(INTEGER, true), type(INTEGER, false)));
    }

    @Test
    public void otherCasts() {
        Assert.assertTrue(RexSafety.castNeverThrows(type(DOUBLE, true), type(DOUBLE, true)));
        Assert.assertFalse(RexSafety.castNeverThrows(type(VARCHAR, true), type(INTEGER, true)));
        Assert.assertFalse(RexSafety.castNeverThrows(type(DOUBLE, true), type(INTEGER, true)));
        // Conservative: these cannot fail, but the analysis does not know it
        Assert.assertFalse(RexSafety.castNeverThrows(type(INTEGER, true), type(VARCHAR, true)));
        Assert.assertFalse(RexSafety.castNeverThrows(type(INTEGER, true), type(DOUBLE, true)));
    }

    @Test
    public void safeExpressions() {
        RexNode x = column(INTEGER, 0);
        RexNode s = column(VARCHAR, 1);
        RexNode in = REX.makeIn(x, List.of(literal(4), literal(9)));
        Assert.assertEquals(SqlKind.SEARCH, in.getKind());
        List<RexNode> expressions = List.of(
                x,
                literal(1),
                in,
                REX.makeCall(SqlStdOperatorTable.EQUALS, x, literal(1)),
                REX.makeCall(SqlStdOperatorTable.IS_NULL, x),
                // VARCHAR compared with CHAR(3)
                REX.makeCall(SqlStdOperatorTable.EQUALS, s, REX.makeLiteral("abc")),
                REX.makeCall(SqlStdOperatorTable.AND, in, REX.makeCall(SqlStdOperatorTable.IS_NOT_NULL, s)),
                REX.makeAbstractCast(type(BIGINT, true), x, false));
        for (RexNode expression : expressions)
            Assert.assertTrue(expression.toString(), RexSafety.neverThrows(expression));
    }

    @Test
    public void unsafeExpressions() {
        RexNode x = column(INTEGER, 0);
        RexNode big = column(BIGINT, 1);
        RexNode s = column(VARCHAR, 2);
        RexNode division = REX.makeCall(SqlStdOperatorTable.DIVIDE, literal(10), x);
        List<RexNode> expressions = List.of(
                REX.makeCall(SqlStdOperatorTable.GREATER_THAN, division, literal(1)),
                REX.makeCall(SqlStdOperatorTable.CHECKED_PLUS, x, literal(1)),
                // Different types need a conversion
                REX.makeCall(SqlStdOperatorTable.EQUALS, x, big),
                REX.makeCall(SqlStdOperatorTable.UPPER, s),
                REX.makeCall(SqlStdOperatorTable.IS_NULL, REX.makeCall(SqlStdOperatorTable.UPPER, s)),
                REX.makeCall(SqlStdOperatorTable.AND,
                        REX.makeCall(SqlStdOperatorTable.IS_NULL, x),
                        REX.makeCall(SqlStdOperatorTable.GREATER_THAN, division, literal(1))),
                REX.makeAbstractCast(type(INTEGER, true), s, false),
                REX.makeAbstractCast(type(INTEGER, true), big, false));
        for (RexNode expression : expressions)
            Assert.assertFalse(expression.toString(), RexSafety.neverThrows(expression));
    }
}
