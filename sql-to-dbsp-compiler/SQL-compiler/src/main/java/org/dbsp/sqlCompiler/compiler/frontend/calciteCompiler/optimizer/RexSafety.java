package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.List;
import java.util.Map;

/**
 * Conservative analysis of Calcite expressions that cannot raise a runtime error.
 *
 * <p>Stricter than Calcite's {@code RexSimplify.isSafeExpression}: every function
 * call (LIKE, TRIM, CEIL, TIMESTAMP_ADD, ...) is unsafe for now.
 */
public final class RexSafety {
    private RexSafety() {}

    private record IntegerType(int bits, boolean signed) {}

    private static final Map<SqlTypeName, IntegerType> INTEGER_TYPES = Map.of(
            SqlTypeName.TINYINT, new IntegerType(8, true),
            SqlTypeName.SMALLINT, new IntegerType(16, true),
            SqlTypeName.INTEGER, new IntegerType(32, true),
            SqlTypeName.BIGINT, new IntegerType(64, true),
            SqlTypeName.UTINYINT, new IntegerType(8, false),
            SqlTypeName.USMALLINT, new IntegerType(16, false),
            SqlTypeName.UINTEGER, new IntegerType(32, false),
            SqlTypeName.UBIGINT, new IntegerType(64, false));

    /** True if evaluating 'node' can never raise an error, for any input row. */
    public static boolean neverThrows(RexNode node) {
        if (node instanceof RexInputRef || node instanceof RexLiteral)
            return true;
        if (!(node instanceof RexCall call))
            return false;
        List<RexNode> operands = call.getOperands();
        switch (call.getKind()) {
            case AND, OR, NOT,
                 IS_NULL, IS_NOT_NULL, IS_TRUE, IS_NOT_TRUE, IS_FALSE, IS_NOT_FALSE:
                return allNeverThrow(operands);
            case EQUALS, NOT_EQUALS, LESS_THAN, LESS_THAN_OR_EQUAL,
                 GREATER_THAN, GREATER_THAN_OR_EQUAL,
                 IS_DISTINCT_FROM, IS_NOT_DISTINCT_FROM, SEARCH:
                // Operands of different types may need a conversion, which can fail
                return sameTypes(operands) && allNeverThrow(operands);
            case CAST:
                return castNeverThrows(operands.get(0).getType(), call.getType())
                        && neverThrows(operands.get(0));
            default:
                return false;
        }
    }

    static boolean allNeverThrow(List<RexNode> nodes) {
        for (RexNode node : nodes) {
            if (!neverThrows(node))
                return false;
        }
        return true;
    }

    /** True if all types are equal ignoring nullability, or all are character strings */
    static boolean sameTypes(List<RexNode> operands) {
        RelDataType first = operands.get(0).getType();
        for (RexNode operand : operands) {
            RelDataType type = operand.getType();
            if (SqlTypeUtil.equalSansNullability(first, type))
                continue;
            if (SqlTypeUtil.inCharFamily(first) && SqlTypeUtil.inCharFamily(type))
                continue;
            return false;
        }
        return true;
    }

    static boolean castNeverThrows(RelDataType from, RelDataType to) {
        // A NULL value cannot be converted to a NOT NULL type
        if (from.isNullable() && !to.isNullable())
            return false;
        if (SqlTypeUtil.equalSansNullability(from, to))
            return true;
        IntegerType source = INTEGER_TYPES.get(from.getSqlTypeName());
        IntegerType target = INTEGER_TYPES.get(to.getSqlTypeName());
        if (source == null || target == null)
            return false;
        if (source.signed() == target.signed())
            return target.bits() >= source.bits();
        // Negative values do not fit an unsigned type; an unsigned value fits a wider signed type
        return !source.signed() && target.bits() > source.bits();
    }
}
