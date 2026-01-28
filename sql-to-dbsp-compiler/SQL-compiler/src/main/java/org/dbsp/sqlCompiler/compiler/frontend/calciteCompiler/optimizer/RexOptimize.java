package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeShortInterval;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

/** An expression optimizer for RexCall expressions with reduced scope.
 * Calcite's RexSimplify and Enumerable implementations have lots of bugs, e.g., related to time computations.
 * <a href="https://issues.apache.org/jira/browse/CALCITE-6752">JavaTypeFactoryImpl cannot represent fractional
 * seconds</a>.  So we reimplement a subset of these using the "correct" semantics. */
public class RexOptimize extends RexShuttle {
    final RexBuilder builder;

    public RexOptimize(RexBuilder builder) {
        this.builder = builder;
    }

    @Override public RexNode visitCall(RexCall call) {
        for (;;) {
            call = (RexCall) super.visitCall(call);
            final RexCall old = call;
            RexNode result = this.optimize(call);
            if (result == old || !(result instanceof RexCall)) {
                return result;
            }
            call = (RexCall) result;
        }
    }

    RexNode optimize(RexCall call) {
        if (SqlTypeName.INTERVAL_TYPES.contains(call.getType().getSqlTypeName()))
            return this.optimizeInterval(call);
        return call;
    }

    /** Constant fold interval computations */
    RexNode optimizeInterval(RexCall call) {
        RelDataType resultType = call.getType();
        switch (call.getKind()) {
            case MINUS_PREFIX: {
                RexNode op = call.getOperands().get(0);
                if (op instanceof RexLiteral lit) {
                    BigDecimal value = lit.getValueAs(BigDecimal.class);
                    Objects.requireNonNull(value);
                    return this.builder.makeLiteral(value.negate(), resultType, true, false);
                }
            }
            case DIVIDE:
            case TIMES:
            case MINUS:
            case PLUS: {
                RexNode op0 = call.getOperands().get(0);
                RexNode op1 = call.getOperands().get(1);
                if (op0 instanceof RexLiteral left && op1 instanceof RexLiteral right) {
                    BigDecimal leftValue = Objects.requireNonNull(left.getValueAs(BigDecimal.class));
                    BigDecimal rightValue = Objects.requireNonNull(right.getValueAs(BigDecimal.class));
                    switch (call.getKind()) {
                        case PLUS:
                        case MINUS: {
                            if (call.getKind() == SqlKind.MINUS) {
                                rightValue = rightValue.negate();
                            }
                            BigDecimal result = leftValue.add(rightValue);
                            return this.builder.makeLiteral(result, resultType, true, false);
                        }
                        case TIMES: {
                            BigDecimal result = leftValue.multiply(rightValue);
                            return this.builder.makeLiteral(result, resultType, true, false);
                        }
                        case DIVIDE: {
                            // Microsecond precision; values are in milliseconds, and we keep 3 more decimals
                            BigDecimal result = leftValue.divide(rightValue, 3, RoundingMode.FLOOR);
                            return this.builder.makeLiteral(result, resultType, true, false);
                        }
                    }
                }
            }
            default:
                break;
        }
        return call;
    }
}
