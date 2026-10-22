package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlTimeLiteral;
import org.apache.calcite.sql.SqlTimestampLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
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
        SqlTypeName type = call.getType().getSqlTypeName();
        if (SqlTypeName.INTERVAL_TYPES.contains(type))
            return this.optimizeInterval(call);
        if (call.getKind() == SqlKind.CAST || call.getKind() == SqlKind.SAFE_CAST)
            return this.optimizeCast(call);
        if (type == SqlTypeName.DATE || type == SqlTypeName.TIMESTAMP)
            return this.optimizeDatetimeArithmetic(call);
        return call;
    }

    static final long MICROSECONDS_PER_DAY = 86_400_000_000L;

    /** Constant fold the sum or the difference of a DATE or TIMESTAMP literal 
     * and an interval literal. */
    RexNode optimizeDatetimeArithmetic(RexCall call) {
        boolean minus = call.getKind() == SqlKind.MINUS;
        if ((!minus && call.getKind() != SqlKind.PLUS) || call.getOperands().size() != 2)
            return call;
        RexNode datetime = call.getOperands().get(0);
        RexNode interval = call.getOperands().get(1);
        if (!minus && SqlTypeName.INTERVAL_TYPES.contains(datetime.getType().getSqlTypeName())) {
            // INTERVAL + DATE
            RexNode first = datetime;
            datetime = interval;
            interval = first;
        }
        if (!(datetime instanceof RexLiteral datetimeLiteral)
                || !(interval instanceof RexLiteral intervalLiteral)
                || !SqlTypeName.INTERVAL_TYPES.contains(interval.getType().getSqlTypeName()))
            return call;
        BigDecimal amount = intervalLiteral.getValueAs(BigDecimal.class);
        if (amount == null)
            return call;
        if (minus)
            amount = amount.negate();
        // Calcite stores a year-month interval in months, and a day-time interval in milliseconds
        boolean months = SqlTypeName.YEAR_INTERVAL_TYPES.contains(interval.getType().getSqlTypeName());
        try {
            switch (call.getType().getSqlTypeName()) {
                case DATE: {
                    DateString date = Objects.requireNonNull(datetimeLiteral.getValueAs(DateString.class));
                    LocalDate value = LocalDate.ofEpochDay(date.getDaysSinceEpoch());
                    LocalDate result;
                    if (months) {
                        result = value.plusMonths(amount.longValueExact());
                    } else {
                        long microseconds = amount.movePointRight(3).longValueExact();
                        result = value.plusDays(microseconds / MICROSECONDS_PER_DAY);
                    }
                    if (result.getYear() < 1 || result.getYear() > 9999)
                        return call;
                    DateString resultString = DateString.fromDaysSinceEpoch(Math.toIntExact(result.toEpochDay()));
                    return this.builder.makeLiteral(resultString, call.getType(), true);
                }
                case TIMESTAMP: {
                    TimestampString timestamp = Objects.requireNonNull(
                            datetimeLiteral.getValueAs(TimestampString.class));
                    LocalDateTime value = LocalDateTime.parse(timestamp.toString().replace(' ', 'T'));
                    LocalDateTime result;
                    if (months) {
                        result = value.plusMonths(amount.longValueExact());
                    } else {
                        long microseconds = amount.movePointRight(3).longValueExact();
                        result = value.plus(microseconds, ChronoUnit.MICROS);
                    }
                    if (result.getYear() < 1 || result.getYear() > 9999)
                        return call;
                    // A literal cannot have more fractional digits than its type
                    int precision = call.getType().getPrecision();
                    if (precision < 0)
                        return call;
                    long unit = 1;
                    for (int i = precision; i < 9; i++)
                        unit *= 10;
                    if (result.getNano() % unit != 0)
                        return call;
                    TimestampString resultString = new TimestampString(
                            result.getYear(), result.getMonthValue(), result.getDayOfMonth(),
                            result.getHour(), result.getMinute(), result.getSecond())
                            .withNanos(result.getNano());
                    return this.builder.makeLiteral(resultString, call.getType(), true);
                }
                default:
                    return call;
            }
        } catch (ArithmeticException | DateTimeException e) {
            // The runtime reports the error
            return call;
        }
    }

    /** Constant fold a cast of a string literal to TIME or TIMESTAMP.  Calcite folds only
     * strings with fractional seconds (CALCITE-7529).  The string must be in the canonical
     * form, which the runtime parses the same way, with no more fractional digits than the
     * precision of the result, so that no rounding is needed.  Any other string is left to
     * the runtime. */
    RexNode optimizeCast(RexCall call) {
        if (!(call.getOperands().get(0) instanceof RexLiteral literal)
                || !SqlTypeUtil.isCharacter(literal.getType()))
            return call;
        NlsString string = literal.getValueAs(NlsString.class);
        if (string == null)
            return call;
        String value = string.getValue();
        RelDataType type = call.getType();
        try {
            switch (type.getSqlTypeName()) {
                case TIME: {
                    SqlTimeLiteral time = SqlParserUtil.parseTimeLiteral(value, SqlParserPos.ZERO);
                    if (!time.toFormattedString().equals(value) || time.getPrec() > type.getPrecision())
                        return call;
                    return this.builder.makeLiteral(time.getValueAs(TimeString.class), type, true);
                }
                case TIMESTAMP: {
                    SqlTimestampLiteral timestamp = SqlParserUtil.parseTimestampLiteral(value, SqlParserPos.ZERO);
                    if (!timestamp.toFormattedString().equals(value)
                            || timestamp.getPrec() > type.getPrecision()
                            // The canonical form starts with the four digits of the year
                            || Integer.parseInt(value.substring(0, 4)) < 1)
                        return call;
                    return this.builder.makeLiteral(timestamp.getValueAs(TimestampString.class), type, true);
                }
                default:
                    return call;
            }
        } catch (RuntimeException e) {
            // Not a literal of this type; the runtime reports the error
            return call;
        }
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
            case CHECKED_PLUS:
            case CHECKED_MINUS:
            case CHECKED_TIMES:
            case CHECKED_DIVIDE:
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
                        case CHECKED_MINUS:
                        case CHECKED_PLUS:
                        case PLUS:
                        case MINUS: {
                            if (call.getKind() == SqlKind.MINUS || call.getKind() == SqlKind.CHECKED_MINUS) {
                                rightValue = rightValue.negate();
                            }
                            BigDecimal result = leftValue.add(rightValue);
                            return this.builder.makeLiteral(result, resultType, true, false);
                        }
                        case CHECKED_TIMES:
                        case TIMES: {
                            BigDecimal result = leftValue.multiply(rightValue);
                            return this.builder.makeLiteral(result, resultType, true, false);
                        }
                        case CHECKED_DIVIDE:
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
