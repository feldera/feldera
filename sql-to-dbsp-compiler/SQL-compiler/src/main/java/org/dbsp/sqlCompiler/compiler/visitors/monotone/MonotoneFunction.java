package org.dbsp.sqlCompiler.compiler.visitors.monotone;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** A runtime function whose result is monotone in its arguments on the domain where the function is
 * defined.
 *
 * <p>The waterline of a call is the function applied to the waterlines of its arguments.  It is a
 * lower bound of the result: if every later value x of the monotone argument is at least its
 * waterline w, then f(x) is at least f(w).  The function must satisfy this only on the arguments
 * where it does not fail; {@link #canFail} is true for a function that fails for some arguments.
 * These arguments must be closed upward: if the function does not fail for w, it does not fail for
 * any value above w, or it fails for the rows as well, e.g. ROUND of a DECIMAL near its maximum.
 *
 * <p>For example, LN fails for a negative argument.  It does not fail for an argument that is at
 * least 0; these arguments are closed upward, and LN is monotone on them.  When the waterline w is
 * at least 0, every later x is at least w, so LN(x) is at least LN(w). */
public abstract class MonotoneFunction {
    /** In a pattern of names, the position of the suffix N of a nullable monotone argument */
    static final String NULLABLE = "#";
    static final boolean FAILS = true;
    static final boolean NEVER_FAILS = false;

    /** The runtime names of the function: a regular expression in which {@link #NULLABLE} stands
     * for the suffix N of the monotone argument when it is nullable, e.g. "ln_d#" for ln_d and
     * ln_dN. */
    final Pattern names;
    /** True if the names mark the position of a nullable monotone argument */
    final boolean hasNullableArgument;
    /** True if the function fails for some arguments, e.g. LN of a negative number.  Such a
     * function has a saturating form, named like its form for arguments that are not nullable,
     * followed by _saturating. */
    final boolean canFail;

    MonotoneFunction(String names, boolean canFail) {
        this.names = Pattern.compile(names.replace(NULLABLE, "(?<N>N?)"));
        this.hasNullableArgument = names.contains(NULLABLE);
        this.canFail = canFail;
    }

    /** A description of an argument of a function.
     * @param expression  An expression computing the argument.
     * @param constant    True if the argument is a constant.
     * @param waterline   An expression computing the waterline of the argument; null if the argument
     *                    is not monotone. */
    record Argument(DBSPExpression expression, boolean constant, @Nullable DBSPExpression waterline) {
        boolean monotone() {
            return this.waterline != null;
        }

        /** The expression computing the waterline of a monotone argument. */
        public DBSPExpression waterline() {
            return Objects.requireNonNull(this.waterline);
        }

        /** A copy of a constant argument. */
        DBSPExpression copy() {
            Utilities.enforce(this.constant, () -> "Argument " + this.expression + " is not constant");
            return this.expression.deepCopy();
        }
    }

    /** @return An expression computing the waterline of {@code call} from the waterlines of
     * its arguments, or null if the call is not monotone.
     * @param call  A call of this function, with what the analysis knows about its arguments. */
    @Nullable
    abstract DBSPExpression waterline(MonotoneFunctionCall call);

    /** The name of the function that computes the waterline of a call named {@code name}: the
     * form of the function for arguments that are not nullable, or its saturating form if the
     * function can fail, e.g. ln_d_saturating for ln_dN.
     * @param name  A name matched by {@link #names}. */
    String waterlineFunctionName(String name) {
        Matcher matcher = this.names.matcher(name);
        Utilities.enforce(matcher.matches(), () -> name + " does not match " + this.names);
        String result = name;
        if (this.hasNullableArgument)
            result = name.substring(0, matcher.start("N")) + name.substring(matcher.end("N"));
        return this.canFail ? result + "_saturating" : result;
    }

    /** The monotone functions.  A runtime function name matches the {@link #names} of at most one
     * of these functions. */
    static final List<MonotoneFunction> FUNCTIONS = List.of(
            // SQRT of a negative number is NaN, the largest DOUBLE
            new UnaryMonotoneFunction("(ln|log10|log|sqrt)_d#", FAILS),
            new ConstantBaseMonotoneFunction("log_d#_d", FAILS, 0, 1),
            new ConstantBaseMonotoneFunction("power_d_(d|i32)#", NEVER_FAILS, 1, 0),
            new UnaryMonotoneFunction("(cbrt|exp|atan|sinh|asinh|tanh|degrees|radians)_d#", NEVER_FAILS),
            new UnaryMonotoneFunction("sign_(d|f|SqlDecimal|i8|i16|i32|i64|i128|u8|u16|u32|u64|u128)#", NEVER_FAILS),
            new UnaryMonotoneFunction("(floor|ceil)_(d|f|SqlDecimal)#", NEVER_FAILS),
            // FLOOR of a DATE or a TIMESTAMP to these units can precede year 1
            new UnaryMonotoneFunction(
                    "floor_(millennium|century|decade|week)_(Date|Timestamp|TimestampTz)#", FAILS),
            new UnaryMonotoneFunction("floor_(year|quarter|month|day|hour|minute|second|millisecond|" +
                    "microsecond|nanosecond)_(Date|Timestamp|TimestampTz)#", NEVER_FAILS),
            new UnaryMonotoneFunction("floor_(millennium|century|decade|year|quarter|month|week|day|hour|" +
                    "minute|second|millisecond|microsecond|nanosecond)_Time#", NEVER_FAILS),
            // CEIL of a DATE or a TIMESTAMP to these units can follow year 9999.  CEIL of a TIME wraps
            // around midnight, e.g. CEIL(23:30 TO HOUR) is 00:00, so it is not monotone.
            new UnaryMonotoneFunction(
                    "ceil_(millennium|century|decade|year|quarter|month|week)_(Date|Timestamp|TimestampTz)#",
                    FAILS),
            new UnaryMonotoneFunction("ceil_(day|hour|minute|second|millisecond)_(Timestamp|TimestampTz)#", FAILS),
            new UnaryMonotoneFunction("ceil_(microsecond|nanosecond)_(Timestamp|TimestampTz)#", NEVER_FAILS),
            new UnaryMonotoneFunction(
                    "ceil_(day|hour|minute|second|millisecond|microsecond|nanosecond)_Date#", NEVER_FAILS),
            new UnaryMonotoneFunction("date_trunc_(decade|week)_(Date|Timestamp|TimestampTz)#", FAILS),
            new UnaryMonotoneFunction(
                    "date_trunc_(millennium|century|year|quarter|month|day)_(Date|Timestamp|TimestampTz)#",
                    NEVER_FAILS),
            new UnaryMonotoneFunction("timestamp_trunc_(decade|week)_(Timestamp|TimestampTz)#", FAILS),
            new UnaryMonotoneFunction("timestamp_trunc_(millennium|century|year|quarter|month|day|hour|" +
                    "minute|second)_(Timestamp|TimestampTz)#", NEVER_FAILS),
            new UnaryMonotoneFunction(
                    "time_trunc_(hour|minute|second|millisecond|microsecond|nanosecond)_Time#", NEVER_FAILS),
            new UnaryMonotoneFunction(
                    "extract_(year|millennium|century|decade)_(Date|Timestamp|TimestampTz|LongInterval)#",
                    NEVER_FAILS),
            new UnaryMonotoneFunction("extract_isoyear_(Date|Timestamp|TimestampTz)#", NEVER_FAILS),
            new UnaryMonotoneFunction("extract_epoch_(Date|Timestamp|TimestampTz|ShortInterval)#", NEVER_FAILS),
            new UnaryMonotoneFunction("extract_epoch_LongInterval#", FAILS),
            // The hour of a TIMESTAMP wraps every day, so it is not monotone
            new UnaryMonotoneFunction("extract_hour_Time#", NEVER_FAILS),
            new UnaryMonotoneFunction(
                    "to_bound_ShortInterval_(ShortInterval|Date|Timestamp|Time)_(u64|u128)", NEVER_FAILS),
            new UnaryMonotoneFunction("(round|trunc)_SqlDecimal#", NEVER_FAILS),
            new FirstArgumentMonotoneFunction("(round|bround)_SqlDecimal#_i32", FAILS),
            new FirstArgumentMonotoneFunction("(truncate|trunc)_SqlDecimal#_i32", NEVER_FAILS),
            new FirstArgumentMonotoneFunction("(round|truncate|trunc)_(d|f)#_i32", NEVER_FAILS),
            new FirstArgumentMonotoneFunction(
                    "tumble_(Timestamp|TimestampTz)#_ShortInterval(_Time|_ShortInterval)?", FAILS),
            new FirstArgumentMonotoneFunction("hop_start_timestamp", NEVER_FAILS),
            new DateDiffMonotoneFunction("datediff_(day|hour|week|month|quarter|year)_Date_Date#"));

    /** The function called {@code name}, or Java null if it is never monotone. */
    @Nullable
    static MonotoneFunction find(String name) {
        List<MonotoneFunction> found = Linq.where(FUNCTIONS, f -> f.names.matcher(name).matches());
        Utilities.enforce(found.size() <= 1, () -> "Several monotone functions are named " + name);
        return found.isEmpty() ? null : found.get(0);
    }
}
