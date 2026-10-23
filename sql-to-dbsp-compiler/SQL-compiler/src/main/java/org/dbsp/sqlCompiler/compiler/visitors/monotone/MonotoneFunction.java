package org.dbsp.sqlCompiler.compiler.visitors.monotone;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

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
    /** Prefix of the runtime names of the function, e.g. "exp_" for exp_d and exp_dN */
    final String prefix;
    /** The numbers of arguments the function accepts, e.g. 1 and 2 for ROUND */
    final int[] arities;

    MonotoneFunction(String prefix, int... arities) {
        this.prefix = prefix;
        this.arities = arities;
    }

    /** True if the function accepts {@code arity} arguments. */
    boolean hasArity(int arity) {
        for (int a : this.arities)
            if (a == arity)
                return true;
        return false;
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

    /** True if the function fails for some arguments, e.g. LN of a negative number. */
    boolean canFail() {
        return false;
    }

    /** The monotone functions.  An entry whose prefix extends the prefix of another entry overrides
     * it, e.g. floor_decade_Date overrides floor_. */
    static final List<MonotoneFunction> FUNCTIONS = List.of(
            new LogLikeMonotoneFunction("ln_"),
            new LogLikeMonotoneFunction("log10_"),
            new DateDiffMonotoneFunction(),
            new FirstArgumentMonotoneFunction("round_", 1, 2),
            new RoundingMonotoneFunction("round_SqlDecimal", 2),
            new FirstArgumentMonotoneFunction("bround_", 2),
            new RoundingMonotoneFunction("bround_SqlDecimal", 2),
            new FirstArgumentMonotoneFunction("truncate_", 2),
            new FirstArgumentMonotoneFunction("trunc_", 1, 2),
            new RoundingMonotoneFunction("tumble_", 2, 3),
            new FirstArgumentMonotoneFunction("hop_start_timestamp", 4),
            new UnaryMonotoneFunction("ceil_"),
            new DateRoundingMonotoneFunction("ceil_millennium_Date"),
            new DateRoundingMonotoneFunction("ceil_century_Date"),
            new DateRoundingMonotoneFunction("ceil_decade_Date"),
            new DateRoundingMonotoneFunction("ceil_year_Date"),
            new DateRoundingMonotoneFunction("ceil_quarter_Date"),
            new DateRoundingMonotoneFunction("ceil_month_Date"),
            new DateRoundingMonotoneFunction("ceil_week_Date"),
            new DateRoundingMonotoneFunction("ceil_millennium_Timestamp"),
            new DateRoundingMonotoneFunction("ceil_century_Timestamp"),
            new DateRoundingMonotoneFunction("ceil_decade_Timestamp"),
            new DateRoundingMonotoneFunction("ceil_year_Timestamp"),
            new DateRoundingMonotoneFunction("ceil_quarter_Timestamp"),
            new DateRoundingMonotoneFunction("ceil_month_Timestamp"),
            new DateRoundingMonotoneFunction("ceil_week_Timestamp"),
            new DateRoundingMonotoneFunction("ceil_day_Timestamp"),
            new DateRoundingMonotoneFunction("ceil_hour_Timestamp"),
            new DateRoundingMonotoneFunction("ceil_minute_Timestamp"),
            new DateRoundingMonotoneFunction("ceil_second_Timestamp"),
            new DateRoundingMonotoneFunction("ceil_millisecond_Timestamp"),
            new UnaryMonotoneFunction("cbrt_"),
            new UnaryMonotoneFunction("exp_"),
            new UnaryMonotoneFunction("atan_"),
            new UnaryMonotoneFunction("sinh_"),
            new UnaryMonotoneFunction("asinh_"),
            new UnaryMonotoneFunction("tanh_"),
            new UnaryMonotoneFunction("degrees_"),
            new UnaryMonotoneFunction("radians_"),
            new UnaryMonotoneFunction("floor_"),
            new DateRoundingMonotoneFunction("floor_millennium_Date"),
            new DateRoundingMonotoneFunction("floor_millennium_Timestamp"),
            new DateRoundingMonotoneFunction("floor_century_Date"),
            new DateRoundingMonotoneFunction("floor_century_Timestamp"),
            new DateRoundingMonotoneFunction("floor_decade_Date"),
            new DateRoundingMonotoneFunction("floor_decade_Timestamp"),
            new DateRoundingMonotoneFunction("floor_week_Date"),
            new DateRoundingMonotoneFunction("floor_week_Timestamp"),
            new UnaryMonotoneFunction("sign_"),
            new UnaryMonotoneFunction("extract_year_"),
            new UnaryMonotoneFunction("extract_millennium_"),
            new UnaryMonotoneFunction("extract_century_"),
            new UnaryMonotoneFunction("extract_decade_"),
            new UnaryMonotoneFunction("extract_isoyear_"),
            new UnaryMonotoneFunction("extract_epoch_"),
            new UnaryMonotoneFunction("extract_hour_Time_"),
            new UnaryMonotoneFunction("to_bound_"),
            new UnaryMonotoneFunction("date_trunc_"),
            new DateRoundingMonotoneFunction("date_trunc_decade_"),
            new DateRoundingMonotoneFunction("date_trunc_week_"),
            new UnaryMonotoneFunction("time_trunc_"),
            new UnaryMonotoneFunction("timestamp_trunc_"),
            new DateRoundingMonotoneFunction("timestamp_trunc_decade_"),
            new DateRoundingMonotoneFunction("timestamp_trunc_week_"));

    /** The function called {@code name} with {@code arity} arguments, or null if it is never monotone. */
    @Nullable
    static MonotoneFunction find(String name, int arity) {
        MonotoneFunction result = null;
        for (MonotoneFunction function : FUNCTIONS) {
            boolean matches = name.startsWith(function.prefix) && function.hasArity(arity);
            if (matches && (result == null || function.prefix.length() > result.prefix.length()))
                result = function;
        }
        return result;
    }
}
