package org.dbsp.sqlCompiler.ir.expression;

/** This enum encodes the various opcodes for unary and
 * binary operations used in the IR of the SQL compiler. */
public enum DBSPOpcode {
    // Unary operations
    WRAP_BOOL("wrap_bool", false),
    NEG("-", false),
    UNARY_PLUS("+", false),
    NOT("!", false),
    // Indicator(x) = if x == null { 0 } else { 1 }
    INDICATOR("indicator", false),
    IS_FALSE("is_false", false),
    IS_TRUE("is_true", false),
    IS_NOT_TRUE("is_not_true", false),
    IS_NOT_FALSE("is_not_false", false),
    TYPEDBOX("TypedBox::new", false),

    // Binary operations
    ADD("+", false),
    SUB("-", false),
    MUL("*", false),
    DIV("/", false),
    // DIV_NULL is like DIV, but returns NULL for a 0 denominator
    DIV_NULL("div_null", false),
    MOD("%", false),
    EQ("==", false),
    NEQ("!=", false),
    LT("<", false),
    GT(">", false),
    LTE("<=", false),
    GTE(">=", false),
    AND("&&", false),
    BW_AND("&", false),
    MUL_WEIGHT("mul_weight", false),
    OR("||", false),
    BW_OR("|", false), // bitwise or
    XOR("^", false),
    MAX("max", false),
    MIN("min", false),
    CONCAT("||", false),
    IS_DISTINCT("is_distinct", false),
    SQL_INDEX("[]", false),
    MAP_INDEX("[]", false),
    // map index in a variant value
    VARIANT_INDEX("[]", false),
    RUST_INDEX("[]", false),
    // Shift left a decimal number by a number of decimal digits.
    // Shift amount may be negative
    SHIFT_LEFT("shift_left", false),

    // Timestamp-based operations
    TS_ADD("+", false),
    TS_SUB("-", false),
    // Interval-based operations
    INTERVAL_MUL("*", false),
    INTERVAL_DIV("/", false),

    // Aggregate operations.  These operations
    // handle NULL values differently from standard
    // arithmetic operations, following SQL semantics.
    AGG_AND("agg_and", true),
    AGG_OR("agg_or", true),
    // Operation which combines an accumulator and a *weighted* value
    AGG_XOR("agg_xor", true),
    AGG_MAX("agg_max", true),
    AGG_MIN("agg_min", true),
    // Operation which combines an accumulator and a *weighted* value
    AGG_ADD("agg_plus", true),
    // > used in aggregation, for computing ARG_MAX.
    // NULL compares in a special way, since it means "uninitialized"
    AGG_GTE("agg_gte", true),
    AGG_LTE("agg_lte", true),
    // Yet another way to compare, used in controlled filters after a waterline
    // The left argument is the data compared, the right argument is the current waterline
    // A NULL on either side returns true.
    // Otherwise, this returns left >= right.
    CONTROLLED_FILTER_GTE("cf_compare_gte", true),
    // Higher order operation: apply a function to every element of an array
    ARRAY_CONVERT("array_map", false),
    // Apply a function to every key-value element of a map
    MAP_CONVERT("map_map", false),
    ;

    private final String text;
    public final boolean isAggregate;

    DBSPOpcode(String text, boolean isAggregate) {
        this.text = text;
        this.isAggregate = isAggregate;
    }

    @Override
    public String toString() {
        return this.text;
    }

    public boolean isComparison() {
        // Some things like AGG_GTE are not listed as comparisons, since
        // their return type follows different rules
        return this.equals(LT) || this.equals(GT) || this.equals(LTE)
                || this.equals(GTE) || this.equals(EQ) || this.equals(NEQ);
    }
}
