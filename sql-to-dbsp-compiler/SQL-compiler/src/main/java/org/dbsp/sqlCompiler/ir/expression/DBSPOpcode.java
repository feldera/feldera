package org.dbsp.sqlCompiler.ir.expression;

/**
 * This enum encodes the various opcodes for unary and
 * binary operations used in the IR of the SQL compiler.
 */
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

    // Binary operations
    ADD("+", false),
    SUB("-", false),
    MUL("*", false),
    DIV("/", false),
    // DIV_NULL is like DIV, but returns NULL for a 0 denominator
    DIV_NULL("/", false),
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
    IS_NOT_DISTINCT("is_not_distinct", false),
    SQL_INDEX("[]", false),
    RUST_INDEX("[]", false),

    // Aggregate operations.  These operations
    // handle NULL values differently from standard
    // arithmetic operations, following SQL semantics.
    AGG_AND("agg_and", true),
    AGG_OR("agg_or", true),
    AGG_XOR("agg_xor", true),
    AGG_MAX("agg_max", true),
    AGG_MIN("agg_min", true),
    AGG_ADD("agg_plus", true),
    // > used in aggregation, for computing ARG_MAX.
    // NULL compares in a special way.
    AGG_GTE("agg_gte", true),
    AGG_LTE("agg_lte", true)
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
                || this.equals(GTE) || this.equals(EQ) || this.equals(NEQ)
                || this.equals(IS_DISTINCT) || this.equals(IS_NOT_DISTINCT);
    }
}
