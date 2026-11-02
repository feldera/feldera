package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.dbsp.sqlCompiler.compiler.Documentation;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.Locale;

/** Rejects plans that the Calcite validator accepts but that Feldera does not support.
 *
 * <p>The rejected constructs are:
 * <ul>
 *   <li>comparisons applied to ROW values, and ordering comparisons applied to ARRAY and
 *   MAP values that contain ROW values;</li>
 *   <li>{@code MODE(DISTINCT value)};</li>
 *   <li>window aggregates with {@code DISTINCT}, such as {@code COUNT(DISTINCT x) OVER (...)};</li>
 *   <li>aggregate calls with a {@code WITHIN DISTINCT} clause;</li>
 *   <li>RANGE window frames with an offset bound over a nullable ORDER BY expression;</li>
 *   <li>{@code SIMILAR TO} and {@code NOT SIMILAR TO}, which Feldera does not implement.</li>
 * </ul>
 *
 * <p>The SQL standard gives ROW comparisons a meaning that may be surprising:
 * fields are compared using three-valued logic, so both
 * {@code ROW(1, NULL) = ROW(1, NULL)} and {@code ROW(1, NULL) <= ROW(1, NULL)}
 * are {@code NULL} rather than {@code TRUE}.
 * Feldera only implements {@code IS [NOT] DISTINCT FROM} instead, which treats
 * {@code NULL} values as equal.  A user-defined type is compiled a ROW type, so the
 * restriction covers user-defined types as well.
 *
 * <p>Row equality is implied in JOIN conditions, a NATURAL JOINs, a USING clause,
 * IN, CASE, and NULLIF.  Row ordering is implied in BETWEEN, GREATEST, LEAST,
 * GREATEST_IGNORE_NULLS, and LEAST_IGNORE_NULLS.
 *
 * <p>Grouping constructs - GROUP BY, DISTINCT, PARTITION BY, UNION, INTERSECT,
 * EXCEPT - compare ROW values using IS NOT DISTINCT FROM, so they are
 * accepted.  Sorting constructs - ORDER BY, MIN, MAX - order ROW values with a
 * NULL field before any other value, so they are accepted too.
 *
 * <p>ARRAY and MAP values compare their elements, keys, and values using
 * IS NOT DISTINCT FROM, so equality is accepted on ARRAY and MAP values that contain
 * ROW values; ordering comparisons are rejected, as they are on ROW values.
 */
public class RejectUnsupportedPlans extends RelHomogeneousShuttle {
    private static final String ERROR_KIND = "Unsupported comparison";

    public static final Documentation.Link ROW_DOCUMENTATION =
            new Documentation.Link("sql/comparisons", "comparing-row-values");
    public static final Documentation.Link WINDOW_DOCUMENTATION =
            new Documentation.Link("sql/unsupported-operations",
                    "range-frames-with-offsets-over-nullable-columns");
    public static final Documentation.Link DISTINCT_WINDOW_DOCUMENTATION =
            new Documentation.Link("sql/unsupported-operations", "distinct-in-window-aggregates");
    public static final Documentation.Link WITHIN_DISTINCT_DOCUMENTATION =
            new Documentation.Link("sql/unsupported-operations", "within-distinct-not-supported");

    private final CheckExpression checker;

    public RejectUnsupportedPlans(IErrorReporter reporter) {
        this.checker = new CheckExpression(reporter);
    }

    @Override
    public RelNode visit(RelNode other) {
        RelNode node = super.visitChildren(other);
        if (node instanceof Aggregate aggregate)
            this.checkAggregates(aggregate);
        // OVER only appears in a Project; the predicates of its input can prove a column NOT NULL
        RelOptPredicateList enclosingPredicates = this.checker.inputPredicates;
        this.checker.inputPredicates = node instanceof Project project ?
                project.getCluster().getMetadataQuery().getPulledUpPredicates(project.getInput()) :
                RelOptPredicateList.EMPTY;
        // The shuttle only reports errors, so the node is returned unchanged
        RelNode result = node.accept(this.checker);
        this.checker.inputPredicates = enclosingPredicates;
        return result;
    }

    /** MODE is rather useless with DISTINCT; WITHIN DISTINCT is not implemented */
    void checkAggregates(Aggregate aggregate) {
        for (AggregateCall agg : aggregate.getAggCallList()) {
            if (agg.getAggregation().getKind() == SqlKind.MODE && agg.isDistinct())
                throw new UnsupportedException("MODE does not support DISTINCT",
                        CalciteObject.create(aggregate, agg));
            if (agg.distinctKeys != null)
                this.checker.reporter.reportError(new SourcePositionRange(agg.getParserPosition()),
                        UnsupportedException.KIND,
                        "WITHIN DISTINCT is not supported.\n" + WITHIN_DISTINCT_DOCUMENTATION.citation());
        }
    }

    /** Reports the offending comparisons and window frames found in an expression. */
    class CheckExpression extends RexShuttle {
        private final IErrorReporter reporter;
        /** Predicates that hold for every row of the input of the expression. */
        RelOptPredicateList inputPredicates = RelOptPredicateList.EMPTY;

        CheckExpression(IErrorReporter reporter) {
            this.reporter = reporter;
        }

        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            subQuery.rel.accept(RejectUnsupportedPlans.this);
            return super.visitSubQuery(subQuery);
        }

        @Override
        public RexNode visitOver(RexOver over) {
            checkDistinct(over);
            this.checkRangeFrame(over);
            return super.visitOver(over);
        }

        /** Throw if a window aggregate uses DISTINCT; the plan cannot be translated */
        static void checkDistinct(RexOver over) {
            if (over.isDistinct())
                throw new UnsupportedException("Window aggregates do not support DISTINCT.\n" +
                        DISTINCT_WINDOW_DOCUMENTATION.citation(),
                        new SourcePositionRange(over.getParserPosition()));
        }

        /** Report an error if a RANGE frame with an offset bound orders by an expression that
         * can be NULL.  The runtime computes such frames from distances between the encoded
         * values, and the encoding of NULL does not preserve distances. */
        void checkRangeFrame(RexOver over) {
            RexWindow window = over.getWindow();
            if (window.isRows() || window.orderKeys.isEmpty())
                return;
            if (!hasOffset(window.getLowerBound()) && !hasOffset(window.getUpperBound()))
                return;
            RexNode key = window.orderKeys.get(0).left;
            if (this.excludesNull(key))
                return;
            this.reporter.reportError(new SourcePositionRange(over.getParserPosition()),
                    UnsupportedException.KIND,
                    "A RANGE window frame with a PRECEDING or FOLLOWING offset requires an " +
                    "ORDER BY expression that cannot be NULL; consider filtering out the NULL values, " +
                    "for example with 'WHERE column IS NOT NULL'.\n" + WINDOW_DOCUMENTATION.citation());
        }

        /** True if the input rows cannot have a NULL {@code key}: its type is NOT NULL, a
         * predicate on the input is not true when {@code key} is NULL, or {@code key} is NULL
         * only when one of its operands is NULL and no operand can be NULL. */
        boolean excludesNull(RexNode key) {
            if (this.inputPredicates.isEffectivelyNotNull(key))
                return true;
            if (key instanceof RexInputRef ref) {
                ImmutableBitSet nullColumns = ImmutableBitSet.of(ref.getIndex());
                for (RexNode predicate : this.inputPredicates.pulledUpPredicates)
                    if (Strong.isNotTrue(predicate, nullColumns))
                        return true;
                return false;
            }
            if (key instanceof RexCall call && Strong.policy(call) == Strong.Policy.ANY) {
                for (RexNode operand : call.getOperands())
                    if (!this.excludesNull(operand))
                        return false;
                return true;
            }
            return false;
        }

        static boolean hasOffset(RexWindowBound bound) {
            return !bound.isUnbounded() && !bound.isCurrentRow();
        }

        @Override
        public RexNode visitCall(RexCall call) {
            checkSimilar(call);
            this.checkComparison(call);
            return super.visitCall(call);
        }

        /** True if 'type' is a ROW type, or an ARRAY or MAP type with a ROW type inside */
        static boolean containsRow(RelDataType type) {
            if (type.isStruct())
                return true;
            RelDataType element = type.getComponentType();
            if (element != null)
                return containsRow(element);
            RelDataType key = type.getKeyType();
            RelDataType value = type.getValueType();
            return key != null && value != null && (containsRow(key) || containsRow(value));
        }

        /** Throw if 'call' is SIMILAR TO; Calcite would otherwise fold it when its operands are constant */
        static void checkSimilar(RexCall call) {
            if (call.getKind() == SqlKind.SIMILAR)
                throw new UnimplementedException("Function " + Utilities.singleQuote(call.getOperator().toString())
                        + " not yet implemented", 1265, CalciteObject.create(call.getParserPosition()));
        }

        /** Report an error if 'call' compares its ROW-typed operands, or orders operands
         * that contain ROW values */
        void checkComparison(RexCall call) {
            // An operand that is a NULL literal does not have a ROW type
            boolean isRow = Linq.any(call.operands, operand -> operand.getType().isStruct());
            boolean hasRow = Linq.any(call.operands, operand -> containsRow(operand.getType()));
            if (!hasRow)
                return;
            String operator = Utilities.singleQuote(call.op.getName().toUpperCase(Locale.ROOT));
            String message = switch (call.getKind()) {
                case EQUALS -> !isRow ? null : "ROW values cannot be compared using " + operator +
                        "; consider using 'IS NOT DISTINCT FROM' (or its shorthand '<=>') instead";
                case NOT_EQUALS -> !isRow ? null : "ROW values cannot be compared using " + operator +
                        "; consider using 'IS DISTINCT FROM' instead";
                case NULLIF -> !isRow ? null : "'NULLIF' compares ROW values for equality; consider using " +
                        "'CASE WHEN x IS NOT DISTINCT FROM y THEN NULL ELSE x END' instead";
                case LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL,
                     GREATEST, LEAST, GREATEST_PG, LEAST_PG -> isRow ?
                        "ROW values cannot be compared using " + operator + "; compare their fields instead" :
                        "ARRAY and MAP values that contain ROW values cannot be compared using " + operator;
                default -> null;
            };
            if (message == null)
                return;
            this.reporter.reportError(new SourcePositionRange(call.getParserPosition()),
                    ERROR_KIND, message + ".\n" + ROW_DOCUMENTATION.citation());
        }
    }
}
