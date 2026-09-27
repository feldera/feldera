package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
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
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.util.Utilities;

/** Rejects plans that the Calcite validator accepts but that Feldera does not support.
 *
 * <p>The rejected constructs are:
 * <ul>
 *   <li>equality comparisons applied to ROW values;</li>
 *   <li>{@code MODE(DISTINCT value)};</li>
 *   <li>RANGE window frames with an offset bound over a nullable ORDER BY expression.</li>
 * </ul>
 *
 * <p>The SQL standard gives ROW comparisons a meaning that may be surprising:
 * fields are compared pairwise using three-valued logic, so
 * {@code ROW(1, NULL) = ROW(1, NULL)} is {@code NULL} rather than {@code TRUE}.
 * Feldera only implements {@code IS [NOT] DISTINCT FROM} instead, which treats
 * {@code NULL} values as equal.  A user-defined type is compiled a ROW type, so the
 * restriction covers user-defined types as well.
 *
 * <p>Row equality is implied in JOIN conditions, a NATURAL JOINs, a USING clause,
 * IN, CASE, and NULLIF.
 *
 * <p>Grouping constructs - GROUP BY, DISTINCT, PARTITION BY, UNION, INTERSECT,
 * EXCEPT - compare ROW values using IS NOT DISTINCT FROM, so they are
 * accepted.  So are the ordering comparisons '&lt;', '&lt;=', '&gt;' and '&gt;='.
 */
public class RejectUnsupportedPlans extends RelHomogeneousShuttle {
    private static final String ERROR_KIND = "Unsupported comparison";

    public static final Documentation.Link ROW_DOCUMENTATION =
            new Documentation.Link("sql/comparisons", "comparing-row-values");
    public static final Documentation.Link WINDOW_DOCUMENTATION =
            new Documentation.Link("sql/unsupported-operations",
                    "range-frames-with-offsets-over-nullable-columns");

    private final CheckExpression checker;

    public RejectUnsupportedPlans(IErrorReporter reporter) {
        this.checker = new CheckExpression(reporter);
    }

    @Override
    public RelNode visit(RelNode other) {
        RelNode node = super.visitChildren(other);
        if (node instanceof Aggregate aggregate)
            checkAggregates(aggregate);
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

    /** MODE is rather useless with DISTINCT */
    static void checkAggregates(Aggregate aggregate) {
        for (AggregateCall agg : aggregate.getAggCallList()) {
            if (agg.getAggregation().getKind() == SqlKind.MODE && agg.isDistinct())
                throw new UnsupportedException("MODE does not support DISTINCT",
                        CalciteObject.create(aggregate, agg));
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
            this.checkRangeFrame(over);
            return super.visitOver(over);
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

        /** True if the input rows cannot have a NULL {@code key}: its type is NOT NULL, or
         * a predicate on the input is not true when {@code key} is NULL. */
        boolean excludesNull(RexNode key) {
            if (this.inputPredicates.isEffectivelyNotNull(key))
                return true;
            if (!(key instanceof RexInputRef ref))
                return false;
            ImmutableBitSet nullColumns = ImmutableBitSet.of(ref.getIndex());
            for (RexNode predicate : this.inputPredicates.pulledUpPredicates)
                if (Strong.isNotTrue(predicate, nullColumns))
                    return true;
            return false;
        }

        static boolean hasOffset(RexWindowBound bound) {
            return !bound.isUnbounded() && !bound.isCurrentRow();
        }

        @Override
        public RexNode visitCall(RexCall call) {
            if (!call.operands.isEmpty() && call.operands.get(0).getType().isStruct())
                this.checkEquality(call);
            return super.visitCall(call);
        }

        /** Report an error if 'call' compares its ROW-typed operands for equality */
        void checkEquality(RexCall call) {
            String message = switch (call.getKind()) {
                case EQUALS -> "ROW values cannot be compared using " +
                        Utilities.singleQuote(call.op.getName()) +
                        "; consider using 'IS NOT DISTINCT FROM' (or its shorthand '<=>') instead";
                case NOT_EQUALS -> "ROW values cannot be compared using " +
                        Utilities.singleQuote(call.op.getName()) +
                        "; consider using 'IS DISTINCT FROM' instead";
                case NULLIF -> "'NULLIF' compares ROW values for equality; consider using " +
                        "'CASE WHEN x IS NOT DISTINCT FROM y THEN NULL ELSE x END' instead";
                default -> null;
            };
            if (message == null)
                return;
            this.reporter.reportError(new SourcePositionRange(call.getParserPosition()),
                    ERROR_KIND, message + ".\n" + ROW_DOCUMENTATION.citation());
        }
    }
}
