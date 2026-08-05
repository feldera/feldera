package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlKind;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.util.Utilities;

/** Rejects plans that the Calcite validator accepts but that Feldera does not support.
 *
 * <p>Two constructs are rejected: equality comparisons applied to ROW values, and
 * {@code MODE(DISTINCT value)}.
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

    private static final String ROW_DOCUMENTATION =
            "See https://docs.feldera.com/sql/comparisons#comparing-row-values";

    private final CheckExpression checker;

    public RejectUnsupportedPlans(IErrorReporter reporter) {
        this.checker = new CheckExpression(reporter);
    }

    @Override
    public RelNode visit(RelNode other) {
        RelNode node = super.visitChildren(other);
        if (node instanceof Aggregate aggregate)
            checkAggregates(aggregate);
        // The shuttle only reports errors, so the node is returned unchanged
        return node.accept(this.checker);
    }

    /** MODE is rather useless with DISTINCT */
    static void checkAggregates(Aggregate aggregate) {
        for (AggregateCall agg : aggregate.getAggCallList()) {
            if (agg.getAggregation().getKind() == SqlKind.MODE && agg.isDistinct())
                throw new UnsupportedException("MODE does not support DISTINCT",
                        CalciteObject.create(aggregate, agg));
        }
    }

    /** Reports the offending comparisons found in an expression. */
    class CheckExpression extends RexShuttle {
        private final IErrorReporter reporter;

        CheckExpression(IErrorReporter reporter) {
            this.reporter = reporter;
        }

        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            subQuery.rel.accept(RejectUnsupportedPlans.this);
            return super.visitSubQuery(subQuery);
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
                    ERROR_KIND, message + ".\n" + ROW_DOCUMENTATION);
        }
    }
}
