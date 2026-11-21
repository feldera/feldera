package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneTransferFunctions;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Supply monotonicity facts to Rust and reattach its classification to the original SQL expressions. */
final class NativeTemporalComparisons {
    private final MonotoneTransferFunctions monotonicity;

    NativeTemporalComparisons(MonotoneTransferFunctions monotonicity) {
        this.monotonicity = monotonicity;
    }

    private void collectMonotone(DBSPExpression operand, Set<Long> ids) {
        var fact = this.monotonicity.maybeGet(operand);
        if (fact != null && fact.mayBeMonotone())
            ids.add(operand.getId());
    }

    List<BooleanExpression> decompose(DBSPClosureExpression closure) {
        DBSPExpression body = closure.body.stripWrapBool();
        var monotone = new HashSet<Long>();
        for (var term : body.conjuncts()) {
            if (term instanceof DBSPBinaryExpression binary) {
                this.collectMonotone(binary.left, monotone);
                this.collectMonotone(binary.right, monotone);
            }
        }
        var parts = RustCircuitPass.classifyTemporalPredicates(this.monotonicity.compiler, body, monotone);
        var result = new ArrayList<BooleanExpression>(parts.size());
        for (var part : parts) {
            result.add(switch (part.kind()) {
                case NoNow -> new NoNow(part.expression());
                case Residual -> new NonTemporalFilter(part.expression());
                case LT, LTE, GT, GTE -> new TemporalFilter(closure.parameters[0], part.expression(),
                        Objects.requireNonNull(part.clock()), DBSPOpcode.valueOf(part.kind().name()));
            });
        }
        return result;
    }
}
