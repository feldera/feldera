package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneTransferFunctions;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;

import java.util.ArrayList;
import java.util.List;

/** Supply monotonicity facts to Rust and reattach its classification to the original SQL expressions. */
final class NativeTemporalComparisons {
    private final MonotoneTransferFunctions monotonicity;
    private final ContainsNow clock;
    private final List<DBSPExpression> expressions = new ArrayList<>();
    private final List<RustCircuitPass.TemporalPredicate> predicates = new ArrayList<>();

    NativeTemporalComparisons(MonotoneTransferFunctions monotonicity) {
        this.monotonicity = monotonicity;
        this.clock = new ContainsNow(monotonicity.compiler, true);
    }

    private RustCircuitPass.ClockOperand operand(DBSPExpression expression) {
        this.clock.apply(expression);
        var monotone = this.monotonicity.maybeGet(expression);
        return new RustCircuitPass.ClockOperand(this.clock.found, monotone != null && monotone.mayBeMonotone());
    }

    private int collect(DBSPExpression expression) {
        RustCircuitPass.TemporalPredicate predicate;
        if (expression instanceof DBSPBinaryExpression binary) {
            if (binary.opcode == DBSPOpcode.AND) {
                int left = this.collect(binary.left);
                int right = this.collect(binary.right);
                predicate = new RustCircuitPass.Conjunction(new int[]{left, right});
            } else {
                predicate = new RustCircuitPass.BinaryPredicate(binary.opcode.name(),
                        this.operand(binary.left), this.operand(binary.right));
            }
        } else {
            this.clock.apply(expression);
            predicate = new RustCircuitPass.OpaquePredicate(this.clock.found);
        }
        int index = this.expressions.size();
        this.expressions.add(expression);
        this.predicates.add(predicate);
        return index;
    }

    private BooleanExpression materialize(DBSPParameter parameter, RustCircuitPass.PredicatePart part) {
        var expression = this.expressions.get(part.index());
        return switch (part.kind()) {
            case NoNow -> new NoNow(expression);
            case Residual -> new NonTemporalFilter(expression);
            case LT, LTE, GT, GTE -> {
                var binary = expression.to(DBSPBinaryExpression.class);
                boolean clockOnLeft = ((RustCircuitPass.BinaryPredicate) this.predicates.get(part.index())).left().clock();
                yield new TemporalFilter(parameter, clockOnLeft ? binary.right : binary.left,
                        clockOnLeft ? binary.left : binary.right, DBSPOpcode.valueOf(part.kind().name()));
            }
        };
    }

    List<BooleanExpression> decompose(DBSPClosureExpression closure) {
        this.collect(closure.body.stripWrapBool());
        List<BooleanExpression> result = new ArrayList<>();
        for (var parts : RustCircuitPass.classifyTemporalPredicates(this.predicates)) {
            var group = this.materialize(closure.parameters[0], parts[0]);
            for (int i = 1; i < parts.length; i++)
                group = group.combine(this.materialize(closure.parameters[0], parts[i]));
            result.add(group);
        }
        return result;
    }
}
