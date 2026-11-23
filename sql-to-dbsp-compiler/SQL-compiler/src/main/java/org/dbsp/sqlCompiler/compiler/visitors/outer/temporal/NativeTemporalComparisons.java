package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
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

    private record Inputs(List<String> kinds, List<IDBSPInnerNode> roots) {
        Inputs() {
            this(new ArrayList<>(), new ArrayList<>());
        }

        void add(BooleanExpression term) {
            if (term instanceof TemporalFilter bound) {
                this.kinds.add(bound.opcode().name());
                this.roots.add(bound.parameter());
                this.roots.add(bound.noNow());
            } else if (term instanceof NoNow predicate) {
                this.kinds.add("NoNow");
                this.roots.add(predicate.noNow());
            } else {
                this.kinds.add("Residual");
                this.roots.add(term.to(NonTemporalFilter.class).expression());
            }
        }
    }

    static List<BooleanExpression> combine(DBSPCompiler compiler, List<BooleanExpression> terms) {
        var inputs = new Inputs();
        terms.forEach(inputs::add);
        var groups = RustCircuitPass.groupTemporalFilters(compiler, inputs.kinds(), inputs.roots());
        if (groups == null)
            return RewriteNow.combineExpressions(terms);
        var result = new ArrayList<BooleanExpression>(groups.size());
        for (var group : groups) {
            var first = terms.get(group.first());
            if (first instanceof TemporalFilter) {
                var bounds = terms.subList(group.first(), group.first() + group.count()).stream()
                        .map(term -> term.to(TemporalFilter.class)).toList();
                result.add(new TemporalFilterList(bounds));
            } else {
                var expression = Objects.requireNonNull(group.expression());
                result.add(first instanceof NoNow ? new NoNow(expression) : new NonTemporalFilter(expression));
            }
        }
        return result;
    }

    static boolean canMerge(DBSPCompiler compiler, List<BooleanExpression> upstream, TemporalFilter downstream) {
        var inputs = new Inputs();
        upstream.forEach(inputs::add);
        inputs.add(downstream);
        var compatible = RustCircuitPass.acceptsTemporalBound(compiler, inputs.kinds(), inputs.roots(), true);
        return compatible != null ? compatible : upstream.stream().allMatch(term ->
                term instanceof TemporalFilter && term.compatible(downstream));
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
        return bind(closure.parameters[0], parts);
    }

    static List<BooleanExpression> bind(DBSPParameter parameter, List<RustCircuitPass.TemporalComparison> parts) {
        var result = new ArrayList<BooleanExpression>(parts.size());
        for (var part : parts) {
            result.add(switch (part.kind()) {
                case NoNow -> new NoNow(part.expression());
                case Residual -> new NonTemporalFilter(part.expression());
                case LT, LTE, GT, GTE -> new TemporalFilter(parameter, part.expression(),
                        Objects.requireNonNull(part.clock()), DBSPOpcode.valueOf(part.kind().name()));
            });
        }
        return result;
    }
}
