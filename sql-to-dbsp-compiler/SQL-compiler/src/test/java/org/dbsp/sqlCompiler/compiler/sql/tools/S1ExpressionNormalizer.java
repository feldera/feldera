package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.FelderaS1ExpressionFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.function.UnaryOperator;

/** Expand SEARCH once per original object across every relation and function.
 * Unchanged objects keep their identity; equal distinct objects stay distinct. */
final class S1ExpressionNormalizer implements UnaryOperator<RexNode> {
    private final RexBuilder builder;
    private final IdentityHashMap<RexNode, RexNode> normalized = new IdentityHashMap<>();
    private final IdentityHashMap<RexNode, Boolean> active = new IdentityHashMap<>();

    S1ExpressionNormalizer(RexBuilder builder) { this.builder = builder; }

    @Override public RexNode apply(RexNode node) {
        RexNode previous = this.normalized.get(node);
        if (previous != null) return previous;
        if (this.active.size() >= 256 || this.active.put(node, true) != null)
            throw new IllegalArgumentException("Cyclic or overdeep expression normalization");
        if (this.normalized.size() >= 100_000)
            throw new IllegalArgumentException("Expression normalization exceeds object limit");
        RexNode result = node;
        if (node.getClass() == RexCall.class || node.getClass() == Window.RexWinAggCall.class) {
            var call = (RexCall) node;
            var operands = new ArrayList<RexNode>();
            boolean changed = false;
            for (RexNode operand: call.getOperands()) {
                RexNode replacement = this.apply(operand);
                operands.add(replacement); changed |= replacement != operand;
            }
            // SEARCH can introduce AND/OR beneath an existing call of the
            // same kind. Filter constructors require these calls to be flat.
            // Flatten only associative Boolean lists; retain declared types,
            // coordinates, operand order, and shared leaf objects.
            if (call.getKind() == SqlKind.AND || call.getKind() == SqlKind.OR) {
                var flattened = RexUtil.flatten(operands, call.getOperator());
                boolean differs = flattened.size() != operands.size();
                for (int index = 0; !differs && index < flattened.size(); index++)
                    differs = flattened.get(index) != operands.get(index);
                if (differs) {
                    operands = new ArrayList<>(flattened); changed = true;
                }
            }
            if (changed) {
                if (call instanceof Window.RexWinAggCall window)
                    result = new Window.RexWinAggCall(call.getParserPosition(), (SqlAggFunction) call.getOperator(),
                            call.getType(), operands, window.ordinal, window.distinct, window.ignoreNulls);
                else result = FelderaS1ExpressionFactory.call(call.getParserPosition(), call.getType(), call.getOperator(), operands);
            }
            if (call.getKind() == SqlKind.SEARCH) {
                RexNode expanded = RexUtil.expandSearch(this.builder, null, result);
                if (expanded == result) throw new IllegalArgumentException("SEARCH expansion made no progress");
                result = this.apply(expanded);
            }
        } else if (node.getClass() == RexFieldAccess.class) {
            var field = (RexFieldAccess) node;
            RexNode input = this.apply(field.getReferenceExpr());
            if (input != field.getReferenceExpr())
                result = FelderaS1ExpressionFactory.field(input, field.getField().getIndex(), field.getType());
        } else if (node.getClass() == RexLambda.class) {
            var lambda = (RexLambda) node;
            RexNode body = this.apply(lambda.getExpression());
            if (body != lambda.getExpression()) result = FelderaS1ExpressionFactory.lambda(lambda.getParameters(), body);
        }
        this.active.remove(node); this.normalized.put(node, result);
        return result;
    }
}
