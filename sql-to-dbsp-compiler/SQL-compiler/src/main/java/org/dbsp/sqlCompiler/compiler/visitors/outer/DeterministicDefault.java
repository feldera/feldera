package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPFunctionItem;
import org.dbsp.util.Logger;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/** Check that default values for all column initializers are constant expressions.
 * This requires building a call graph. */
public class DeterministicDefault extends CircuitVisitor {
    final Set<String> nonDeterministic;

    static class HasNondeterminism extends InnerVisitor {
        public boolean found;
        final Set<String> nonDeterministic;

        public HasNondeterminism(DBSPCompiler compiler, Set<String> nonDeterministic) {
            super(compiler);
            this.found = false;
            this.nonDeterministic = nonDeterministic;
        }

        @Override
        public VisitDecision preorder(DBSPApplyExpression node) {
            String name = node.getFunctionName();
            if (name != null) {
                boolean nonDet = this.nonDeterministic.contains(name.toLowerCase());
                if (nonDet) {
                    Logger.INSTANCE.belowLevel(this, 1)
                            .append("Found nondeterministic call to ")
                            .append(name)
                            .newline();
                    this.found = true;
                }
            }
            return super.preorder(node);
        }

        @Override
        public void startVisit(IDBSPInnerNode node) {
            super.startVisit(node);
            this.found = false;
        }

        public boolean found() {
            return this.found;
        }
    }


    final HasNondeterminism hn;

    public DeterministicDefault(DBSPCompiler compiler) {
        super(compiler);
        this.nonDeterministic = new HashSet<>();
        this.markNondeterministic("now");
        this.hn = new HasNondeterminism(this.compiler, this.nonDeterministic);
    }

    @Override
    public VisitDecision preorder(DBSPSourceMultisetOperator operator) {
        for (var meta: operator.metadata.getColumns()) {
            if (meta.defaultValue != null) {
                boolean det = this.isDeterministic(meta.defaultValue);
                if (!det) {
                    this.compiler.reportError(
                            Objects.requireNonNull(meta.defaultValuePosition),
                            "Non-deterministic initializer",
                            "Default value for column " +
                                    meta.name.singleQuote() +
                                    " must be a compile-time constant.\n" +
                            "See https://github.com/feldera/feldera/issues/2051");
                }
            }
        }
        return VisitDecision.STOP;
    }

    void markNondeterministic(String function) {
        this.nonDeterministic.add(function.toLowerCase());
    }

    @Override
    public VisitDecision preorder(DBSPDeclaration decl) {
        if (decl.item.is(DBSPFunctionItem.class)) {
            DBSPFunctionItem function = decl.item.to(DBSPFunctionItem.class);
            if (function.function.body == null)
                return VisitDecision.STOP;
            boolean det = this.isDeterministic(function.function.body);
            if (!det)
                this.markNondeterministic(function.function.getName());
        }
        return VisitDecision.STOP;
    }

    boolean isDeterministic(DBSPExpression expression) {
        this.hn.apply(expression);
        return !this.hn.found();
    }
}
