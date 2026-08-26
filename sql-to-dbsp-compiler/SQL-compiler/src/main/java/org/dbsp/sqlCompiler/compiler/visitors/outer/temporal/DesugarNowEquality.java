package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerRewriteVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;

/** Rewrites 'e = expr(now())' in filter conditions into 'e >= expr(now()) AND e <= expr(now())'. */
public class DesugarNowEquality extends CircuitCloneVisitor {
    final Rewriter rewriter;

    public DesugarNowEquality(DBSPCompiler compiler) {
        super(compiler, false);
        this.rewriter = new Rewriter(compiler);
    }

    static class Rewriter extends InnerRewriteVisitor {
        final ContainsNow containsNow;

        Rewriter(DBSPCompiler compiler) {
            super(compiler, false);
            this.containsNow = new ContainsNow(compiler, true);
        }

        boolean hasNow(DBSPExpression expression) {
            this.containsNow.apply(expression);
            return this.containsNow.found;
        }

        @Override
        public VisitDecision preorder(DBSPBinaryExpression expression) {
            if (expression.opcode != DBSPOpcode.EQ
                    || !expression.left.getType().is(DBSPTypeBaseType.class)
                    || this.hasNow(expression.left) == this.hasNow(expression.right))
                return super.preorder(expression);
            this.push(expression);
            DBSPExpression left = this.transform(expression.left);
            DBSPExpression right = this.transform(expression.right);
            this.pop(expression);
            DBSPExpression ge = new DBSPBinaryExpression(
                    expression.getNode(), expression.getType(), DBSPOpcode.GTE, left, right);
            DBSPExpression le = new DBSPBinaryExpression(
                    expression.getNode(), expression.getType(), DBSPOpcode.LTE,
                    left.deepCopy(), right.deepCopy());
            DBSPExpression result = new DBSPBinaryExpression(
                    expression.getNode(), expression.getType(), DBSPOpcode.AND, ge, le);
            this.map(expression, result);
            return VisitDecision.STOP;
        }
    }

    @Override
    public void postorder(DBSPFilterOperator filter) {
        DBSPClosureExpression function = filter.getClosureFunction();
        if (!ContainsNow.find(this.compiler(), function)) {
            super.postorder(filter);
            return;
        }
        DBSPClosureExpression rewritten = this.rewriter
                .apply(function.ensureTree(this.compiler()))
                .to(DBSPClosureExpression.class);
        if (rewritten == function) {
            super.postorder(filter);
            return;
        }
        DBSPFilterOperator replacement = new DBSPFilterOperator(
                filter.getRelNode(), rewritten, this.mapped(filter.input()));
        this.map(filter, replacement);
    }
}
