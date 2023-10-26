package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;

/**
 * Replaces MUL_WEIGHT with a cast followed by a multiplication.
 */
public class EliminateMulWeight extends InnerRewriteVisitor {
    public EliminateMulWeight(IErrorReporter reporter) {
        super(reporter);
    }

    @Override
    public VisitDecision preorder(DBSPBinaryExpression expression) {
        if (expression.operation.equals(DBSPOpcode.MUL_WEIGHT)) {
            this.push(expression);
            DBSPExpression left = this.transform(expression.left);
            DBSPExpression right = this.transform(expression.right);
            this.pop(expression);
            DBSPExpression result = new DBSPBinaryExpression(
                    expression.getNode(), expression.getType(), DBSPOpcode.MUL,
                    left, right.cast(left.getType()));
            this.map(expression, result);
            return VisitDecision.STOP;
        }
        return super.preorder(expression);
    }
}
