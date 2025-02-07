package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPMapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPStaticExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;

/** Use static values when possible */
public class LazyStatics extends InnerRewriteVisitor {
    public LazyStatics(DBSPCompiler compiler) {
        super(compiler, false);
    }

    @Override
    public VisitDecision preorder(DBSPMapExpression expression) {
        if (!expression.isConstant()) {
            return super.preorder(expression);
        }
        // No need to recurse; just do this expression
        DBSPExpression result = new DBSPStaticExpression(expression.getNode(), expression)
                .applyClone();
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStringLiteral expression) {
        DBSPExpression result = new DBSPStaticExpression(expression.getNode(), expression)
                .applyClone();
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPArrayExpression expression) {
        if (!expression.isConstant()) {
            return super.preorder(expression);
        }
        // No need to recurse; just do this expression
        DBSPExpression result = new DBSPStaticExpression(expression.getNode(), expression)
                .applyClone();
        this.map(expression, result);
        return VisitDecision.STOP;
    }
}
