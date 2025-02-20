package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.apache.calcite.util.Pair;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPMapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPStaticExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;

import java.util.ArrayList;
import java.util.List;

/** Use static values when possible */
public class LazyStatics extends InnerRewriteVisitor {
    List<Pair<DBSPStringLiteral, DBSPStaticExpression>> literal;

    public LazyStatics(DBSPCompiler compiler) {
        super(compiler, false);
        this.literal = new ArrayList<>();
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
        DBSPStaticExpression result = null;
        for (var e: this.literal) {
            if (expression.getType().sameType(e.getKey().getType()) &&
                    expression.sameValue(e.getKey())) {
                 result = e.getValue();
                 break;
            }
        }

        if (result == null) {
            result = new DBSPStaticExpression(expression.getNode(), expression);
            this.literal.add(new Pair<>(expression, result));
        }
        this.map(expression, result.applyClone());
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
