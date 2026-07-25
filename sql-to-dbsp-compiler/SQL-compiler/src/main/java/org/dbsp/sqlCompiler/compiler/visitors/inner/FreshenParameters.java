package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Linq;

/** Rewrites closures to use fresh parameter objects with the same names and types. */
public class FreshenParameters extends InnerRewriteVisitor {
    public FreshenParameters(DBSPCompiler compiler) {
        super(compiler, false);
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression closure) {
        this.push(closure);
        DBSPExpression body = this.transform(closure.body);
        this.pop(closure);
        DBSPParameter[] parameters = Linq.map(
                closure.parameters, DBSPParameter::deepCopy, DBSPParameter.class);
        this.map(closure, new DBSPClosureExpression(closure.getNode(), body, parameters));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPType type) {
        return VisitDecision.STOP;
    }

    /** Freshen the parameters of the closures nested inside 'expression'; if the
     * expression itself is a closure, keep its own parameter objects. */
    public static DBSPExpression freshenNested(DBSPCompiler compiler, DBSPExpression expression) {
        if (expression.is(DBSPClosureExpression.class)) {
            DBSPClosureExpression closure = expression.to(DBSPClosureExpression.class);
            DBSPExpression body = new FreshenParameters(compiler)
                    .apply(closure.body).to(DBSPExpression.class);
            return new DBSPClosureExpression(closure.getNode(), body, closure.parameters);
        }
        return new FreshenParameters(compiler).apply(expression).to(DBSPExpression.class);
    }
}
