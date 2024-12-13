package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;

/** Visitor which rewrites a {@link DBSPClosureExpression} in a canonical form,
 * by using standard names for parameters (p0, p1, ...).  Handy for writing deterministic tests. */
public class CanonicalForm extends InnerRewriteVisitor {
    final Substitution<DBSPParameter, DBSPParameter> newParam;
    final ResolveReferences resolver;

    public CanonicalForm(DBSPCompiler compiler) {
        super(compiler, false);
        this.newParam = new Substitution<>();
        this.resolver = new ResolveReferences(compiler, false);
    }

    @Override
    public VisitDecision preorder(DBSPParameter param) {
        DBSPParameter replacement = this.newParam.get(param);
        this.map(param, replacement);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPVariablePath var) {
        IDBSPDeclaration declaration = this.resolver.reference.getDeclaration(var);
        if (declaration.is(DBSPParameter.class)) {
            DBSPParameter replacement = this.newParam.get(declaration.to(DBSPParameter.class));
            this.map(var, replacement.asVariable());
            return VisitDecision.STOP;
        }
        return super.preorder(var);
    }

    @Override
    public void startVisit(IDBSPInnerNode node) {
        this.resolver.apply(node);
        // Only works for Closures
        DBSPClosureExpression closure = node.to(DBSPClosureExpression.class);
        for (int i = 0; i < closure.parameters.length; i++)
            this.newParam.substitute(closure.parameters[i],
                    new DBSPParameter("p" + i, closure.parameters[i].getType()));
        super.startVisit(node);
    }
}
