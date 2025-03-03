package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.util.Utilities;

/** Visitor which rewrites a {@link DBSPClosureExpression} in a canonical form,
 * by using standard names for parameters (p0, p1, ...).  Handy for writing deterministic tests. */
public class CanonicalForm extends InnerRewriteVisitor {
    final Substitution<DBSPParameter, DBSPParameter> newParam;
    final ResolveReferences resolver;
    int parameterCounter;

    public CanonicalForm(DBSPCompiler compiler) {
        super(compiler, false);
        this.newParam = new Substitution<>();
        // Allow free variables - useful when this is called on inner closures
        this.resolver = new ResolveReferences(compiler, true);
        this.parameterCounter = 0;
    }

    @Override
    public VisitDecision preorder(DBSPParameter param) {
        DBSPParameter replacement = Utilities.getExists(this.newParam, param);
        this.map(param, replacement);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPVariablePath var) {
        IDBSPDeclaration declaration = this.resolver.reference.getDeclaration(var);
        if (declaration.is(DBSPParameter.class)) {
            DBSPParameter replacement = this.newParam.get(declaration.to(DBSPParameter.class));
            assert replacement.getType().sameType(var.getType());
            this.map(var, replacement.asVariable());
            return VisitDecision.STOP;
        }
        return super.preorder(var);
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression node) {
        node.accept(this.resolver);
        DBSPClosureExpression closure = node.to(DBSPClosureExpression.class);
        for (int i = 0; i < closure.parameters.length; i++) {
            DBSPParameter param = closure.parameters[i];
            DBSPParameter replacement = new DBSPParameter("p" + this.parameterCounter++, param.getType());
            this.newParam.substituteNew(param, replacement);
        }
        return super.preorder(node);
    }

    @Override
    public void startVisit(IDBSPInnerNode node) {
        this.newParam.clear();
        this.parameterCounter = 0;
        this.resolver.startVisit(node);
        super.startVisit(node);
    }
}
