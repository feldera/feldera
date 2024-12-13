package org.dbsp.sqlCompiler.compiler.visitors.inner.unusedFields;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CanonicalForm;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerRewriteVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Substitution;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;

/** Rewrite field accesses according to a {@link ParameterFieldRemap}.
 * Very similar to the {@link CanonicalForm} visitor. */
public class RewriteFields extends InnerRewriteVisitor {
    final Substitution<DBSPParameter, DBSPParameter> newParam;
    /** Maps original parameters to their remap tables
        param.X is remapped to newParam.Y, where newParam is given
        by the 'newParam' table, and Y is given by fieldRemap[param][X]. */
    final ParameterFieldRemap fieldRemap;
    final ResolveReferences resolver;

    public RewriteFields(DBSPCompiler compiler,
                         Substitution<DBSPParameter, DBSPParameter> newParam,
                         ParameterFieldRemap fieldRemap) {
        super(compiler, false);
        this.fieldRemap = fieldRemap;
        this.newParam = newParam;
        this.resolver = new ResolveReferences(compiler, false);
    }

    public void changeToIdentity(DBSPParameter parameter) {
        this.fieldRemap.changeMap(parameter, FieldMap.identity(parameter.getType()));
        this.newParam.substitute(parameter, parameter);
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
    public VisitDecision preorder(DBSPFieldExpression expression) {
        // Check for a pattern of the form (*param).x
        // to rewrite them as (*newParam).y
        int field = expression.fieldNo;
        if (expression.expression.is(DBSPDerefExpression.class)) {
            DBSPDerefExpression deref = expression.expression.to(DBSPDerefExpression.class);
            if (deref.expression.is(DBSPVariablePath.class)) {
                DBSPVariablePath var = deref.expression.to(DBSPVariablePath.class);
                IDBSPDeclaration declaration = this.resolver.reference.getDeclaration(var);
                if (declaration.is(DBSPParameter.class)) {
                    FieldMap remap = this.fieldRemap.get(declaration.to(DBSPParameter.class));
                    if (remap != null) {
                        field = remap.getNewIndex(field);
                    }
                }
                this.push(expression);
                DBSPExpression source = this.transform(expression.expression);
                this.pop(expression);
                this.map(expression, source.field(field));
                return VisitDecision.STOP;
            }
        }
        return super.preorder(expression);
    }

    @Override
    public void startVisit(IDBSPInnerNode node) {
        this.resolver.apply(node);
        super.startVisit(node);
    }
}
