package org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ExpressionTranslator;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;

/**
 * For a parameter param this is given a map from integer to integer and a variable.
 * If the map[a] = b, this rewrites (*param).a to (*var).b
 */
class ParameterIndexRewriter extends ExpressionTranslator {
    final ResolveReferences resolver;
    final ReplaceSharedIndexes.ParameterIndexMapSet rewriteMap;

    public ParameterIndexRewriter(DBSPCompiler compiler, ReplaceSharedIndexes.ParameterIndexMapSet rewriteMap) {
        super(compiler);
        this.resolver = new ResolveReferences(compiler, false);
        this.rewriteMap = rewriteMap;
    }

    @Override
    public void startVisit(IDBSPInnerNode node) {
        super.startVisit(node);
        this.resolver.apply(node);
    }

    @Override
    public void postorder(DBSPVariablePath var) {
        if (this.maybeGet(var) != null) {
            // Already translated
            return;
        }
        var decl = this.resolver.reference.getDeclaration(var);
        if (decl.is(DBSPParameter.class)) {
            var map = this.rewriteMap.get(decl.to(DBSPParameter.class));
            if (map != null) {
                this.map(var, map.var().deepCopy());
                return;
            }
        }
        super.postorder(var);
    }

    @Override
    public void postorder(DBSPFieldExpression field) {
        if (this.maybeGet(field) != null) {
            // Already translated
            return;
        }
        if (field.expression.is(DBSPDerefExpression.class)) {
            var deref = field.expression.to(DBSPDerefExpression.class);
            if (deref.expression.is(DBSPVariablePath.class)) {
                var var = deref.expression.to(DBSPVariablePath.class);
                var decl = this.resolver.reference.getDeclaration(var);
                if (decl.is(DBSPParameter.class)) {
                    var map = this.rewriteMap.get(decl.to(DBSPParameter.class));
                    if (map != null) {
                        Integer newField = map.indexRemap().get(field.fieldNo);
                        if (newField == null)
                            newField = field.fieldNo;
                        this.map(field, map.var().deepCopy().deref().field(newField));
                        return;
                    }
                }
            }
        }
        super.postorder(field);
    }
}
