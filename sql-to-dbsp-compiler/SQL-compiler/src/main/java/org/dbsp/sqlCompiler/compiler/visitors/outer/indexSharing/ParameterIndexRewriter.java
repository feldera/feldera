package org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing;

import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ExpressionTranslator;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Utilities;
import org.dbsp.util.Linq;

/** Rewrites a closure whose parameters read the values of {@link DBSPMapIndexOperator}s so that
 * they read the wider values of the shared MapIndexes that replace them. */
class ParameterIndexRewriter extends ExpressionTranslator {
    final ResolveReferences resolver;
    /** Key is a parameter to replace; value is a fresh variable of the shared value type
     * together with a map from a field index in the original value to the index of the same
     * field in the shared value.  A field entry {@code a -> b} turns {@code (*param).a} into
     * {@code (*var).b}. */
    final ReplaceSharedIndexes.ParameterIndexMapSet rewriteMap;

    public ParameterIndexRewriter(DBSPCompiler compiler, ReplaceSharedIndexes.ParameterIndexMapSet rewriteMap) {
        super(compiler);
        this.resolver = new ResolveReferences(compiler, false);
        this.rewriteMap = rewriteMap;
    }

    @Override
    public IDBSPInnerNode apply(IDBSPInnerNode node) {
        IDBSPInnerNode result = super.apply(node);
        if (node.is(DBSPClosureExpression.class)) {
            // The closure reads the same fields from a wider value, so it computes the same
            // result.
            DBSPType before = node.to(DBSPClosureExpression.class).getResultType();
            DBSPType after = result.to(DBSPClosureExpression.class).getResultType();
            Utilities.enforce(after.sameType(before),
                    () -> "Reading the shared index changed the result of " + node +
                            "\nfrom " + before + " to " + after);
        }
        return result;
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

    @Override
    public void postorder(DBSPClosureExpression closure) {
        if (this.maybeGet(closure) != null) {
            // Already translated
            return;
        }
        DBSPExpression body = this.getE(closure.body);
        DBSPParameter[] parameters = Linq.map(closure.parameters, this::rewriteParameter, DBSPParameter.class);
        this.map(closure, new DBSPClosureExpression(closure.getNode(), body, parameters));
    }

    DBSPParameter rewriteParameter(DBSPParameter parameter) {
        var map = this.rewriteMap.get(parameter);
        if (map == null)
            return parameter;
        return map.var().asParameter();
    }
}
