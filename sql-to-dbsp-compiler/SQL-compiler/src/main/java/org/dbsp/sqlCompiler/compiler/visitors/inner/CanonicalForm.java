package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.aggregate.IAggregate;
import org.dbsp.util.Linq;
import java.util.List;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Utilities;

/** Visitor which rewrites a {@link DBSPClosureExpression} in a canonical form,
 * by using standard names for parameters (p0, p1, ...).  Handy for generating deterministic code.
 * Unfortunately this needs different variable declarations of the same variable to
 * be different IR nodes. */
public class CanonicalForm extends InnerRewriteVisitor {
    final Substitution<DBSPParameter, DBSPParameter> newParam;
    final Substitution<DBSPLetStatement, DBSPVariablePath> newLetVar;
    final Substitution<DBSPLetExpression, DBSPVariablePath> newLetExprVar;
    final ResolveReferences resolver;
    int counter;

    /** The canonical form of `node` as a string. */
    public static String asString(DBSPCompiler compiler, IDBSPInnerNode node) {
        return new CanonicalForm(compiler).apply(node).toString();
    }

    public CanonicalForm(DBSPCompiler compiler) {
        super(compiler, false);
        this.newParam = new Substitution<>();
        this.newLetVar = new Substitution<>();
        this.newLetExprVar = new Substitution<>();
        // Allow free variables - useful when this is called on inner closures
        this.resolver = new ResolveReferences(compiler, true);
        this.counter = 0;
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
            Utilities.enforce(replacement.getType().sameType(var.getType()));
            this.map(var, replacement.asVariable());
            return VisitDecision.STOP;
        } else if (declaration.is(DBSPLetStatement.class)) {
            DBSPVariablePath repl = this.newLetVar.get(declaration.to(DBSPLetStatement.class));
            this.map(var, repl);
            return VisitDecision.STOP;
        } else if (declaration.is(DBSPLetExpression.class)) {
            DBSPVariablePath repl = this.newLetExprVar.get(declaration.to(DBSPLetExpression.class));
            this.map(var, repl);
            return VisitDecision.STOP;
        }
        return super.preorder(var);
    }

    String nextName() {
        return "p" + this.counter++;
    }

    @Override
    public VisitDecision preorder(DBSPLetStatement stat) {
        DBSPExpression init = this.transformN(stat.initializer);
        DBSPVariablePath var = new DBSPVariablePath(stat.getNode(), this.nextName(), stat.type);
        this.newLetVar.substituteNew(stat, var);
        DBSPLetStatement result;
        if (init == null)
            result = new DBSPLetStatement(var.variable, stat.type, stat.mutable);
        else
            result = new DBSPLetStatement(var.variable, init, stat.mutable);
        this.map(stat, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPLetExpression expr) {
        DBSPExpression init = this.transform(expr.initializer);
        DBSPVariablePath var = new DBSPVariablePath(expr.getNode(), this.nextName(), expr.variable.type);
        this.newLetExprVar.substituteNew(expr, var);
        DBSPExpression consumer = this.transform(expr.consumer);
        DBSPLetExpression result = new DBSPLetExpression(var, init, consumer);
        this.map(expr, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPType type) {
        return VisitDecision.STOP;
    }

    /** An aggregate list declares the row variable its entries share: every entry's row
     * parameter carries the list's row-variable name, so all of them take one canonical name.
     * Reached only under {@code --gen2}; the Rust backend packs the list before this pass. */
    @Override
    public VisitDecision preorder(DBSPAggregateList list) {
        String rowName = this.nextName();
        for (IAggregate aggregate : list.aggregates) {
            for (DBSPParameter param : aggregate.getRowVariableReferences()) {
                if (!this.newParam.containsKey(param))
                    this.newParam.substituteNew(
                            param, new DBSPParameter(param.getNode(), rowName, param.getType()));
            }
        }
        this.push(list);
        // The entries share variable nodes (every step uses the compiler's one weight variable),
        // and the resolver needs a distinct node per declaration.  A deep copy has fresh variable
        // nodes and the same parameter objects, which the substitutions above are keyed by.
        List<IAggregate> aggregates = Linq.map(
                list.aggregates, a -> this.transform(a.deepCopy()).to(IAggregate.class));
        this.pop(list);
        DBSPVariablePath rowVar = new DBSPVariablePath(
                list.rowVar.getNode(), rowName, list.rowVar.getType());
        this.map(list, new DBSPAggregateList(list.getNode(), rowVar, aggregates));
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression node) {
        node.accept(this.resolver);
        DBSPClosureExpression closure = node.to(DBSPClosureExpression.class);
        for (int i = 0; i < closure.parameters.length; i++) {
            DBSPParameter param = closure.parameters[i];
            if (this.newParam.containsKey(param))
                // The row parameter of an aggregate list entry, named by the list.
                continue;
            DBSPParameter replacement = new DBSPParameter(param.getNode(), this.nextName(), param.getType());
            this.newParam.substituteNew(param, replacement);
        }
        return super.preorder(node);
    }

    @Override
    public void startVisit(IDBSPInnerNode node) {
        this.newParam.clear();
        this.newLetVar.clear();
        this.newLetExprVar.clear();
        this.counter = 0;
        this.resolver.startVisit(node);
        super.startVisit(node);
    }
}
