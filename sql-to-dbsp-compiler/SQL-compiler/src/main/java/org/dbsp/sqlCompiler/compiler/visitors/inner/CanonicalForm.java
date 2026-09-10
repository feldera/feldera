package org.dbsp.sqlCompiler.compiler.visitors.inner;

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
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.aggregate.IAggregate;
import org.dbsp.util.Linq;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

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
    /** Row variable of the aggregate list being rewritten, and its canonical replacement */
    @Nullable String rowVar;
    @Nullable DBSPVariablePath canonicalRowVar;

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

    @Override
    public VisitDecision preorder(DBSPClosureExpression node) {
        node.accept(this.resolver);
        DBSPClosureExpression closure = node.to(DBSPClosureExpression.class);
        for (int i = 0; i < closure.parameters.length; i++) {
            DBSPParameter param = closure.parameters[i];
            String name = param.name.equals(this.rowVar) ?
                    Objects.requireNonNull(this.canonicalRowVar).variable : this.nextName();
            DBSPParameter replacement = new DBSPParameter(param.getNode(), name, param.getType());
            this.newParam.substituteNew(param, replacement);
        }
        return super.preorder(node);
    }

    @Override
    public VisitDecision preorder(DBSPAggregateList list) {
        Utilities.enforce(this.rowVar == null, () -> "Aggregate lists do not nest");
        DBSPVariablePath canonical = new DBSPVariablePath(list.rowVar.getNode(), this.nextName(), list.rowVar.getType());
        this.rowVar = list.rowVar.variable;
        this.canonicalRowVar = canonical;
        this.push(list);
        List<IAggregate> aggregates = Linq.map(list.aggregates, a -> this.transform(a).to(IAggregate.class));
        this.pop(list);
        this.rowVar = null;
        this.canonicalRowVar = null;
        this.map(list, new DBSPAggregateList(list.getNode(), canonical, aggregates));
        return VisitDecision.STOP;
    }

    @Override
    public void startVisit(IDBSPInnerNode node) {
        this.newParam.clear();
        this.newLetVar.clear();
        this.newLetExprVar.clear();
        this.counter = 0;
        this.rowVar = null;
        this.canonicalRowVar = null;
        this.resolver.startVisit(node);
        super.startVisit(node);
    }
}
