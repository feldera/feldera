package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CanonicalForm;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerRewriteVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Substitution;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.aggregate.LinearAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.MinMaxAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.NonLinearAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;

/** Rewrite field accesses according to a {@link ParameterFieldRemap}.
 * Very similar to the {@link CanonicalForm} visitor. */
public class RewriteFields extends InnerRewriteVisitor {
    final Substitution<DBSPParameter, DBSPParameter> newParam;
    /** Maps original parameters to their remap tables
        param.X is remapped to newParam.Y, where newParam is given
        by the 'newParam' table, and Y is given by fieldRemap[param][X]. */
    final ParameterFieldRemap fieldRemap;
    final ResolveReferences resolver;
    final int depth;
    /** Field use map for the currently evaluated expression */
    @Nullable FieldUseMap current;
    int currentDepth;

    public RewriteFields(DBSPCompiler compiler,
                         Substitution<DBSPParameter, DBSPParameter> newParam,
                         ParameterFieldRemap fieldRemap,
                         int depth) {
        super(compiler, false);
        this.fieldRemap = fieldRemap;
        this.newParam = newParam;
        this.resolver = new ResolveReferences(compiler, false);
        this.depth = depth;
        this.current = null;
        this.currentDepth = 0;
    }

    /** Essentially says that "all fields of this parameter are used" */
    public void parameterFullyUsed(DBSPParameter parameter) {
        this.fieldRemap.changeMap(parameter, FieldUseMap.identity(parameter.getType()));
        this.newParam.substitute(parameter, parameter);
    }

    void setCurrent(@Nullable FieldUseMap map, int depth) {
        this.current = map;
        this.currentDepth = depth;
    }

    @Override
    public VisitDecision preorder(DBSPParameter param) {
        DBSPParameter replacement = this.newParam.get(param);
        this.map(param, replacement);
        this.setCurrent(this.fieldRemap.get(param), 0);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPVariablePath var) {
        IDBSPDeclaration declaration = this.resolver.reference.getDeclaration(var);
        if (declaration.is(DBSPParameter.class)) {
            DBSPParameter param = declaration.to(DBSPParameter.class);
            DBSPParameter replacement = this.newParam.get(param);
            this.map(var, replacement.asVariable());
            this.setCurrent(this.fieldRemap.get(param), 0);
            return VisitDecision.STOP;
        } else {
            this.setCurrent(null, 0);
        }
        return super.preorder(var);
    }

    @Override
    public VisitDecision preorder(DBSPDerefExpression expression) {
        super.preorder(expression);
        if (this.current != null)
            this.setCurrent(this.current.deref(), this.currentDepth);
        return VisitDecision.STOP;
    }

    @Override
    public void visitingExpression(DBSPExpression expression) {
        if (expression.is(DBSPDerefExpression.class) ||
                expression.is(DBSPFieldExpression.class) ||
                expression.is(DBSPVariablePath.class)) return;
        this.setCurrent(null, 0);
    }

    @Override
    public VisitDecision preorder(DBSPFieldExpression expression) {
        int field = expression.fieldNo;
        this.push(expression);
        DBSPExpression source = this.transform(expression.expression);
        this.pop(expression);
        if (this.current != null && this.currentDepth < this.depth) {
            FieldUseMap next = this.current.field(field);
            field = this.current.getNewIndex(field);
            this.setCurrent(next, currentDepth+1);
        }
        DBSPExpression result = source.field(field);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression expression) {
        for (IDBSPInnerNode node: this.context) {
            if (node.is(DBSPClosureExpression.class)) {
                // Nested closure
                this.map(expression, expression);
                return VisitDecision.STOP;
            }
        }
        return super.preorder(expression);
    }

    @Override
    public VisitDecision preorder(NonLinearAggregate aggregate) {
        this.parameterFullyUsed(aggregate.increment.parameters[0]);
        this.parameterFullyUsed(aggregate.increment.parameters[2]);
        DBSPExpression increment = this.transform(aggregate.increment);
        NonLinearAggregate result = new NonLinearAggregate(aggregate.getNode(),
                aggregate.zero, increment.to(DBSPClosureExpression.class), aggregate.postProcess,
                aggregate.emptySetResult, aggregate.semigroup);
        this.map(aggregate, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(LinearAggregate aggregate) {
        DBSPExpression map = this.transform(aggregate.map);
        LinearAggregate result = new LinearAggregate(aggregate.getNode(),
                map.to(DBSPClosureExpression.class),
                aggregate.postProcess, aggregate.emptySetResult);
        this.map(aggregate, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(MinMaxAggregate aggregate) {
        DBSPExpression aggregatedValue = this.transform(aggregate.aggregatedValue);
        this.parameterFullyUsed(aggregate.increment.parameters[0]);
        this.parameterFullyUsed(aggregate.increment.parameters[2]);
        DBSPClosureExpression increment = this.transform(aggregate.increment).to(DBSPClosureExpression.class);
        Utilities.enforce(aggregate.increment.getResultType().sameType(increment.getResultType()));
        MinMaxAggregate result = new MinMaxAggregate(aggregate.getNode(),
                aggregate.zero, increment,
                aggregate.emptySetResult, aggregate.semigroup,
                aggregatedValue.to(DBSPClosureExpression.class), aggregate.isMin);
        this.map(aggregate, result);
        return VisitDecision.STOP;
    }

    public DBSPClosureExpression rewriteClosure(DBSPClosureExpression closure) {
        IDBSPInnerNode result = this.apply(closure);
        return result.to(DBSPClosureExpression.class);
    }

    @Override
    public void startVisit(IDBSPInnerNode node) {
        this.resolver.reference.clear();
        this.resolver.apply(node);
        super.startVisit(node);
    }

    public FieldUseMap getUseMap(DBSPParameter param) {
        return this.fieldRemap.get(param);
    }
}
