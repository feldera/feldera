package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.circuit.IDBSPInnerNode;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Discovers whether a closure is just a projection:
 * selects some fields from the input tuple.
 * A conservative approximation.
 */
public class Projection extends InnerVisitor {
    @Nullable
    public DBSPClosureExpression expression;
    /**
     * Set to true if this is indeed a projection.
     */
    public boolean isProjection;
    /**
     * Parameters of the enclosing closure.
     */
    public final Set<String> parameters;

    public Projection(IErrorReporter reporter) {
        super(reporter, true);
        this.parameters = new HashSet<>();
        this.isProjection = true;
    }

    @Override
    public boolean preorder(DBSPExpression expression) {
        // Any other expression makes this not be a projection.
        this.isProjection = false;
        return false;
    }

    @Override
    public boolean preorder(DBSPBlockExpression expression) {
        if (!expression.contents.isEmpty()) {
            // Too hard.  Give up.
            this.isProjection = false;
            return false;
        }
        return true;
    }

    @Override
    public boolean preorder(DBSPVariablePath path) {
        if (!this.parameters.contains(path.variable)) {
            this.isProjection = false;
            return false;
        }
        return true;
    }

    @Override
    public boolean preorder(DBSPFieldExpression field) {
        if (!field.expression.is(DBSPVariablePath.class)) {
            this.isProjection = false;
            return false;
        }
        return true;
    }

    @Override
    public boolean preorder(DBSPTupleExpression expression) {
        // Nothing
        return true;
    }

    @Override
    public boolean preorder(DBSPClosureExpression expression) {
        if (!this.context.isEmpty()) {
            // We only allow closures in the outermost context.
            this.isProjection = false;
            return false;
        }
        this.expression = expression;
        if (expression.parameters.length == 0) {
            this.isProjection = false;
            return false;
        }
        for (DBSPParameter param: expression.parameters) {
            this.parameters.add(param.asVariableReference().variable);
        }
        return true;
    }

    /**
     * Compose this projection by applying it after another
     * closure expression.  This closure must have exactly 1
     * parameter, while the before one can have multiple ones.
     * @param before Closure to compose.
     */
    public DBSPClosureExpression applyAfter(DBSPClosureExpression before) {
        Objects.requireNonNull(this.expression);
        if (this.expression.parameters.length != 1)
            throw new RuntimeException();
        DBSPExpression apply = new DBSPApplyExpression(this.expression, before.body);
        DBSPClosureExpression result = new DBSPClosureExpression(apply, before.parameters);
        BetaReduction reduction = new BetaReduction(this.errorReporter);
        Simplify simplify = new Simplify(this.errorReporter);
        IDBSPInnerNode reduced = reduction.apply(result);
        IDBSPInnerNode simplified = simplify.apply(reduced);
        return simplified.to(DBSPClosureExpression.class);
    }
}

