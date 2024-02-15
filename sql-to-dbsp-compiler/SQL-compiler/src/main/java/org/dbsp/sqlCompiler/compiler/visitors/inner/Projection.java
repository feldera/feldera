package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Discovers whether a closure is just a projection:
 * selects some fields from the input tuple.
 * A conservative approximation.
 */
public class Projection extends InnerVisitor {
    /**
     * Description of a projection: list of projected fields.
     */
    public static class Description {
        public final List<Integer> fields;

        Description() {
            this.fields = new ArrayList<>();
        }

        public void add(int field) {
            this.fields.add(field);
        }
    }

    @Nullable
    public DBSPClosureExpression expression;
    /**
     * Set to true if this is indeed a projection.
     */
    public boolean isProjection;
    /**
     * Set to true if this is a simple expression.
     * In addition to a projection it may also contain constant fields.
     */
    public boolean isSimple;
    /**
     * Parameters of the enclosing closure.
     */
    public final Set<String> parameters;

    final Description description;

    public Projection(IErrorReporter reporter) {
        super(reporter);
        this.parameters = new HashSet<>();
        this.isProjection = true;
        this.isSimple = true;
        this.description = new Description();
    }

    @Override
    public VisitDecision preorder(DBSPExpression expression) {
        // Any other expression makes this not be a projection.
        this.isProjection = false;
        this.isSimple = false;
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBlockExpression expression) {
        if (!expression.contents.isEmpty()) {
            // Too hard.  Give up.
            this.isProjection = false;
            this.isSimple = false;
            return VisitDecision.STOP;
        }
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPVariablePath path) {
        if (!this.parameters.contains(path.variable)) {
            this.isProjection = false;
            this.isSimple = false;
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFieldExpression field) {
        if (!field.expression.is(DBSPVariablePath.class)) {
            this.isProjection = false;
            this.isSimple = false;
            return VisitDecision.STOP;
        }
        this.description.add(field.fieldNo);
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPCloneExpression expression) {
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPTupleExpression expression) {
        return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPLiteral expression) {
        // Not a projection, but may still be simple
        this.isProjection = false;
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression expression) {
        if (!this.context.isEmpty()) {
            // We only allow closures in the outermost context.
            this.isProjection = false;
            this.isSimple = false;
            return VisitDecision.STOP;
        }
        this.expression = expression;
        if (expression.parameters.length == 0) {
            this.isProjection = false;
            return VisitDecision.STOP;
        }
        for (DBSPParameter param: expression.parameters) {
            this.parameters.add(param.asVariable().variable);
        }
        return VisitDecision.CONTINUE;
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
            throw new InternalCompilerError("Expected closure with 1 parameter", this.expression);
        DBSPExpression apply = new DBSPApplyExpression(this.expression, before.body);
        DBSPClosureExpression result = new DBSPClosureExpression(apply, before.parameters);
        BetaReduction reduction = new BetaReduction(this.errorReporter);
        Simplify simplify = new Simplify(this.errorReporter);
        IDBSPInnerNode reduced = reduction.apply(result);
        IDBSPInnerNode simplified = simplify.apply(reduced);
        return simplified.to(DBSPClosureExpression.class);
    }

    public Description getDescription() {
        if (!this.isProjection)
            throw new InternalCompilerError("This is not a projection",
                    this.expression != null ? this.expression : DBSPTypeAny.getDefault());
        return this.description;
    }

    /**
     * Compose this projection with a constant expression.
     * @param before Constant expression.
     * @return A new constant expression.
     */
    public DBSPExpression applyAfter(DBSPZSetLiteral before) {
        Objects.requireNonNull(this.expression);

        Map<DBSPExpression, Long> result = new HashMap<>();
        InnerPasses inner = new InnerPasses(
                new BetaReduction(this.errorReporter),
                new Simplify(this.errorReporter)
        );

        DBSPType elementType = null;
        for (Map.Entry<DBSPExpression, Long> entry: before.data.entrySet()) {
            DBSPExpression row = entry.getKey();
            DBSPExpression apply = new DBSPApplyExpression(this.expression, row);
            DBSPExpression simplified = inner.apply(apply).to(DBSPExpression.class);
            if (elementType == null)
                elementType = simplified.getType();
            result.put(simplified, entry.getValue());
        }
        return new DBSPZSetLiteral(result, Objects.requireNonNull(elementType));
    }
}

