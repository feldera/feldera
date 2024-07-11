package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.apache.commons.math3.util.Pair;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.ExplicitShuffle;
import org.dbsp.util.Shuffle;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Objects;

/** Discovers whether a closure is just a projection:
 * selects some fields from the input tuple.
 * A conservative approximation. */
public class Projection extends InnerVisitor {
    // TODO: resolve identifiers properly

    @Nullable
    public DBSPClosureExpression expression;
    /** Set to true if this is indeed a projection. */
    public boolean isProjection;
    /** Parameters of the enclosing closure. */
    public final List<String> parameters;

    /** If the description can be described as a shuffle,
     * this is it.
     * For a projection to be described as a shuffle,
     * it cannot contain constant fields, or nested fields.
     * (a.1, a.3) is simple
     * (2, a.3, a.3.2) is not simple.
     * Only makes sense for functions with a single parameter. */
    @Nullable
    ExplicitShuffle shuffle;

    /** A list indexed by output number.  For each output, the list
     * contains the input parameter index, and the field index, if the analyzed
     * function is a projection. */
    @Nullable
    List<Pair<Integer, Integer>> outputs;

    VisitDecision notProjection() {
        this.shuffle = null;
        this.outputs = null;
        this.isProjection = false;
        return VisitDecision.STOP;
    }

    int currentParameterIndex = -1;

    public Projection(IErrorReporter reporter) {
        super(reporter);
        this.parameters = new ArrayList<>();
        this.isProjection = true;
        this.outputs = new ArrayList<>();
        this.shuffle = new ExplicitShuffle();
    }

    @Override
    public VisitDecision preorder(DBSPExpression expression) {
        // Any other expression makes this not be a projection.
        return this.notProjection();
    }

    @Override
    public VisitDecision preorder(DBSPBlockExpression expression) {
        if (!expression.contents.isEmpty()) {
            // Too hard.  Give up.
            return this.notProjection();
        }
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPLetStatement statement) {
        return this.notProjection();
    }

    @Override
    public VisitDecision preorder(DBSPVariablePath path) {
        if (!this.parameters.contains(path.variable)) {
            return this.notProjection();
        }
        this.currentParameterIndex = this.parameters.indexOf(path.variable);
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPDerefExpression expression) {
        if (!expression.expression.is(DBSPVariablePath.class)) {
            this.shuffle = null;
            this.outputs = null;
        }
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPFieldExpression field) {
        if (!field.expression.is(DBSPDerefExpression.class)) {
            this.shuffle = null;
            return VisitDecision.CONTINUE;
        }
        if (this.shuffle != null)
            this.shuffle.add(field.fieldNo);
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(DBSPFieldExpression field) {
        if (this.outputs != null) {
            assert this.currentParameterIndex >= 0;
            this.outputs.add(Pair.create(this.currentParameterIndex, field.fieldNo));
        }
    }

    @Override
    public VisitDecision preorder(DBSPCloneExpression expression) {
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPUnwrapExpression expression) { return VisitDecision.CONTINUE; }

    @Override
    public VisitDecision preorder(DBSPTupleExpression expression) {
        return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPLiteral expression) {
        this.shuffle = null;
        this.outputs = null;
        return VisitDecision.CONTINUE;
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression expression) {
        if (!this.context.isEmpty()) {
            // We only allow closures in the outermost context.
            return this.notProjection();
        }
        this.expression = expression;
        if (expression.parameters.length == 0) {
            return this.notProjection();
        }
        for (DBSPParameter param: expression.parameters) {
            this.parameters.add(param.asVariable().variable);
        }
        return VisitDecision.CONTINUE;
    }

    public Shuffle getShuffle() {
        assert this.isProjection;
        return Objects.requireNonNull(this.shuffle);
    }

    /** Compose this projection with a constant expression.
     * @param before Constant expression.
     * @return A new constant expression. */
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
            DBSPExpression apply = this.expression.call(row.borrow());
            DBSPExpression simplified = inner.apply(apply).to(DBSPExpression.class);
            if (elementType == null)
                elementType = simplified.getType();
            result.put(simplified, entry.getValue());
        }
        return new DBSPZSetLiteral(result, Objects.requireNonNull(elementType));
    }

    public boolean isShuffle() {
        assert this.parameters.size() == 1;
        return this.shuffle != null;
    }

    public List<Pair<Integer, Integer>> getOutputs() {
        return Objects.requireNonNull(this.outputs);
    }
}
