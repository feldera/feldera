package org.dbsp.sqlCompiler.ir.aggregate;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.BetaReduction;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPAssignmentExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeSemigroup;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A non-linear aggregate is compiled as functional fold operation,
 * described by
 * - a zero (initial value),
 * - an increment function, and
 * - a postprocessing step that makes any necessary conversions.
 * For example, (a non-linear version of) AVG has
 * - a zero of (0,0),
 * - an increment of (1, value), and
 * - a postprocessing step of |a| a.1/a.0.
 * Notice that the DBSP `Fold` structure has a slightly different signature
 * for the increment. */
public class NonLinearAggregate extends IAggregate {
    /** Zero of the fold function. */
    public final DBSPExpression zero;
    /** A closure with signature |&mut accumulator, value, weight|.
     * The closure may return a result, or may just mutate the accumulator. */
    public final DBSPClosureExpression increment;
    /** Function that may post-process the accumulator to produce the final result. */
    @Nullable
    public final DBSPClosureExpression postProcess;
    /** Result produced for an empty set (DBSP produces no result in this case). */
    public final DBSPExpression emptySetResult;
    /** The type that implements the semigroup for this operation. */
    public final DBSPTypeUser semigroup;

    public NonLinearAggregate(
            CalciteObject node,
            DBSPExpression zero,
            DBSPClosureExpression increment,
            @Nullable
            DBSPClosureExpression postProcess,
            DBSPExpression emptySetResult,
            DBSPTypeUser semigroup) {
        super(node, emptySetResult.getType());
        this.zero = zero;
        this.increment = increment;
        Utilities.enforce(increment.parameters.length == 3);
        this.postProcess = postProcess;
        this.emptySetResult = emptySetResult;
        this.semigroup = semigroup;
    }

    public NonLinearAggregate(
            CalciteObject node,
            DBSPExpression zero,
            DBSPClosureExpression increment,
            DBSPExpression emptySetResult,
            DBSPTypeUser semigroup) {
        this(node, zero, increment, null, emptySetResult, semigroup);
    }

    /** Result produced for an empty set. */
    public DBSPExpression getEmptySetResult() {
        return this.emptySetResult;
    }

    public DBSPType getIncrementType() {
        return this.increment.parameters[0].getType();
    }

    @Override
    public boolean compatible(IAggregate other) {
        return other.is(NonLinearAggregate.class) &&
                !other.is(MinMaxAggregate.class);
    }

    @Override
    public List<DBSPParameter> getRowVariableReferences() {
        return Linq.list(this.increment.parameters[1]);
    }

    @Override
    public void validate() {
        // These validation rules actually don't apply for window-based aggregates.
        DBSPType emptyResultType = this.emptySetResult.getType();
        if (this.postProcess != null) {
            DBSPType postProcessType = this.postProcess.getResultType();
            if (!emptyResultType.sameType(postProcessType))
                throw new InternalCompilerError("Post-process result type " + postProcessType +
                        " different from empty set type " + emptyResultType, this);
        } else {
            DBSPType incrementResultType = this.getIncrementType();
            if (!emptyResultType.sameType(incrementResultType)) {
                throw new InternalCompilerError("Increment result type " + incrementResultType +
                        " different from empty set type " + emptyResultType, this);
            }
        }
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("semigroup");
        this.semigroup.accept(visitor);
        visitor.property("zero");
        this.zero.accept(visitor);
        visitor.property("increment");
        this.increment.accept(visitor);
        if (this.postProcess != null) {
            visitor.property("postProcess");
            this.postProcess.accept(visitor);
        }
        visitor.property("emptySetResult");
        this.emptySetResult.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    public DBSPClosureExpression getPostprocessing() {
        if (this.postProcess != null)
            return this.postProcess;
        // If it is not set return the identity function
        DBSPVariablePath var = this.getIncrementType().var();
        return var.closure(var);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        NonLinearAggregate o = other.as(NonLinearAggregate.class);
        if (o == null)
            return false;
        return this.zero == o.zero &&
                this.increment == o.increment &&
                this.postProcess == o.postProcess &&
                this.emptySetResult == o.emptySetResult &&
                this.semigroup == o.semigroup;
    }

    public DBSPFold asFold() {
        return new DBSPFold(this.getNode(), this.semigroup,
                this.zero, this.increment, Objects.requireNonNull(this.postProcess));
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append("[").increase();
        builder.append("zero=")
                .append(this.zero)
                .newline()
                .append("increment=")
                .append(this.increment);
        if (this.postProcess != null) {
            builder.newline()
                    .append("postProcess=")
                    .append(this.postProcess);
        }
        builder.newline()
                .append("emptySetResult=")
                .append(this.emptySetResult)
                .newline()
                .append("semigroup=")
                .append(this.semigroup);
        builder.newline().decrease().append("]");
        return builder;
    }

    /** Combines multiple {@link NonLinearAggregate} objects into one.
     * Note: 'this' is not used in the result; it is only used for dynamic dispatch. */
    @Override
    public IAggregate combine(
            CalciteObject node, DBSPCompiler compiler,
            DBSPVariablePath rowVar, List<IAggregate> components) {
        int parts = components.size();
        DBSPExpression[] zeros = new DBSPExpression[parts];
        DBSPClosureExpression[] increments = new DBSPClosureExpression[parts];
        DBSPExpression[] posts = new DBSPExpression[parts];
        DBSPExpression[] emptySetResults = new DBSPExpression[parts];

        DBSPType[] accumulatorTypes = new DBSPType[parts];
        DBSPType[] semigroups = new DBSPType[parts];
        DBSPType weightType = null;
        for (int i = 0; i < parts; i++) {
            NonLinearAggregate implementation = components.get(i).to(NonLinearAggregate.class);
            DBSPType incType = implementation.getIncrementType();
            zeros[i] = implementation.zero;
            increments[i] = implementation.increment;
            if (implementation.increment.parameters.length != 3)
                throw new InternalCompilerError("Expected increment function to have 3 parameters",
                        implementation.increment);
            DBSPType lastParamType = implementation.increment.parameters[2].getType();
            // Extract weight type from increment function signature.
            // It may not be DBSPTypeWeight anymore.
            if (weightType == null)
                weightType = lastParamType;
            else
            if (!weightType.sameType(lastParamType))
                throw new InternalCompilerError("Not all increment functions have the same type "
                        + weightType + " and " + lastParamType, node);
            accumulatorTypes[i] = Objects.requireNonNull(incType);
            semigroups[i] = implementation.semigroup;
            posts[i] = implementation.getPostprocessing();
            emptySetResults[i] = implementation.emptySetResult;
        }

        DBSPTypeTuple accumulatorType = new DBSPTypeTuple(accumulatorTypes);
        DBSPVariablePath accumulator = accumulatorType.ref(true).var();
        DBSPVariablePath postAccumulator = accumulatorType.var();

        List<DBSPStatement> block = new ArrayList<>();
        DBSPVariablePath weightVar = new DBSPVariablePath(Objects.requireNonNull(weightType));
        for (int i = 0; i < parts; i++) {
            DBSPExpression accumulatorField = accumulator.deref().field(i);
            DBSPExpression expr = increments[i].call(accumulatorField, rowVar, weightVar);
            BetaReduction reducer = new BetaReduction(compiler);
            expr = reducer.reduce(expr);
            // Generate either increment(&a.i...); or *a.i = increment(&a.i...)
            // depending on the type of the result returned by the increment function
            if (increments[i].getResultType().is(DBSPTypeVoid.class))
                block.add(new DBSPExpressionStatement(expr));
            else
                block.add(new DBSPExpressionStatement(
                        new DBSPAssignmentExpression(accumulatorField, expr)));
            DBSPExpression postAccumulatorField = postAccumulator.field(i).applyCloneIfNeeded();
            expr = posts[i].call(postAccumulatorField);
            posts[i] = reducer.reduce(expr);
        }
        DBSPExpression accumulatorBody = new DBSPBlockExpression(block, null);
        DBSPClosureExpression accumFunction = accumulatorBody.closure(
                accumulator, rowVar,
                weightVar);
        DBSPClosureExpression postClosure = new DBSPTupleExpression(posts).closure(postAccumulator);
        DBSPTypeUser semigroup = new DBSPTypeSemigroup(accumulatorTypes, semigroups);
        return new NonLinearAggregate(node, new DBSPTupleExpression(zeros),
                accumFunction, postClosure, new DBSPTupleExpression(emptySetResults), semigroup);
    }

    @Override
    public boolean isLinear() {
        return false;
    }

    public boolean equivalent(EquivalenceContext context, NonLinearAggregate other) {
        return context.equivalent(this.zero, other.zero) &&
                context.equivalent(this.increment, other.increment) &&
                context.equivalent(this.postProcess, other.postProcess) &&
                context.equivalent(this.emptySetResult, other.emptySetResult) &&
                this.semigroup.sameType(other.semigroup);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new NonLinearAggregate(this.getNode(),
                this.zero.deepCopy(),
                this.increment.deepCopy().to(DBSPClosureExpression.class),
                this.postProcess != null ? this.postProcess.deepCopy().to(DBSPClosureExpression.class) : null,
                this.emptySetResult.deepCopy(),
                this.semigroup);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        return false;
    }

    @SuppressWarnings("unused")
    public static NonLinearAggregate fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPExpression zero = fromJsonInner(node, "zero", decoder, DBSPExpression.class);
        DBSPClosureExpression increment = fromJsonInner(node, "increment", decoder, DBSPClosureExpression.class);
        DBSPExpression emptySetResult = fromJsonInner(node, "emptySetResult", decoder, DBSPExpression.class);
        DBSPTypeUser semigroup = fromJsonInner(node, "semigroup", decoder, DBSPTypeUser.class);
        DBSPClosureExpression postProcess = fromJsonInner(node, "postProcess", decoder, DBSPClosureExpression.class);
        return new NonLinearAggregate(CalciteObject.EMPTY, zero, increment, postProcess, emptySetResult, semigroup);
    }
}
