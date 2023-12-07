package org.dbsp.sqlCompiler.ir;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.BetaReduction;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.path.DBSPSimplePathSegment;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeSemigroup;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


/**
 * Description of an aggregate.
 * In general an aggregate performs multiple simple aggregates simultaneously.
 */
public class DBSPAggregate extends DBSPNode implements IDBSPInnerNode {
    public final DBSPVariablePath rowVar;
    public final Implementation[] components;
    public final boolean isWindowAggregate;

    public DBSPAggregate(CalciteObject node, DBSPVariablePath rowVar,
                         Implementation[] components, boolean isWindowAggregate) {
        super(node);
        this.rowVar = rowVar;
        this.components = components;
        this.isWindowAggregate = isWindowAggregate;
    }

    public DBSPTypeTuple defaultZeroType() {
        return this.defaultZero().getType().to(DBSPTypeTuple.class);
    }

    public DBSPExpression defaultZero() {
        return new DBSPTupleExpression(Linq.map(this.components, c -> c.emptySetResult, DBSPExpression.class));
    }

    public DBSPExpression getZero() {
        return new DBSPTupleExpression(Linq.map(this.components, c -> c.zero, DBSPExpression.class));
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        for (Implementation impl: this.components) {
            impl.accept(visitor);
        }
        visitor.pop(this);
        visitor.postorder(this);
    }

    /**
     * If a type is a tuple, return the list of components.
     * Otherwise, return the type itself.
     */
    List<DBSPType> flatten(DBSPType type) {
        DBSPTypeTupleBase tuple = type.as(DBSPTypeTupleBase.class);
        if (tuple == null)
            return Linq.list(type);
        return Linq.list(tuple.tupFields);
    }

    public DBSPClosureExpression getIncrement() {
        // Here we rely on the fact that all increment functions have the following signature:
        // closure0 = (a0: AType0, v: Row, w: Weight) -> AType0 { body0 }
        // closure1 = (a1: AType1, v: Row, w: Weight) -> AType1 { body1 }
        // And all accumulators have distinct names.
        // We generate the following closure:
        // (a: (AType0, AType1), v: Row, W: Weight) -> (AType0, AType1) {
        //    let tmp0 = closure0(a.0, v, row);
        //    let tmp1 = closure1(a.1, v, row);
        //    (tmp0, tmp1)
        // }
        // If an accumulator type is a tuple, we flatten it into the component fields,
        // as follows:
        // Let's say the signature originally is:
        // (a: ((AType0, AType1), AType2), v: Row, W: Weight) -> ((AType0, AType1), AType2) {
        //    let tmp0 = closure0(a.0, v, row);
        //    let tmp1 = closure1(a.1, v, row);
        //    (tmp0, tmp1)
        // }
        // Then the flattened version is:
        // (a: (AType0, AType1, AType2), v: Row, W: Weight) -> (AType0, AType1, AType2) {
        //    let tmp0 = closure0((a.0, a.1), v, row);
        //    let tmp1 = closure1(a.2, v, row);
        //    (tmp0, tmp1)
        // }
        if (this.components.length == 0)
            throw new InternalCompilerError("Empty aggregation components", this);
        DBSPClosureExpression[] closures = Linq.map(this.components, c -> c.increment, DBSPClosureExpression.class);
        for (DBSPClosureExpression expr: closures) {
            if (expr.parameters.length != 3)
                throw new InternalCompilerError("Expected exactly 3 parameters for increment closure", expr);
        }
        DBSPType[] accumulatorTypes = Linq.map(closures, c -> c.parameters[0].getType(), DBSPType.class);
        List<DBSPType> flatAccumTypes = Linq.flatMap(accumulatorTypes, this::flatten);
        DBSPVariablePath accumParam = new DBSPTypeTuple(flatAccumTypes).var("a");
        DBSPParameter accumulator = accumParam.asParameter();
        DBSPParameter row = closures[0].parameters[1];
        DBSPParameter weight = closures[0].parameters[2];

        List<DBSPStatement> body = new ArrayList<>();
        List<DBSPExpression> tmpVars = new ArrayList<>();
        int start = 0;
        for (int i = 0; i < closures.length; i++) {
            DBSPClosureExpression closure = closures[i];
            String tmp = "tmp" + i;
            // Must package together several accumulator fields if the original type was a tuple
            DBSPType accumulatorType = accumulatorTypes[i];
            DBSPTypeTupleBase tuple = accumulatorType.as(DBSPTypeTupleBase.class);
            DBSPExpression accumulatorArgs = accumParam.field(start);
            if (tuple != null) {
                DBSPExpression[] accumArgFields = new DBSPExpression[tuple.size()];
                for (int j = 0; j < tuple.size(); j++, start++) {
                    accumArgFields[j] = accumParam.field(start);
                }
                accumulatorArgs = new DBSPRawTupleExpression(accumArgFields);
            } else {
                start++;
            }
            DBSPExpression init = closure.call(
                    accumulatorArgs, row.asVariable(), weight.asVariable());
            DBSPLetStatement stat = new DBSPLetStatement(tmp, init);
            DBSPVariablePath tmpI = new DBSPVariablePath(tmp, init.getType());
            tmpVars.addAll(DBSPTypeTupleBase.flatten(tmpI));
            body.add(stat);
        }

        DBSPExpression last = new DBSPTupleExpression(tmpVars, false);
        DBSPBlockExpression block = new DBSPBlockExpression(body, last);
        return new DBSPClosureExpression(block, accumulator, row, weight);
    }

    /**
     * Get a function that performs post-processing for all aggregates.
     * Let's say the closures are:
     * closure0 = move |x: i64, | -> i64 { x }
     * closure1 = move |x: Option[i64], | -> Option[i64] { x }
     * The combined result will have this signature:
     * move | p: (i64, Option[i64]) | -> (i64, Option[i64]) {
     *     let tmp0: i64 = (move |x: i64, | -> i64 { x })(p0.0);
     *     let tmp1: Option<i32> = (move |x: Option<i32>, | -> Option<i32> { x })(p0.1);
     *     Tuple2::new(tmp0, tmp1)
     * }
     * Similarly to getIncrement above, the type of the function arguments
     * is flattened if it is a nested tuple.
     */
    public DBSPClosureExpression getPostprocessing() {
        DBSPClosureExpression[] closures = Linq.map(
                this.components, Implementation::getPostprocessing, DBSPClosureExpression.class);
        DBSPParameter[][] allParams = Linq.map(closures, c -> c.parameters, DBSPParameter[].class);
        int paramCount = -1;
        for (DBSPParameter[] params: allParams) {
            if (paramCount == -1)
                paramCount = params.length;
            else if (paramCount != params.length)
                throw new InternalCompilerError("Closures cannot be combined", this);
        }

        DBSPVariablePath[] resultParams = new DBSPVariablePath[paramCount];
        for (int i = 0; i < paramCount; i++) {
            int finalI = i;
            DBSPParameter[] first = Linq.map(allParams, p -> p[finalI], DBSPParameter.class);
            String name = "p" + i;
            DBSPVariablePath pi = new DBSPVariablePath(
                    name, new DBSPTypeTuple(Linq.flatMap(first, p -> this.flatten(p.type))));
            resultParams[i] = pi;
        }

        List<DBSPStatement> body = new ArrayList<>();
        List<DBSPExpression> tmps = new ArrayList<>();
        int start = 0;
        for (int i = 0; i < closures.length; i++) {
            DBSPClosureExpression closure = closures[i];
            String tmp = "tmp" + i;
            DBSPExpression[] args = new DBSPExpression[closure.parameters.length];
            for (int j = 0; j < args.length; j++) {
                DBSPType paramType = closure.parameters[j].getType();
                if (paramType.is(DBSPTypeRef.class))
                    paramType = paramType.to(DBSPTypeRef.class).type;
                if (paramType.is(DBSPTypeTupleBase.class)) {
                    DBSPTypeTupleBase tuple = paramType.to(DBSPTypeTupleBase.class);
                    DBSPExpression[] argFields = new DBSPExpression[tuple.size()];
                    for (int k = 0; k < tuple.size(); k++) {
                        argFields[k] = resultParams[j].field(start);
                        start++;
                    }
                    args[j] = tuple.makeTuple(argFields);
                } else {
                    args[j] = resultParams[j].field(start);
                    start++;
                }
            }
            DBSPExpression init = closure.call(args);
            DBSPLetStatement stat = new DBSPLetStatement(tmp, init);
            tmps.add(new DBSPVariablePath(tmp, init.getType()));
            body.add(stat);
        }

        DBSPExpression last = new DBSPTupleExpression(tmps, false);
        DBSPBlockExpression block = new DBSPBlockExpression(body, last);
        DBSPParameter[] params = Linq.map(resultParams, DBSPVariablePath::asParameter, DBSPParameter.class);
        return new DBSPClosureExpression(block, params);
    }

    /**
     * Given an DBSPAggregate where all Implementation objects have a linearFunction
     * component, combine these linear functions into a single one.
     * The linear functions have the signature:
     * '|row| value', where 'row' is always the same variable.
     * The result function will have the signature:
     * |row| (value0, value1, ...).
     */
    public DBSPClosureExpression combineLinear() {
        DBSPClosureExpression[] closures = Linq.map(this.components, c -> c.linearFunction, DBSPClosureExpression.class);
        for (DBSPClosureExpression expr: closures) {
            if (expr.parameters.length != 1)
                throw new InternalCompilerError("Expected exactly 1 parameter for linear closure", expr);
        }
        DBSPParameter row = closures[0].parameters[0];
        DBSPExpression[] bodies = Linq.map(closures, c -> c.body, DBSPExpression.class);
        DBSPTupleExpression tuple = new DBSPTupleExpression(bodies);
        return tuple.closure(row);
    }

    /**
     * An aggregate is compiled as functional fold operation,
     * described by a zero (initial value), an increment
     * function, and a postprocessing step that makes any necessary conversions.
     * For example, AVG has a zero of (0,0), an increment of (1, value),
     * and a postprocessing step of |a| a.1/a.0.
     * Notice that the DBSP `Fold` structure has a slightly different signature
     * for the increment.
     */
    public static class Implementation extends DBSPNode implements IDBSPInnerNode {
        /**
         * Zero of the fold function.
         */
        public final DBSPExpression zero;
        /**
         * A closure with signature |accumulator, value, weight| -> accumulator
         */
        public final DBSPClosureExpression increment;
        /**
         * Function that may post-process the accumulator to produce the final result.
         */
        @Nullable
        public final DBSPClosureExpression postProcess;
        /**
         * Result produced for an empty set (DBSP produces no result in this case).
         */
        public final DBSPExpression emptySetResult;
        /**
         * Name of the Type that implements the semigroup for this operation.
         */
        public final DBSPType semigroup;
        /**
         * If non-null this is a function with the signature
         * |value| accumulator, where 'accumulator' implements
         * GroupValue.  The function is applied to each row, the result
         * is weighted by the row weight, and the results are added.
         */
        @Nullable
        public final DBSPClosureExpression linearFunction;

        public Implementation(
                CalciteObject origin,
                DBSPExpression zero,
                DBSPClosureExpression increment,
                @Nullable
                DBSPClosureExpression postProcess,
                DBSPExpression emptySetResult,
                DBSPType semigroup,
                @Nullable
                DBSPClosureExpression linearFunction) {
            super(origin);
            this.zero = zero;
            this.increment = increment;
            this.postProcess = postProcess;
            this.emptySetResult = emptySetResult;
            this.semigroup = semigroup;
            this.linearFunction = linearFunction;
        }

        public Implementation(
                CalciteObject operator,
                DBSPExpression zero,
                DBSPClosureExpression increment,
                DBSPExpression emptySetResult,
                DBSPType semigroup,
                @Nullable
                DBSPClosureExpression linearFunction) {
            this(operator, zero, increment, null, emptySetResult, semigroup, linearFunction);
        }

        public void validate() {
            // These validation rules actually don't apply for window-based aggregates.
            DBSPType emptyResultType = this.emptySetResult.getType();
            if (this.postProcess != null) {
                DBSPType postProcessType = this.postProcess.getResultType();
                if (!emptyResultType.sameType(postProcessType))
                    throw new InternalCompilerError("Post-process result type " + postProcessType +
                            " different from empty set type " + emptyResultType, this);
            } else {
                DBSPType incrementResultType = this.increment.getResultType();
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
            this.semigroup.accept(visitor);
            this.zero.accept(visitor);
            this.increment.accept(visitor);
            if (this.postProcess != null)
                this.postProcess.accept(visitor);
            this.emptySetResult.accept(visitor);
            visitor.pop(this);
            visitor.postorder(this);
        }

        public DBSPClosureExpression getPostprocessing() {
            if (this.postProcess != null)
                return this.postProcess;
            // If it is not set return the identity function
            DBSPVariablePath var = new DBSPVariablePath("x", Objects.requireNonNull(this.increment.getResultType()));
            return var.closure(var.asParameter());
        }

        @Override
        public boolean sameFields(IDBSPNode other) {
            Implementation o = other.as(Implementation.class);
            if (o == null)
                return false;
            return this.zero == o.zero &&
                this.increment == o.increment &&
                this.postProcess == o.postProcess &&
                this.emptySetResult == o.emptySetResult &&
                this.semigroup == o.semigroup;
        }

        public DBSPExpression asFold(boolean compact) {
            DBSPType[] typeArgs;
            if (compact) {
                typeArgs = new DBSPType[0];
            } else {
                typeArgs = new DBSPType[4];
                typeArgs[0] = DBSPTypeAny.getDefault();
                typeArgs[1] = this.semigroup;
                typeArgs[2] = DBSPTypeAny.getDefault();
                typeArgs[3] = DBSPTypeAny.getDefault();
            }

            DBSPExpression constructor =
                    new DBSPPath(
                            new DBSPSimplePathSegment("Fold", typeArgs),
                            new DBSPSimplePathSegment("with_output"))
                            .toExpression();
            return constructor.call(this.zero, this.increment, this.postProcess);
        }

        public DBSPExpression asFold() {
            return this.asFold(false);
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
            if (this.linearFunction != null) {
                builder.newline()
                        .append("linearFunction=")
                        .append(this.linearFunction);
            }
            builder.newline().decrease().append("]");
            return builder;
        }
    }

    public boolean isLinear() {
        return Linq.all(this.components, c -> c.linearFunction != null);
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPAggregate o = other.as(DBSPAggregate.class);
        if (o == null)
            return false;
        return this.rowVar == o.rowVar &&
                Linq.same(this.components, o.components);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append("Aggregate:").increase();
        for (DBSPAggregate.Implementation impl : this.components) {
            builder.append(impl);
        }
        return builder.decrease();
    }

    /**
     * Combines multiple DBSPAggregate.Implementation objects into one.
     */
    public DBSPAggregate.Implementation combine(IErrorReporter reporter) {
        int parts = this.components.length;
        DBSPExpression[] zeros = new DBSPExpression[parts];
        DBSPExpression[] increments = new DBSPExpression[parts];
        DBSPExpression[] posts = new DBSPExpression[parts];
        DBSPExpression[] emptySetResults = new DBSPExpression[parts];

        DBSPType[] accumulatorTypes = new DBSPType[parts];
        DBSPType[] semigroups = new DBSPType[parts];
        DBSPType weightType = null;
        for (int i = 0; i < parts; i++) {
            DBSPAggregate.Implementation implementation = this.components[i];
            DBSPType incType = implementation.increment.getResultType();
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
                        + weightType + " and " + lastParamType, this);
            accumulatorTypes[i] = Objects.requireNonNull(incType);
            semigroups[i] = implementation.semigroup;
            posts[i] = implementation.getPostprocessing();
            emptySetResults[i] = implementation.emptySetResult;
        }

        DBSPTypeTuple accumulatorType = new DBSPTypeTuple(accumulatorTypes);
        DBSPVariablePath accumulator = accumulatorType.ref(true).var("a");
        DBSPVariablePath postAccumulator = accumulatorType.var("a");

        BetaReduction reducer = new BetaReduction(reporter);
        DBSPVariablePath weightVar = new DBSPVariablePath("w", Objects.requireNonNull(weightType));
        for (int i = 0; i < parts; i++) {
            DBSPExpression accumulatorField = accumulator.field(i);
            DBSPExpression expr = increments[i].call(
                    accumulatorField, this.rowVar, weightVar);
            increments[i] = Objects.requireNonNull(reducer.apply(expr)).to(DBSPExpression.class);
            DBSPExpression postAccumulatorField = postAccumulator.field(i);
            expr = posts[i].call(postAccumulatorField);
            posts[i] = Objects.requireNonNull(reducer.apply(expr)).to(DBSPExpression.class);
        }
        DBSPAssignmentExpression accumulatorBody = new DBSPAssignmentExpression(
                accumulator.deref(), new DBSPTupleExpression(increments));
        DBSPClosureExpression accumFunction = accumulatorBody.closure(
                accumulator.asParameter(), this.rowVar.asParameter(),
                weightVar.asParameter());
        DBSPClosureExpression postClosure = new DBSPTupleExpression(posts).closure(postAccumulator.asParameter());
        DBSPType semigroup = new DBSPTypeSemigroup(semigroups, accumulatorTypes);
        return new Implementation(this.getNode(), new DBSPTupleExpression(zeros),
                accumFunction, postClosure, new DBSPTupleExpression(emptySetResults), semigroup, null);
    }
}
