/*
 * Copyright 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.dbsp.sqlCompiler.compiler.frontend;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlAbstractGroupFunction;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlBasicAggFunction;
import org.apache.calcite.sql.fun.SqlBitOpAggFunction;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlMinMaxAggFunction;
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConditionalAggregateExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVecLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeVec;
import org.dbsp.util.ICastable;
import org.dbsp.util.Linq;
import org.dbsp.util.NameGen;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.SEMIGROUP;

/**
 * Compiles SQL aggregate functions.
 */
public class AggregateCompiler implements ICompilerComponent {
    public final DBSPCompiler compiler;
    /** Type of result expected. */
    public final DBSPType resultType;
    /** Almost all aggregates may return nullable results, even if Calcite pretends it's not true. */
    public final DBSPType nullableResultType;
    // Deposit compilation result here
    @Nullable
    private DBSPAggregate.Implementation foldingFunction;
    
    /** Expression that stands for the whole input row in the input zset. */
    private final DBSPVariablePath v;
    private final boolean isDistinct;
    private final SqlAggFunction aggFunction;
    private final int filterArgument;
    // null only for COUNT(*)
    @Nullable
    private final DBSPExpression aggArgument;
    private final NameGen generator;
    private final RelNode aggregateNode;
    private final ImmutableBitSet groups;
    private final AggregateCall call;

    public AggregateCompiler(
            RelNode node,
            DBSPCompiler compiler,
            AggregateCall call, DBSPType resultType,
            DBSPVariablePath v,
            ImmutableBitSet groups) {
        this.aggregateNode = node;
        this.call = call;
        this.groups = groups;
        this.compiler = compiler;
        this.resultType = resultType;
        this.nullableResultType = resultType.setMayBeNull(true);
        this.foldingFunction = null;
        this.v = v;
        this.isDistinct = call.isDistinct();
        this.aggFunction = call.getAggregation();
        this.generator = new NameGen("a");
        this.filterArgument = call.filterArg;
        List<Integer> argList = call.getArgList();
        if (argList.isEmpty()) {
            this.aggArgument = null;
        } else if (argList.size() == 1) {
            int fieldNumber = call.getArgList().get(0);
            this.aggArgument = this.v.deref().field(fieldNumber);
        } else {
            List<DBSPExpression> fields = Linq.map(call.getArgList(),
                    a -> this.v.deref().field(a).applyCloneIfNeeded());
            this.aggArgument = new DBSPTupleExpression(fields, false);
        }
    }

    boolean isWindowAggregate() {
        return !(this.aggregateNode instanceof LogicalAggregate);
    }

    public String genAccumulatorName() {
        return this.generator.nextName();
    }

    <T> boolean process(SqlAggFunction function, Class<T> clazz, Consumer<T> method) {
        T value = ICastable.as(function, clazz);
        if (value != null) {
            method.accept(value);
            return true;
        }
        return false;
    }

    /**
     * Given the body of a closure, make a closure with arguments accumulator, row, weight
     */
    DBSPClosureExpression makeRowClosure(DBSPExpression body, DBSPVariablePath accumulator) {
        return body.closure(
                accumulator.asParameter(), this.v.asParameter(),
                this.compiler.weightVar.asParameter());
    }

    @Nullable
    DBSPExpression filterArgument() {
        if (this.filterArgument < 0)
            return null;
        return this.v.deref().field(this.filterArgument);
    }

    void processBitOp(SqlBitOpAggFunction function) {
        CalciteObject node = CalciteObject.create(function);
        DBSPExpression zero = DBSPLiteral.none(this.nullableResultType);
        DBSPExpression increment;
        DBSPExpression aggregatedValue = this.getAggregatedValue();
        DBSPVariablePath accumulator = this.nullableResultType.var(this.genAccumulatorName());

        DBSPOpcode opcode = switch (function.getKind()) {
            case BIT_OR -> DBSPOpcode.AGG_OR;
            case BIT_AND -> DBSPOpcode.AGG_AND;
            case BIT_XOR -> DBSPOpcode.AGG_XOR;
            default -> throw new UnimplementedException(node);
        };

        // TODO: some of these are linear
        increment = this.aggregateOperation(node, opcode,
                this.nullableResultType, accumulator, aggregatedValue, this.filterArgument());
        DBSPType semigroup = new DBSPTypeUser(CalciteObject.EMPTY, SEMIGROUP, "UnimplementedSemigroup",
                false, accumulator.getType());
        this.setFoldingFunction(new DBSPAggregate.Implementation(
                node, zero, this.makeRowClosure(increment, accumulator), zero, semigroup, null));
    }

    void processGrouping(SqlAbstractGroupFunction function) {
        CalciteObject node = CalciteObject.create(function);
        DBSPExpression zero = this.nullableResultType.to(IsNumericType.class).getZero();

        long result = 0;
        long mask = 1;
        List<Integer> args = new ArrayList<>(this.call.getArgList());
        Collections.reverse(args);
        for (int field: args) {
            if (!this.groups.get(field)) {
                result = result | mask;
            }
            mask <<= 1;
        }
        DBSPExpression increment = new DBSPI64Literal(result).cast(this.nullableResultType);
        DBSPVariablePath accumulator = this.nullableResultType.var(this.genAccumulatorName());
        DBSPType semigroup = new DBSPTypeUser(CalciteObject.EMPTY, SEMIGROUP, "UnimplementedSemigroup",
                false, accumulator.getType());
        this.setFoldingFunction(new DBSPAggregate.Implementation(
                node, zero, this.makeRowClosure(increment, accumulator), zero, semigroup, null));
    }

    void processCount(SqlCountAggFunction function) {
        // The result of 'count' can never be null.
        CalciteObject node = CalciteObject.create(function);
        DBSPExpression increment;
        DBSPExpression argument;
        DBSPExpression zero = this.resultType.to(IsNumericType.class).getZero();
        DBSPExpression one = this.resultType.to(DBSPTypeInteger.class).getOne();

        if (this.aggArgument == null) {
            // COUNT(*)
            argument = one;
        } else {
            DBSPExpression agg = this.getAggregatedValue();
            argument = ExpressionCompiler.makeIndicator(node, resultType, agg);
        }

        @Nullable
        DBSPClosureExpression linear = argument.closure(this.v.asParameter());
        DBSPVariablePath accumulator = this.resultType.var(this.genAccumulatorName());
        if (this.isDistinct) {
            linear = null;
            increment = this.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD,
                    this.resultType, accumulator, argument, this.filterArgument());
        } else {
            DBSPExpression weighted = new DBSPBinaryExpression(node,
                    this.resultType, DBSPOpcode.MUL_WEIGHT, argument, this.compiler.weightVar);
            increment = this.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD, this.resultType,
                    accumulator, weighted, this.filterArgument());
        }
        if (this.filterArgument >= 0)
            // no longer linear, since it returns 0 instead of nothing for an empty collection
            linear = null;
        DBSPType semigroup = new DBSPTypeUser(node, SEMIGROUP, "DefaultSemigroup", false, this.resultType);
        this.setFoldingFunction(new DBSPAggregate.Implementation(
                node, zero, this.makeRowClosure(increment, accumulator),
                zero, semigroup, linear));
    }

    void processArrayAgg(SqlBasicAggFunction function) {
        CalciteObject node = CalciteObject.create(function);
        SqlKind kind = function.getKind();
        assert kind == SqlKind.ARRAY_AGG;

        boolean ignoreNulls = this.call.ignoreNulls();
        boolean distinct = this.call.isDistinct();
        DBSPType elementType = this.resultType.to(DBSPTypeVec.class).getElementType();
        DBSPExpression zero = new DBSPVecLiteral(elementType);
        DBSPExpression aggregatedValue = this.getAggregatedValue();
        DBSPVariablePath accumulator = this.resultType.var(this.genAccumulatorName());
        String functionName;
        DBSPExpression[] arguments;
        if (ignoreNulls && elementType.mayBeNull) {
            functionName = "array_agg_opt";
            arguments = new DBSPExpression[5];
        } else {
            functionName = "array_agg";
            arguments = new DBSPExpression[4];
        }
        arguments[0] = accumulator.borrow(true);
        arguments[1] = aggregatedValue.applyCloneIfNeeded();
        arguments[2] = this.compiler.weightVar;
        arguments[3] = new DBSPBoolLiteral(distinct);
        if (arguments.length == 5) {
            arguments[4] = new DBSPBoolLiteral(ignoreNulls);
        }
        DBSPExpression increment = new DBSPApplyExpression(node, functionName, this.resultType, arguments);
        DBSPType semigroup = new DBSPTypeUser(node, SEMIGROUP, "ConcatSemigroup", false, accumulator.getType());
        this.setFoldingFunction(new DBSPAggregate.Implementation(
                node, zero, this.makeRowClosure(increment, accumulator), zero, semigroup, null));
    }

    void processBasic(SqlBasicAggFunction function) {
        SqlKind kind = function.getKind();
        if (kind == SqlKind.ARRAY_AGG) {
            this.processArrayAgg(function);
            return;
        }
        CalciteObject node = CalciteObject.create(function);
        DBSPTupleExpression tuple = Objects.requireNonNull(this.aggArgument).to(DBSPTupleExpression.class);
        assert tuple.fields.length == 2: "Expected 2 arguments for " + kind;
        DBSPOpcode compare = switch (kind) {
            case ARG_MAX -> DBSPOpcode.AGG_GTE;
            case ARG_MIN -> DBSPOpcode.AGG_LTE;
            default -> throw new UnimplementedException(node);
        };

        // Accumulator is a pair of fields.
        DBSPExpression zero = new DBSPTupleExpression(
                tuple.fields[0].getType().nullValue(), tuple.fields[1].getType().nullValue());
        DBSPVariablePath accumulator = tuple.getType().var(this.genAccumulatorName());
        DBSPClosureExpression linear = null;  // not linear
        DBSPExpression ge = new DBSPBinaryExpression(
                node, DBSPTypeBool.create(false), compare,
                tuple.fields[1].applyCloneIfNeeded(),
                accumulator.field(1).applyCloneIfNeeded());
        DBSPExpression increment = new DBSPIfExpression(node, ge, this.aggArgument, accumulator.applyCloneIfNeeded());
        DBSPType semigroup = new DBSPTypeUser(node, SEMIGROUP, "UnimplementedSemigroup", false, tuple.getType());
        DBSPExpression postBody = accumulator.field(0).applyCloneIfNeeded();
        this.setFoldingFunction(new DBSPAggregate.Implementation(
                node, zero,
                this.makeRowClosure(increment, accumulator),
                postBody.closure(accumulator.asParameter()),
                this.resultType.nullValue(),
                semigroup, linear));
    }

    private DBSPExpression getAggregatedValue() {
        return Objects.requireNonNull(this.aggArgument);
    }

    private DBSPType getAggregatedValueType() {
        return this.getAggregatedValue().getType();
    }

    void setFoldingFunction(DBSPAggregate.Implementation foldingFunction) {
        this.foldingFunction = foldingFunction;
        if (!this.isWindowAggregate())
            this.foldingFunction.validate();
    }

    public DBSPExpression aggregateOperation(
            CalciteObject node, DBSPOpcode op,
            DBSPType type, DBSPExpression left, DBSPExpression right, @Nullable DBSPExpression filter) {
        DBSPType leftType = left.getType();
        DBSPType rightType = right.getType();
        DBSPType resultType = type.setMayBeNull(leftType.mayBeNull || rightType.mayBeNull);
        DBSPExpression binOp = new DBSPConditionalAggregateExpression(
                node, op, resultType, left.applyCloneIfNeeded(), right.applyCloneIfNeeded(), filter);
        return binOp.cast(type);
    }

    void processMinMax(SqlMinMaxAggFunction function) {
        DBSPExpression zero = DBSPLiteral.none(this.nullableResultType);
        CalciteObject node = CalciteObject.create(function);
        DBSPOpcode call;
        String semigroupName = switch (function.getKind()) {
            case MIN -> {
                call = DBSPOpcode.AGG_MIN;
                yield "MinSemigroup";
            }
            case MAX -> {
                call = DBSPOpcode.AGG_MAX;
                yield "MaxSemigroup";
            }
            default -> throw new UnimplementedException(node);
        };
        DBSPExpression aggregatedValue = this.getAggregatedValue();
        DBSPVariablePath accumulator = this.nullableResultType.var(this.genAccumulatorName());
        DBSPExpression increment = this.aggregateOperation(
                node, call, this.nullableResultType, accumulator, aggregatedValue, this.filterArgument());
        DBSPType semigroup = new DBSPTypeUser(node, SEMIGROUP, semigroupName, false, accumulator.getType());
        this.setFoldingFunction(new DBSPAggregate.Implementation(
                node, zero, this.makeRowClosure(increment, accumulator), zero, semigroup, null));
    }

    void processSum(SqlSumAggFunction function) {
        CalciteObject node = CalciteObject.create(function);
        DBSPExpression zero = DBSPLiteral.none(this.nullableResultType);
        DBSPExpression increment;
        DBSPExpression aggregatedValue = this.getAggregatedValue();
        DBSPVariablePath accumulator = this.nullableResultType.var(this.genAccumulatorName());

        if (this.isDistinct) {
            increment = this.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD,
                    this.nullableResultType, accumulator, aggregatedValue, this.filterArgument());
        } else {
            DBSPExpression weighted = new DBSPBinaryExpression(node,
                    aggregatedValue.getType(), DBSPOpcode.MUL_WEIGHT,
                    aggregatedValue, this.compiler.weightVar);
            increment = this.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD, this.nullableResultType,
                    accumulator, weighted, this.filterArgument());
        }
        DBSPType semigroup = new DBSPTypeUser(CalciteObject.EMPTY, SEMIGROUP, "DefaultOptSemigroup",
                false, accumulator.getType().setMayBeNull(false));
        this.setFoldingFunction(new DBSPAggregate.Implementation(
                node, zero, this.makeRowClosure(increment, accumulator), zero, semigroup, null));
    }

    void processSumZero(SqlSumEmptyIsZeroAggFunction function) {
        CalciteObject node = CalciteObject.create(function);
        DBSPExpression zero = this.resultType.to(IsNumericType.class).getZero();
        DBSPExpression increment;
        DBSPExpression aggregatedValue = this.getAggregatedValue();
        DBSPVariablePath accumulator = this.resultType.var(this.genAccumulatorName());

        @Nullable
        DBSPClosureExpression linear = null;
        if (this.isDistinct) {
            increment = this.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD,
                    this.resultType, accumulator, aggregatedValue, this.filterArgument());
        } else {
            linear = aggregatedValue.closure(this.v.asParameter());
            DBSPExpression weighted = new DBSPBinaryExpression(
                    node, aggregatedValue.getType(),
                    DBSPOpcode.MUL_WEIGHT, aggregatedValue, this.compiler.weightVar);
            increment = this.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD, this.resultType,
                    accumulator, weighted, this.filterArgument());
        }
        String semigroupName = "DefaultSemigroup";
        if (this.filterArgument >= 0)
            // If we are filtering the sum is no longer linear
            linear = null;
        if (accumulator.getType().mayBeNull)
            semigroupName = "DefaultOptSemigroup";
        DBSPType semigroup = new DBSPTypeUser(node, SEMIGROUP, semigroupName, false,
                accumulator.getType().setMayBeNull(false));
        this.setFoldingFunction(new DBSPAggregate.Implementation(
                node, zero, this.makeRowClosure(increment, accumulator), zero, semigroup, linear));
    }

    void processSingle(SqlSingleValueAggFunction function) {
        CalciteObject node = CalciteObject.create(function);
        DBSPExpression zero = DBSPLiteral.none(this.nullableResultType);
        DBSPExpression aggregatedValue = this.getAggregatedValue();
        if (this.filterArgument >= 0) {
            throw new UnimplementedException(node);
            //aggregatedValue = new DBSPBinaryExpression(node, aggregatedValue.type,
            //        DBSPOpcode.IF_SELECTED, aggregatedValue, this.v.field(this.filterArgument));
        }
        DBSPVariablePath accumulator = this.nullableResultType.var(this.genAccumulatorName());
        // Single is supposed to be applied to a single value, and should return a runtime
        // error otherwise.  We approximate this behavior by returning the last value seen.
        DBSPExpression increment = aggregatedValue;
        if (!increment.getType().mayBeNull)
            increment = increment.some();
        DBSPType semigroup = new DBSPTypeUser(CalciteObject.EMPTY, SEMIGROUP, "UnimplementedSemigroup",
                false, accumulator.getType());
        this.setFoldingFunction(new DBSPAggregate.Implementation(
                node, zero, this.makeRowClosure(increment, accumulator), zero, semigroup, null));
    }

    DBSPAggregate.Implementation doAverage(SqlAvgAggFunction function) {
        assert function.getKind() == SqlKind.AVG;
        CalciteObject node = CalciteObject.create(function);
        DBSPType aggregatedValueType = this.getAggregatedValueType();
        DBSPType intermediateResultType = aggregatedValueType.setMayBeNull(true);
        DBSPExpression zero = new DBSPTupleExpression(
                DBSPLiteral.none(intermediateResultType), DBSPLiteral.none(intermediateResultType));
        DBSPType pairType = zero.getType();
        DBSPExpression count, sum;
        DBSPVariablePath accumulator = pairType.var(this.genAccumulatorName());
        final int sumIndex = 0;
        final int countIndex = 1;
        DBSPExpression countAccumulator = accumulator.field(countIndex);
        DBSPExpression sumAccumulator = accumulator.field(sumIndex);
        DBSPExpression aggregatedValue = this.getAggregatedValue().cast(intermediateResultType);
        DBSPType intermediateResultTypeNonNull = intermediateResultType.setMayBeNull(false);
        DBSPExpression plusOne = intermediateResultTypeNonNull.to(IsNumericType.class).getOne();

        if (aggregatedValueType.mayBeNull)
            plusOne = ExpressionCompiler.makeIndicator(
                    node, intermediateResultTypeNonNull, aggregatedValue.deepCopy());
        if (this.isDistinct) {
            count = this.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD,
                    intermediateResultType, countAccumulator, plusOne, this.filterArgument());
            sum = this.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD,
                    intermediateResultType, sumAccumulator, aggregatedValue, this.filterArgument());
        } else {
            DBSPExpression weightedCount = new DBSPBinaryExpression(
                    node, intermediateResultType.setMayBeNull(plusOne.getType().mayBeNull),
                    DBSPOpcode.MUL_WEIGHT, plusOne,
                    this.compiler.weightVar);
            count = this.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD, intermediateResultType,
                    countAccumulator, weightedCount, this.filterArgument());
            DBSPExpression weightedSum = new DBSPBinaryExpression(
                    node, intermediateResultType, DBSPOpcode.MUL_WEIGHT,
                    aggregatedValue, this.compiler.weightVar);
            sum = this.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD, intermediateResultType,
                    sumAccumulator, weightedSum, this.filterArgument());
        }
        DBSPExpression increment = new DBSPTupleExpression(sum, count);

        DBSPVariablePath a = pairType.var(this.genAccumulatorName());
        DBSPExpression divide = ExpressionCompiler.makeBinaryExpression(
                node, this.resultType, DBSPOpcode.DIV,
                a.field(sumIndex), a.field(countIndex));
        divide = divide.cast(this.nullableResultType);
        DBSPClosureExpression post = new DBSPClosureExpression(
                node, divide, a.asParameter());
        DBSPExpression postZero = DBSPLiteral.none(this.nullableResultType);
        DBSPType semigroup = new DBSPTypeUser(node, SEMIGROUP, "PairSemigroup", false,
                intermediateResultType, intermediateResultType,
                new DBSPTypeUser(node, SEMIGROUP, "DefaultOptSemigroup", false, intermediateResultTypeNonNull),
                new DBSPTypeUser(node, SEMIGROUP, "DefaultOptSemigroup", false, intermediateResultTypeNonNull));
        return new DBSPAggregate.Implementation(
                node, zero, this.makeRowClosure(increment, accumulator), post, postZero, semigroup, null);
    }

    DBSPAggregate.Implementation doStddev(SqlAvgAggFunction function) {
        assert function.getKind() == SqlKind.STDDEV_POP || function.getKind() == SqlKind.STDDEV_SAMP;
        boolean isSamp = function.getKind() == SqlKind.STDDEV_SAMP;
        CalciteObject node = CalciteObject.create(function);
        DBSPType aggregatedValueType = this.getAggregatedValueType();
        DBSPType intermediateResultType = aggregatedValueType.setMayBeNull(true);
        // Compute 3 sums: Sum(value^2), Sum(value), Count(value)
        DBSPExpression zero = new DBSPTupleExpression(
                DBSPLiteral.none(intermediateResultType),
                DBSPLiteral.none(intermediateResultType),
                DBSPLiteral.none(intermediateResultType));
        DBSPType tripleType = zero.getType();
        DBSPExpression count, sum, sumSquares;
        DBSPVariablePath accumulator = tripleType.var(this.genAccumulatorName());
        final int sumIndex = 0;
        final int countIndex = 1;
        final int sumSquaresIndex = 2;
        DBSPExpression countAccumulator = accumulator.field(countIndex);
        DBSPExpression sumAccumulator = accumulator.field(sumIndex);
        DBSPExpression sumSquaresAccumulator = accumulator.field(sumSquaresIndex);

        DBSPExpression aggregatedValue = this.getAggregatedValue().cast(intermediateResultType);
        DBSPType intermediateResultTypeNonNull = intermediateResultType.setMayBeNull(false);
        DBSPExpression plusOne = intermediateResultTypeNonNull.to(IsNumericType.class).getOne();

        if (aggregatedValueType.mayBeNull)
            plusOne = ExpressionCompiler.makeIndicator(node, intermediateResultTypeNonNull, aggregatedValue.deepCopy());
        if (this.isDistinct) {
            count = this.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD,
                    intermediateResultType, countAccumulator, plusOne, this.filterArgument());
            sum = this.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD,
                    intermediateResultType, sumAccumulator, aggregatedValue, this.filterArgument());
            sumSquares = this.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD,
                    intermediateResultType, sumAccumulator, ExpressionCompiler.makeBinaryExpression(
                            node, intermediateResultType, DBSPOpcode.MUL, aggregatedValue, aggregatedValue),
                    this.filterArgument());
        } else {
            DBSPExpression weightedCount = new DBSPBinaryExpression(
                    node, intermediateResultType.setMayBeNull(plusOne.getType().mayBeNull),
                    DBSPOpcode.MUL_WEIGHT, plusOne,
                    this.compiler.weightVar);
            count = this.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD, intermediateResultType,
                    countAccumulator, weightedCount, this.filterArgument());

            DBSPExpression weightedElement = new DBSPBinaryExpression(
                    node, intermediateResultType, DBSPOpcode.MUL_WEIGHT,
                    aggregatedValue, this.compiler.weightVar);
            sum = this.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD, intermediateResultType,
                    sumAccumulator, weightedElement, this.filterArgument());

            DBSPExpression weightedSq = ExpressionCompiler.makeBinaryExpression(
                    node, intermediateResultType, DBSPOpcode.MUL,
                    aggregatedValue, weightedElement);
            sumSquares = this.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD,
                    intermediateResultType, sumSquaresAccumulator, weightedSq,
                    this.filterArgument());
        }
        DBSPExpression increment = new DBSPTupleExpression(sum, count, sumSquares);

        DBSPVariablePath a = tripleType.var(this.genAccumulatorName());
        DBSPExpression sumSquared = ExpressionCompiler.makeBinaryExpression(
                node, this.resultType, DBSPOpcode.MUL,
                a.field(sumIndex), a.field(sumIndex));
        DBSPExpression normalized = ExpressionCompiler.makeBinaryExpression(
                node, this.resultType, DBSPOpcode.DIV,
                sumSquared, a.field(countIndex));
        DBSPExpression sub = ExpressionCompiler.makeBinaryExpression(
                node, this.resultType, DBSPOpcode.SUB,
                a.field(sumSquaresIndex), normalized);

        DBSPExpression denom = isSamp ? ExpressionCompiler.makeBinaryExpression(
                node, this.resultType, DBSPOpcode.SUB,
                a.field(countIndex), this.resultType.to(IsNumericType.class).getOne()) :
                a.field(countIndex);
        // We need to call sqrt, which only works for doubles.
        DBSPType sqrtType = new DBSPTypeDouble(node, this.resultType.mayBeNull);
        DBSPExpression div = ExpressionCompiler.makeBinaryExpression(
                node, this.resultType, DBSPOpcode.DIV_NULL,
                sub, denom).cast(sqrtType);
        DBSPExpression sqrt = ExpressionCompiler.compilePolymorphicFunction(
                "sqrt", node, sqrtType, Linq.list(div), 1);
        sqrt = sqrt.cast(this.resultType);
        DBSPClosureExpression post = new DBSPClosureExpression(node, sqrt, a.asParameter());
        DBSPExpression postZero = DBSPLiteral.none(this.nullableResultType);
        DBSPType semigroup = new DBSPTypeUser(node, SEMIGROUP, "TripleSemigroup", false,
                intermediateResultType, intermediateResultType, intermediateResultType,
                new DBSPTypeUser(node, SEMIGROUP, "DefaultOptSemigroup", false, intermediateResultTypeNonNull),
                new DBSPTypeUser(node, SEMIGROUP, "DefaultOptSemigroup", false, intermediateResultTypeNonNull),
                new DBSPTypeUser(node, SEMIGROUP, "DefaultOptSemigroup", false, intermediateResultTypeNonNull));
        return new DBSPAggregate.Implementation(
                node, zero, this.makeRowClosure(increment, accumulator), post, postZero, semigroup, null);
    }

    void processAvg(SqlAvgAggFunction function) {
        CalciteObject node = CalciteObject.create(function);
        DBSPAggregate.Implementation implementation = switch (function.getKind()) {
            case AVG -> this.doAverage(function);
            case STDDEV_POP, STDDEV_SAMP -> this.doStddev(function);
            default -> throw new UnimplementedException(node);
        };
        this.setFoldingFunction(implementation);
    }

    public DBSPAggregate.Implementation compile() {
        boolean success =
                this.process(this.aggFunction, SqlCountAggFunction.class, this::processCount) ||
                this.process(this.aggFunction, SqlBasicAggFunction.class, this::processBasic) ||
                this.process(this.aggFunction, SqlMinMaxAggFunction.class, this::processMinMax) ||
                this.process(this.aggFunction, SqlSumAggFunction.class, this::processSum) ||
                this.process(this.aggFunction, SqlSumEmptyIsZeroAggFunction.class, this::processSumZero) ||
                this.process(this.aggFunction, SqlAvgAggFunction.class, this::processAvg) ||
                this.process(this.aggFunction, SqlBitOpAggFunction.class, this::processBitOp) ||
                this.process(this.aggFunction, SqlSingleValueAggFunction.class, this::processSingle) ||
                this.process(this.aggFunction, SqlAbstractGroupFunction.class, this::processGrouping);
        if (!success || this.foldingFunction == null)
            throw new UnimplementedException(CalciteObject.create(this.aggFunction));
        return this.foldingFunction;
    }

    @Override
    public DBSPCompiler getCompiler() {
        return this.compiler;
    }
}
