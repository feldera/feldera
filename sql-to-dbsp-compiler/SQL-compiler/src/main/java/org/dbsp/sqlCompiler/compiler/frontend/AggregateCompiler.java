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
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlBitOpAggFunction;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlMinMaxAggFunction;
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConditionalAggregateExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeNull;
import org.dbsp.util.ICastable;
import org.dbsp.util.Linq;
import org.dbsp.util.NameGen;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.USER;

/**
 * Compiles SQL aggregate functions.
 */
public class AggregateCompiler implements ICompilerComponent {
    public final DBSPCompiler compiler;
    /**
     * Type of result expected.
     */
    public final DBSPType resultType;
    /**
     * Almost all aggregates may return nullable results, even if Calcite pretends it's not true.
     */
    public final DBSPType nullableResultType;
    // Deposit compilation result here
    @Nullable
    private DBSPAggregate.Implementation foldingFunction;
    
    /**
     * Expression that stands for the whole input row in the input zset.
     */
    private final DBSPVariablePath v;
    private final boolean isDistinct;
    private final SqlAggFunction aggFunction;
    private final int filterArgument;
    // null only for COUNT(*)
    @Nullable
    private final DBSPExpression aggArgument;
    private final NameGen generator;
    private final RelNode aggregateNode;


    public AggregateCompiler(
            RelNode node,
            DBSPCompiler compiler,
            AggregateCall call, DBSPType resultType,
            DBSPVariablePath v) {
        this.aggregateNode = node;
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
            throw new UnimplementedException(CalciteObject.create(call.getAggregation()));
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
        DBSPType semigroup = new DBSPTypeUser(CalciteObject.EMPTY, USER, "UnimplementedSemigroup",
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
            if (agg.getType().mayBeNull)
                argument = new DBSPUnaryExpression(node, this.resultType.setMayBeNull(false),
                        DBSPOpcode.INDICATOR, agg.borrow());
            else
                argument = one;
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
            DBSPExpression weighted = new DBSPBinaryExpression(node, new DBSPTypeInteger(CalciteObject.EMPTY, 64, true,false),
                    DBSPOpcode.MUL_WEIGHT, argument, this.compiler.weightVar);
            increment = this.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD, this.resultType,
                    accumulator, weighted, this.filterArgument());
        }
        if (this.filterArgument >= 0)
            // no longer linear, since it returns 0 instead of nothing for an empty collection
            linear = null;
        DBSPType semigroup = new DBSPTypeUser(node, USER, "DefaultSemigroup", false, this.resultType);
        this.setFoldingFunction(new DBSPAggregate.Implementation(
                node, zero, this.makeRowClosure(increment, accumulator),
                zero, semigroup, linear));
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
        DBSPType commonBase = ExpressionCompiler.reduceType(leftType, rightType);
        if (commonBase.is(DBSPTypeNull.class)) {
            return DBSPLiteral.none(type);
        }
        DBSPType resultType = commonBase.setMayBeNull(leftType.mayBeNull || rightType.mayBeNull);
        DBSPExpression binOp = new DBSPConditionalAggregateExpression(node, op, resultType, left, right, filter);
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
        DBSPType semigroup = new DBSPTypeUser(node, USER, semigroupName, false, accumulator.getType());
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
        DBSPType semigroup = new DBSPTypeUser(CalciteObject.EMPTY, USER, "DefaultOptSemigroup",
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
        DBSPType semigroup = new DBSPTypeUser(node, USER, semigroupName, false,
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
        DBSPType semigroup = new DBSPTypeUser(CalciteObject.EMPTY, USER, "DefaultOptSemigroup",
                false, accumulator.getType().setMayBeNull(false));
        this.setFoldingFunction(new DBSPAggregate.Implementation(
                node, zero, this.makeRowClosure(increment, accumulator), zero, semigroup, null));
    }

    void processAvg(SqlAvgAggFunction function) {
        CalciteObject node = CalciteObject.create(function);
        DBSPType aggregatedValueType = this.getAggregatedValueType();
        DBSPType intermediateResultType = aggregatedValueType.setMayBeNull(true);
        if (aggregatedValueType.is(DBSPTypeInteger.class))
            // If the result type is an integer type use the highest possible precision for intermediate results.
            intermediateResultType = new DBSPTypeInteger(CalciteObject.EMPTY, 64, true,true);
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
            plusOne = new DBSPUnaryExpression(node,
                    new DBSPTypeInteger(CalciteObject.EMPTY, 64, true,false),
                    DBSPOpcode.INDICATOR, aggregatedValue.deepCopy().borrow()).cast(intermediateResultTypeNonNull);
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
                Linq.list(a.field(sumIndex), a.field(countIndex)));
        divide = divide.cast(this.nullableResultType);
        DBSPClosureExpression post = new DBSPClosureExpression(
                node, divide, a.asParameter());
        DBSPExpression postZero = DBSPLiteral.none(this.nullableResultType);
        DBSPType semigroup = new DBSPTypeUser(node, USER, "PairSemigroup", false,
                intermediateResultType, intermediateResultType,
                new DBSPTypeUser(node, USER, "DefaultOptSemigroup", false, intermediateResultTypeNonNull),
                new DBSPTypeUser(node, USER, "DefaultOptSemigroup", false, intermediateResultTypeNonNull));
        this.setFoldingFunction(new DBSPAggregate.Implementation(
                node, zero, this.makeRowClosure(increment, accumulator), post, postZero, semigroup, null));
    }

    public DBSPAggregate.Implementation compile() {
        boolean success =
                this.process(this.aggFunction, SqlCountAggFunction.class, this::processCount) ||
                this.process(this.aggFunction, SqlMinMaxAggFunction.class, this::processMinMax) ||
                this.process(this.aggFunction, SqlSumAggFunction.class, this::processSum) ||
                this.process(this.aggFunction, SqlSumEmptyIsZeroAggFunction.class, this::processSumZero) ||
                this.process(this.aggFunction, SqlAvgAggFunction.class, this::processAvg) ||
                this.process(this.aggFunction, SqlBitOpAggFunction.class, this::processBitOp) ||
                this.process(this.aggFunction, SqlSingleValueAggFunction.class, this::processSingle);
        if (!success || this.foldingFunction == null)
            throw new UnimplementedException(CalciteObject.create(this.aggFunction));
        return this.foldingFunction;
    }

    @Override
    public DBSPCompiler getCompiler() {
        return this.compiler;
    }
}
