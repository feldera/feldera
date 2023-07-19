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

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.*;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.util.*;

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
    // null only for COUNT(*)
    @Nullable
    private final DBSPExpression aggArgument;
    private final NameGen generator;

    public AggregateCompiler(
            CalciteObject node,
            DBSPCompiler compiler,
            AggregateCall call, DBSPType resultType,
            DBSPVariablePath v) {
        this.compiler = compiler;
        this.resultType = resultType;
        this.nullableResultType = resultType.setMayBeNull(true);
        this.foldingFunction = null;
        this.v = v;
        this.isDistinct = call.isDistinct();
        this.aggFunction = call.getAggregation();
        this.generator = new NameGen("a");
        List<Integer> argList = call.getArgList();
        if (argList.size() == 0) {
            this.aggArgument = null;
        } else if (argList.size() == 1) {
            int fieldNumber = call.getArgList().get(0);
            this.aggArgument = this.v.field(fieldNumber);
        } else {
            throw new UnimplementedException(new CalciteObject(call.getAggregation()));
        }
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

    void processCount(SqlCountAggFunction function) {
        // The result of 'count' can never be null.
        CalciteObject node = new CalciteObject(function);
        DBSPExpression zero = this.resultType.to(IsNumericType.class).getZero();
        DBSPExpression increment;
        DBSPExpression argument;
        DBSPExpression one = this.resultType.to(DBSPTypeInteger.class).getOne();
        if (this.aggArgument == null) {
            // COUNT(*)
            argument = one;
        } else {
            DBSPExpression agg = this.getAggregatedValue();
            if (agg.getType().mayBeNull)
                argument = new DBSPUnaryExpression(node, this.resultType.setMayBeNull(false),
                        DBSPOpcode.INDICATOR, agg);
            else
                argument = one;
        }

        @Nullable
        DBSPClosureExpression linear = argument.closure(this.v.asParameter());
        DBSPVariablePath accumulator = this.resultType.var(this.genAccumulatorName());
        if (this.isDistinct) {
            linear = null;
            increment = ExpressionCompiler.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD,
                    this.resultType, accumulator, argument);
        } else {
            increment = ExpressionCompiler.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD, this.resultType,
                    accumulator, new DBSPBinaryExpression(node, DBSPTypeInteger.SIGNED_64,
                            DBSPOpcode.MUL_WEIGHT,
                            argument,
                            this.compiler.weightVar));
        }
        DBSPType semigroup = new DBSPTypeUser(node, USER, "DefaultSemigroup", false, this.resultType);
        this.foldingFunction = new DBSPAggregate.Implementation(
                node, zero, this.makeRowClosure(increment, accumulator),
                zero, semigroup, linear);
    }

    private DBSPExpression getAggregatedValue() {
        return Objects.requireNonNull(this.aggArgument);
    }

    private DBSPType getAggregatedValueType() {
        return this.getAggregatedValue().getType();
    }

    void processMinMax(SqlMinMaxAggFunction function) {
        DBSPExpression zero = DBSPLiteral.none(this.nullableResultType);
        CalciteObject node = new CalciteObject(function);
        DBSPOpcode call;
        String semigroupName;
        switch (function.getKind()) {
            case MIN:
                call = DBSPOpcode.AGG_MIN;
                semigroupName = "MinSemigroup";
                break;
            case MAX:
                call = DBSPOpcode.AGG_MAX;
                semigroupName = "MaxSemigroup";
                break;
            default:
                throw new UnimplementedException(node);
        }
        DBSPExpression aggregatedValue = this.getAggregatedValue();
        DBSPVariablePath accumulator = this.nullableResultType.var(this.genAccumulatorName());
        DBSPExpression increment = ExpressionCompiler.aggregateOperation(
                node, call, this.nullableResultType, accumulator, aggregatedValue);
        DBSPType semigroup = new DBSPTypeUser(node, USER, semigroupName, false, accumulator.getType());
        this.foldingFunction = new DBSPAggregate.Implementation(
                node, zero, this.makeRowClosure(increment, accumulator), zero, semigroup, null);
    }

    void processSum(SqlSumAggFunction function) {
        CalciteObject node = new CalciteObject(function);
        DBSPExpression zero = DBSPLiteral.none(this.nullableResultType);
        DBSPExpression increment;
        DBSPExpression aggregatedValue = this.getAggregatedValue();
        DBSPVariablePath accumulator = this.nullableResultType.var(this.genAccumulatorName());

        if (this.isDistinct) {
            increment = ExpressionCompiler.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD,
                    this.nullableResultType, accumulator, aggregatedValue);
        } else {
            increment = ExpressionCompiler.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD, this.nullableResultType,
                    accumulator, new DBSPBinaryExpression(node,
                            aggregatedValue.getType(),
                            DBSPOpcode.MUL_WEIGHT,
                            aggregatedValue,
                            this.compiler.weightVar));
        }
        DBSPType semigroup = new DBSPTypeUser(CalciteObject.EMPTY, USER, "DefaultOptSemigroup",
                false, accumulator.getType().setMayBeNull(false));
        this.foldingFunction = new DBSPAggregate.Implementation(
                node, zero, this.makeRowClosure(increment, accumulator), zero, semigroup, null);
    }

    void processSumZero(SqlSumEmptyIsZeroAggFunction function) {
        DBSPExpression zero = this.resultType.to(IsNumericType.class).getZero();
        DBSPExpression increment;
        DBSPExpression aggregatedValue = this.getAggregatedValue();
        DBSPVariablePath accumulator = this.resultType.var(this.genAccumulatorName());
        CalciteObject node = new CalciteObject(function);

        @Nullable
        DBSPClosureExpression linear = null;
        if (this.isDistinct) {
            increment = ExpressionCompiler.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD,
                    this.resultType, accumulator, aggregatedValue);
        } else {
            linear = aggregatedValue.closure(this.v.asParameter());
            increment = ExpressionCompiler.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD, this.resultType,
                    accumulator, new DBSPBinaryExpression(
                            node, aggregatedValue.getType(),
                            DBSPOpcode.MUL_WEIGHT, aggregatedValue, this.compiler.weightVar));
        }
        String semigroupName = "DefaultSemigroup";
        if (accumulator.getType().mayBeNull)
            semigroupName = "DefaultOptSemigroup";
        DBSPType semigroup = new DBSPTypeUser(node, USER, semigroupName, false,
                accumulator.getType().setMayBeNull(false));
        this.foldingFunction = new DBSPAggregate.Implementation(
                node, zero, this.makeRowClosure(increment, accumulator), zero, semigroup, linear);
    }

    void processAvg(SqlAvgAggFunction function) {
        CalciteObject node = new CalciteObject(function);
        DBSPType aggregatedValueType = this.getAggregatedValueType();
        DBSPType i64 = DBSPTypeInteger.SIGNED_64.setMayBeNull(true);
        DBSPExpression zero = new DBSPRawTupleExpression(
                DBSPLiteral.none(i64), DBSPLiteral.none(i64));
        DBSPType pairType = zero.getType();
        DBSPExpression count, sum;
        DBSPVariablePath accumulator = pairType.var(this.genAccumulatorName());
        final int sumIndex = 0;
        final int countIndex = 1;
        DBSPExpression countAccumulator = accumulator.field(countIndex);
        DBSPExpression sumAccumulator = accumulator.field(sumIndex);
        DBSPExpression aggregatedValue = this.getAggregatedValue().cast(i64);
        DBSPExpression plusOne = new DBSPI64Literal(1L);

        if (aggregatedValueType.mayBeNull)
            plusOne = new DBSPUnaryExpression(node, DBSPTypeInteger.SIGNED_64,
                    DBSPOpcode.INDICATOR, aggregatedValue);
        if (this.isDistinct) {
            count = ExpressionCompiler.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD,
                    i64, countAccumulator, plusOne);
            sum = ExpressionCompiler.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD,
                    i64, sumAccumulator, aggregatedValue);
        } else {
            count = ExpressionCompiler.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD, i64,
                    countAccumulator, new DBSPBinaryExpression(
                            node, DBSPTypeInteger.SIGNED_64.setMayBeNull(plusOne.getType().mayBeNull),
                            DBSPOpcode.MUL_WEIGHT, plusOne,
                            this.compiler.weightVar));
            sum = ExpressionCompiler.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD, i64,
                    sumAccumulator, new DBSPBinaryExpression(
                            node,
                            i64, DBSPOpcode.MUL_WEIGHT,
                            aggregatedValue, this.compiler.weightVar));
        }
        DBSPExpression increment = new DBSPRawTupleExpression(sum, count);

        DBSPVariablePath a = pairType.var(this.genAccumulatorName());
        DBSPExpression divide = ExpressionCompiler.makeBinaryExpression(
                node, this.resultType, DBSPOpcode.DIV,
                Linq.list(a.field(sumIndex), a.field(countIndex)));
        divide = divide.cast(this.nullableResultType);
        DBSPClosureExpression post = new DBSPClosureExpression(
                node, divide, a.asParameter());
        DBSPExpression postZero = DBSPLiteral.none(this.nullableResultType);
        DBSPType semigroup = new DBSPTypeUser(node, USER, "PairSemigroup", false, i64, i64,
                new DBSPTypeUser(node, USER, "DefaultOptSemigroup", false, DBSPTypeInteger.SIGNED_64),
                new DBSPTypeUser(node, USER, "DefaultOptSemigroup", false, DBSPTypeInteger.SIGNED_64));
        this.foldingFunction = new DBSPAggregate.Implementation(
                node, zero, this.makeRowClosure(increment, accumulator), post, postZero, semigroup, null);
    }

    public DBSPAggregate.Implementation compile() {
        boolean success =
                this.process(this.aggFunction, SqlCountAggFunction.class, this::processCount) ||
                this.process(this.aggFunction, SqlMinMaxAggFunction.class, this::processMinMax) ||
                this.process(this.aggFunction, SqlSumAggFunction.class, this::processSum) ||
                this.process(this.aggFunction, SqlSumEmptyIsZeroAggFunction.class, this::processSumZero) ||
                this.process(this.aggFunction, SqlAvgAggFunction.class, this::processAvg);
        if (!success || this.foldingFunction == null)
            throw new UnimplementedException(new CalciteObject(this.aggFunction));
        return this.foldingFunction;
    }

    @Override
    public DBSPCompiler getCompiler() {
        return this.compiler;
    }
}
