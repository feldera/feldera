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
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.aggregate.AggregateBase;
import org.dbsp.sqlCompiler.ir.aggregate.LinearAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.MinMaxAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.NonLinearAggregate;
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
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBinary;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeNull;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeRuntimeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeVec;
import org.dbsp.util.ICastable;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.SEMIGROUP;

/** Compiles SQL aggregate functions. */
public class AggregateCompiler implements ICompilerComponent {
    public final CalciteObject node;
    public final DBSPCompiler compiler;
    /** Type of result expected. */
    public final DBSPType resultType;
    /** Almost all aggregates may return nullable results, even if Calcite pretends it's not true. */
    public final DBSPType nullableResultType;
    // Deposit compilation result here
    @Nullable
    private AggregateBase result;

    /** Expression that stands for the whole input row in the input zset. */
    private final DBSPVariablePath v;
    private final SqlAggFunction aggFunction;
    private final int filterArgument;
    // null only for COUNT(*)
    @Nullable
    private final DBSPExpression aggArgument;
    private final RelNode aggregateNode;
    private final ImmutableBitSet groups;
    private final AggregateCall call;
    private final boolean linearAllowed;

    public AggregateCompiler(
            RelNode node,
            DBSPCompiler compiler,
            AggregateCall call,
            DBSPType resultType,
            DBSPVariablePath v,
            ImmutableBitSet groups,
            boolean linearAllowed) {
        this.aggregateNode = node;
        this.node = CalciteObject.create(node, call);
        this.linearAllowed = linearAllowed;
        this.call = call;
        this.groups = groups;
        this.compiler = compiler;
        this.resultType = resultType;
        this.nullableResultType = resultType.withMayBeNull(true);
        this.result = null;
        this.v = v;
        // distinct aggregates should have been removed by preprocessing
        Utilities.enforce(!call.isDistinct());
        this.aggFunction = call.getAggregation();
        this.filterArgument = call.filterArg;
        List<Integer> argList = call.getArgList();
        if (argList.isEmpty()) {
            this.aggArgument = null;
        } else if (argList.size() == 1) {
            int fieldNumber = call.getArgList().get(0);
            this.aggArgument = this.v.deref().field(fieldNumber);
            if (this.aggArgument.getType().is(DBSPTypeNull.class)) {
                throw new CompilationError("Argument of aggregate has NULL type",
                        CalciteObject.create(call.getParserPosition()));
            }
        } else {
            List<DBSPExpression> fields = Linq.map(call.getArgList(),
                    a -> this.v.deref().field(a).applyCloneIfNeeded());
            this.aggArgument = new DBSPTupleExpression(fields, false);
        }
    }

    boolean isWindowAggregate() {
        return !(this.aggregateNode instanceof LogicalAggregate);
    }

    <T> boolean process(SqlAggFunction function, Class<T> clazz, Consumer<T> method) {
        T value = ICastable.as(function, clazz);
        if (value != null) {
            method.accept(value);
            return true;
        }
        return false;
    }

    /** Given the body of a closure, make a closure with arguments accumulator, row, weight */
    DBSPClosureExpression makeRowClosure(DBSPExpression body, DBSPVariablePath accumulator) {
        return body.closure(accumulator, this.v, this.compiler.weightVar);
    }

    @Nullable
    DBSPExpression filterArgument() {
        if (this.filterArgument < 0)
            return null;
        return this.v.deref().field(this.filterArgument);
    }

    void processBitOp(SqlBitOpAggFunction function) {
        DBSPExpression zero = DBSPLiteral.none(this.nullableResultType);
        DBSPExpression increment;
        DBSPExpression aggregatedValue = this.getAggregatedValue();
        DBSPVariablePath accumulator = this.nullableResultType.var();

        DBSPOpcode opcode = switch (function.getKind()) {
            case BIT_OR -> DBSPOpcode.AGG_OR;
            case BIT_AND -> DBSPOpcode.AGG_AND;
            case BIT_XOR -> DBSPOpcode.AGG_XOR;
            default -> throw new UnimplementedException("Aggregation function not yet implemented", node);
        };

        if (opcode == DBSPOpcode.AGG_XOR) {
            // This function returns if (w % 2 == 0) { 0 } else { aggregateValue }
            String helper = aggregatedValue.getType().is(DBSPTypeBinary.class) ?
                    "right_xor_weigh_bytes" : "right_xor_weigh";
            helper += aggregatedValue.getType().nullableSuffix();
            aggregatedValue = new DBSPApplyExpression(this.node, helper,
                    aggregatedValue.getType(), aggregatedValue.applyCloneIfNeeded(), this.compiler.weightVar);
        }
        increment = this.aggregateOperation(this.node, opcode,
                this.nullableResultType, accumulator, aggregatedValue, this.filterArgument());
        DBSPType semigroup = new DBSPTypeUser(CalciteObject.EMPTY, SEMIGROUP, "UnimplementedSemigroup",
                false, accumulator.getType());
        this.setResult(new NonLinearAggregate(
                this.node, zero, this.makeRowClosure(increment, accumulator), zero, semigroup));
    }

    void processGrouping(SqlAbstractGroupFunction function) {
        DBSPExpression zero = this.nullableResultType.to(IsNumericType.class).getZero();
        if (this.filterArgument() != null) {
            throw new UnimplementedException("GROUPING with FILTER not implemented", this.node);
        }

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

        // TODO: should this be looking at the filter argument?
        DBSPExpression increment = new DBSPI64Literal(result).cast(
                this.node, this.nullableResultType, false);
        DBSPVariablePath accumulator = this.nullableResultType.var();
        DBSPType semigroup = new DBSPTypeUser(CalciteObject.EMPTY, SEMIGROUP, "UnimplementedSemigroup",
                false, accumulator.getType());
        // Always non-linear (result can not be zero).
        this.setResult(new NonLinearAggregate(
                this.node, zero, this.makeRowClosure(increment, accumulator), zero, semigroup));
    }

    void processCount(SqlCountAggFunction function) {
        DBSPExpression zero = this.resultType.to(IsNumericType.class).getZero();
        DBSPExpression one = this.resultType.to(DBSPTypeInteger.class).getOne();
        if (this.linearAllowed) {
            DBSPClosureExpression map;
            DBSPClosureExpression post;
            if (this.aggArgument == null && this.filterArgument() == null) {
                // COUNT(*), no filter
                // map = |v| -> 1
                // post = |x| -> x
                map = one.closure(this.v);
                post = DBSPClosureExpression.id(one.getType());
            } else {
                // map = |v| ( if filter(v) { indicator(v.field) } else { 0 }, 1)
                DBSPExpression indicator;
                if (this.aggArgument != null) {
                    DBSPExpression agg = this.getAggregatedValue();
                    indicator = ExpressionCompiler.makeIndicator(this.node, this.resultType, agg);
                } else {
                    indicator = one;
                }

                DBSPExpression filter = this.filterArgument();
                DBSPExpression combined = indicator;
                if (filter != null)
                    combined = new DBSPIfExpression(this.node, filter, indicator, zero);
                DBSPExpression mapBody = new DBSPTupleExpression(combined, one);
                DBSPVariablePath postVar = mapBody.getType().var();
                // post = |x| x.0
                post = postVar.field(0).closure(postVar);
                map = mapBody.closure(this.v);
            }
            this.setResult(new LinearAggregate(node, map, post, zero));
        } else {
            // The result of 'count' can never be null.
            DBSPExpression increment;
            DBSPExpression argument;

            if (this.aggArgument == null) {
                // COUNT(*)
                argument = one;
            } else {
                DBSPExpression agg = this.getAggregatedValue();
                argument = ExpressionCompiler.makeIndicator(node, this.resultType, agg);
            }

            @Nullable
            DBSPVariablePath accumulator = this.resultType.var();
            DBSPExpression weighted = new DBSPBinaryExpression(node,
                    this.resultType, DBSPOpcode.MUL_WEIGHT, argument, this.compiler.weightVar);
            increment = this.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD, this.resultType,
                    accumulator, weighted, this.filterArgument());
            DBSPType semigroup = new DBSPTypeUser(node, SEMIGROUP, "DefaultSemigroup", false, this.resultType);
            this.setResult(new NonLinearAggregate(
                    node, zero, this.makeRowClosure(increment, accumulator),
                    zero, semigroup));
        }
    }

    void processArrayAgg(SqlBasicAggFunction function) {
        SqlKind kind = function.getKind();
        Utilities.enforce(kind == SqlKind.ARRAY_AGG);

        boolean ignoreNulls = this.call.ignoreNulls();
        boolean distinct = this.call.isDistinct();
        DBSPTypeArray arrayType = this.resultType.to(DBSPTypeArray.class).to(DBSPTypeArray.class);
        DBSPType elementType = arrayType.getElementType();
        DBSPTypeVec accumulatorType = arrayType.innerType();

        DBSPExpression empty = DBSPArrayExpression.emptyWithElementType(elementType, this.resultType.mayBeNull);
        DBSPExpression zero = accumulatorType.emptyVector();
        DBSPExpression aggregatedValue = this.getAggregatedValue();
        DBSPVariablePath accumulator = accumulatorType.var();
        String functionName;
        DBSPExpression[] arguments;
        if (ignoreNulls && elementType.mayBeNull) {
            functionName = "array_agg_opt" + this.resultType.nullableSuffix();
            arguments = new DBSPExpression[6];
        } else {
            functionName = "array_agg" + this.resultType.nullableSuffix();
            arguments = new DBSPExpression[5];
        }
        arguments[0] = accumulator.borrow(true);
        arguments[1] = aggregatedValue.applyCloneIfNeeded();
        arguments[2] = this.compiler.weightVar;
        arguments[3] = new DBSPBoolLiteral(distinct);
        arguments[4] = this.filterArgument >= 0 ? this.filterArgument() : new DBSPBoolLiteral(true);
        if (arguments.length == 6) {
            arguments[5] = new DBSPBoolLiteral(ignoreNulls);
        }
        DBSPExpression increment = new DBSPApplyExpression(
                node, functionName, DBSPTypeVoid.INSTANCE, arguments);
        DBSPType semigroup = new DBSPTypeUser(
                node, SEMIGROUP, "ConcatSemigroup", false, accumulatorType);
        DBSPVariablePath p = accumulatorType.var();
        String convertName = "to_array";
        if (arrayType.mayBeNull)
            convertName += "N";
        DBSPClosureExpression post = new DBSPApplyExpression(convertName, arrayType, p).closure(p);
        this.setResult(new NonLinearAggregate(
                node, zero, this.makeRowClosure(increment, accumulator), post, empty, semigroup));
    }

    void processBasic(SqlBasicAggFunction function) {
        // ARRAY_AGG or
        // ARG_MAX(value, compared).
        // For ARG_MAX first argument is the output, and has type `this.returnType`
        SqlKind kind = function.getKind();
        if (kind == SqlKind.ARRAY_AGG) {
            this.processArrayAgg(function);
            return;
        }
        DBSPOpcode compareOpcode = switch (kind) {
            case ARG_MAX -> DBSPOpcode.AGG_GTE;
            case ARG_MIN -> DBSPOpcode.AGG_LTE;
            default -> throw new UnimplementedException("Aggregate function not yet implemented", node);
        };

        DBSPTupleExpression tuple = Objects.requireNonNull(this.aggArgument).to(DBSPTupleExpression.class);
        Utilities.enforce(tuple.fields != null && tuple.fields.length == 2, "Expected 2 arguments for " + kind);
        DBSPType currentType = tuple.fields[1].getType().withMayBeNull(true);
        DBSPType zeroType = new DBSPTypeTuple(this.resultType, currentType);
        DBSPExpression zero = new DBSPTupleExpression(
                this.resultType.defaultValue(),
                currentType.nullValue());
        DBSPVariablePath accumulator = zeroType.var();
        DBSPExpression ge = new DBSPBinaryExpression(
                node, DBSPTypeBool.create(false), compareOpcode,
                tuple.fields[1].cast(this.node, currentType, false).applyCloneIfNeeded(),
                accumulator.field(1).applyCloneIfNeeded());
        if (this.filterArgument >= 0) {
            ge = ExpressionCompiler.makeBinaryExpression(
                    node, ge.getType(), DBSPOpcode.AND, ge, Objects.requireNonNull(this.filterArgument()));
        }
        DBSPTupleExpression aggArgCast = new DBSPTupleExpression(
                tuple.fields[0].cast(this.node, this.resultType, false).applyCloneIfNeeded(),
                tuple.fields[1].cast(this.node, currentType, false).applyCloneIfNeeded());
        DBSPExpression increment = new DBSPIfExpression(node, ge, aggArgCast, accumulator.applyCloneIfNeeded());
        DBSPType semigroup = new DBSPTypeUser(this.node, SEMIGROUP, "UnimplementedSemigroup",
                false, aggArgCast.getType());
        DBSPExpression postBody = accumulator.field(0).applyCloneIfNeeded();
        this.setResult(new NonLinearAggregate(
                node, zero,
                this.makeRowClosure(increment, accumulator),
                postBody.closure(accumulator),
                this.resultType.defaultValue(),
                semigroup));
    }

    private DBSPExpression getAggregatedValue() {
        return Objects.requireNonNull(this.aggArgument);
    }

    private DBSPType getAggregatedValueType() {
        return this.getAggregatedValue().getType();
    }

    void setResult(AggregateBase result) {
        this.result = result;
        if (!this.isWindowAggregate())
            this.result.validate();
    }

    public DBSPExpression aggregateOperation(
            CalciteObject node, DBSPOpcode op,
            DBSPType type, DBSPExpression left, DBSPExpression right, @Nullable DBSPExpression filter) {
        Utilities.enforce(op.isAggregate);
        DBSPType leftType = left.getType();
        DBSPType rightType = right.getType();
        DBSPType resultType = type.withMayBeNull(leftType.mayBeNull || rightType.mayBeNull);
        Utilities.enforce(type.sameType(leftType));
        // For all types the nullability of leftType and rightType may differ.
        // For recursive types the element types of leftType and rightType may be wrong from Calcite,
        // because Calcite sometimes assumes (but not always!) that field types are always nullable.
        if (!leftType.withMayBeNull(rightType.mayBeNull).sameType(rightType)) {
            if (!rightType.is(DBSPTypeBaseType.class)) {
                // These can also be different DECIMAL types
                right = right.applyCloneIfNeeded().cast(node, leftType.withMayBeNull(rightType.mayBeNull), false);
            }
        }

        return new DBSPConditionalAggregateExpression(
                node, op, resultType, left.applyCloneIfNeeded(), right.applyCloneIfNeeded(), filter);
    }

    void processMinMax(SqlMinMaxAggFunction function) {
        DBSPExpression zero = DBSPLiteral.none(this.nullableResultType);
        DBSPOpcode call;
        boolean isMin = true;
        String semigroupName = switch (function.getKind()) {
            case MIN -> {
                call = DBSPOpcode.AGG_MIN;
                yield "MinSemigroup";
            }
            case MAX -> {
                call = DBSPOpcode.AGG_MAX;
                isMin = false;
                yield "MaxSemigroup";
            }
            default -> throw new UnimplementedException("Aggregate function not yet implemented", node);
        };
        DBSPExpression aggregatedValue = this.getAggregatedValue();
        DBSPVariablePath accumulator = this.nullableResultType.var();
        DBSPExpression increment = this.aggregateOperation(
                node, call, this.nullableResultType, accumulator, aggregatedValue, this.filterArgument());
        DBSPType semigroup = new DBSPTypeUser(node, SEMIGROUP, semigroupName, false, accumulator.getType());
        // If there is a filter, do not use a MinMaxAggregate
        NonLinearAggregate aggregate;
        if (this.filterArgument >= 0)
            aggregate = new NonLinearAggregate(
                    node, zero, this.makeRowClosure(increment, accumulator), zero, semigroup);
        else
            aggregate = new MinMaxAggregate(
                    node, zero, this.makeRowClosure(increment, accumulator),
                    zero, semigroup, aggregatedValue, isMin);
        this.setResult(aggregate);
    }

    void processSum(SqlSumAggFunction function) {
        DBSPExpression zero = DBSPLiteral.none(this.nullableResultType);

        if (this.linearAllowed) {
            DBSPClosureExpression map;
            DBSPClosureExpression post;
            // map = |v| {
            //     ( if filter(v) && !v.field.is_null() { cast_non_null(v.field) } else { 0 },
            //       if filter(v) && !v.field.is_null() { 1 } else { 0 },
            //       1 )}
            DBSPExpression one = new DBSPI64Literal(1);
            DBSPExpression realZero = new DBSPI64Literal(0);
            DBSPExpression typedZero = this.getAggregatedValueType()
                    .withMayBeNull(false).to(IsNumericType.class).getZero();
            DBSPExpression agg = this.getAggregatedValue().is_null().not();

            DBSPExpression filter = this.filterArgument();
            DBSPExpression condition;
            if (filter != null)
                condition = ExpressionCompiler.makeBinaryExpression(
                        this.node, DBSPTypeBool.create(false), DBSPOpcode.AND, filter, agg);
            else
                condition = agg;
            DBSPExpression first = new DBSPIfExpression(
                    this.node, condition,
                    this.getAggregatedValue().cast(this.node, typedZero.getType(), false),
                    typedZero);
            DBSPExpression second = new DBSPIfExpression(node, condition, one, realZero);
            DBSPExpression mapBody = new DBSPTupleExpression(first, second, one);
            DBSPVariablePath postVar = mapBody.getType().var();
            // post = |x| if (x.1 != 0) { Some(x.0) } else { None }
            DBSPExpression postBody = new DBSPIfExpression(node,
                    ExpressionCompiler.makeBinaryExpression(node,
                            DBSPTypeBool.create(false), DBSPOpcode.NEQ, postVar.field(1), realZero),
                    postVar.field(0).cast(this.node, this.nullableResultType, false), zero);
            post = postBody.closure(postVar);
            map = mapBody.closure(this.v);
            this.setResult(new LinearAggregate(node, map, post, zero));
        } else {
            DBSPExpression increment;
            DBSPExpression aggregatedValue = this.getAggregatedValue();
            DBSPVariablePath accumulator = this.nullableResultType.var();

            DBSPExpression weighted = new DBSPBinaryExpression(node,
                    aggregatedValue.getType(), DBSPOpcode.MUL_WEIGHT,
                    aggregatedValue, this.compiler.weightVar);
            increment = this.aggregateOperation(
                    node, DBSPOpcode.AGG_ADD, this.nullableResultType,
                    accumulator, weighted, this.filterArgument());
            DBSPType semigroup = new DBSPTypeUser(CalciteObject.EMPTY, SEMIGROUP, "DefaultOptSemigroup",
                    false, accumulator.getType().withMayBeNull(false));
            this.setResult(new NonLinearAggregate(
                    node, zero, this.makeRowClosure(increment, accumulator), zero, semigroup));
        }
    }

    void processSumZero(SqlSumEmptyIsZeroAggFunction function) {
        DBSPExpression zero = this.resultType.to(IsNumericType.class).getZero();
        DBSPExpression increment;
        DBSPExpression aggregatedValue = this.getAggregatedValue();
        DBSPVariablePath accumulator = this.resultType.var();
        if (this.linearAllowed) {
            DBSPClosureExpression map;
            DBSPClosureExpression post;
            // map = |v| ( if filter(v) && !v.field.is_null() { v.field } else { 0 }, 1)
            DBSPExpression one = new DBSPI64Literal(1);
            DBSPExpression agg = this.getAggregatedValue().is_null().not();

            DBSPExpression filter = this.filterArgument();
            DBSPExpression condition;
            if (filter != null)
                condition = ExpressionCompiler.makeBinaryExpression(
                        this.node, DBSPTypeBool.create(false), DBSPOpcode.AND, filter, agg);
            else
                condition = agg;
            DBSPExpression first = new DBSPIfExpression(
                    this.node, condition, this.getAggregatedValue().cast(this.node, zero.getType(), false), zero);
            DBSPExpression mapBody = new DBSPTupleExpression(first, one);
            DBSPVariablePath postVar = mapBody.getType().var();
            // post = |x| x.0
            post = postVar.field(0).closure(postVar);
            map = mapBody.closure(this.v);
            this.setResult(new LinearAggregate(this.node, map, post, zero));
        } else {
            DBSPExpression weighted = new DBSPBinaryExpression(
                    this.node, aggregatedValue.getType(),
                    DBSPOpcode.MUL_WEIGHT, aggregatedValue, this.compiler.weightVar);
            increment = this.aggregateOperation(
                    this.node, DBSPOpcode.AGG_ADD_NON_NULL, this.resultType,
                    accumulator, weighted, this.filterArgument());
            String semigroupName = "DefaultSemigroup";
            if (accumulator.getType().mayBeNull)
                semigroupName = "DefaultOptSemigroup";
            DBSPType semigroup = new DBSPTypeUser(this.node, SEMIGROUP, semigroupName, false,
                    accumulator.getType().withMayBeNull(false));
            this.setResult(new NonLinearAggregate(
                    this.node, zero, this.makeRowClosure(increment, accumulator), zero, semigroup));
        }
    }

    void processSingle(SqlSingleValueAggFunction function) {
        DBSPExpression zero = DBSPLiteral.none(this.nullableResultType);
        DBSPExpression aggregatedValue = this.getAggregatedValue();
        if (this.filterArgument >= 0) {
            throw new UnimplementedException("SINGLE aggregation function with FILTER not yet implemented", node);
        }
        DBSPVariablePath accumulator = this.nullableResultType.var();
        // Single is supposed to be applied to a single value, and should return a runtime
        // error otherwise.  We approximate this behavior by returning the last value seen.
        DBSPExpression increment = aggregatedValue;
        if (!increment.getType().mayBeNull)
            increment = increment.applyCloneIfNeeded().some();
        DBSPType semigroup = new DBSPTypeUser(CalciteObject.EMPTY, SEMIGROUP, "UnimplementedSemigroup",
                false, accumulator.getType());
        this.setResult(new NonLinearAggregate(
                node, zero, this.makeRowClosure(increment, accumulator), zero, semigroup));
    }

    AggregateBase doAverage(SqlAvgAggFunction function) {
        Utilities.enforce(function.getKind() == SqlKind.AVG);
        DBSPExpression postZero = DBSPLiteral.none(this.nullableResultType);

        if (this.linearAllowed) {
            DBSPClosureExpression map;
            DBSPClosureExpression post;
            // map = |v| {
            //     ( if filter(v) && !v.field.is_null() { cast_non_null(v.field) } else { 0 },
            //       if filter(v) && !v.field.is_null() { 1 } else { 0 },
            //       1 )}
            IsNumericType nonNullAggregateType = this.getAggregatedValueType()
                    .withMayBeNull(false)
                    .to(IsNumericType.class);
            DBSPExpression one = nonNullAggregateType.getOne();
            DBSPExpression typedZero = nonNullAggregateType.getZero();
            DBSPExpression agg = this.getAggregatedValue().is_null().not();

            DBSPExpression filter = this.filterArgument();
            DBSPExpression condition;
            if (filter != null)
                condition = ExpressionCompiler.makeBinaryExpression(
                        node, DBSPTypeBool.create(false), DBSPOpcode.AND, filter, agg);
            else
                condition = agg;
            DBSPExpression first = new DBSPIfExpression(
                    node, condition,
                    this.getAggregatedValue().cast(this.node, typedZero.getType(), false),
                    typedZero);
            DBSPExpression second = new DBSPIfExpression(node, condition, one, typedZero);
            DBSPExpression mapBody = new DBSPTupleExpression(first, second, one);
            DBSPVariablePath postVar = mapBody.getType().var();
            // post = |x| if (x.1 != 0) { Some(x.0 / x.1) } else { None }
            DBSPExpression div = ExpressionCompiler.makeBinaryExpression(node,
                    this.resultType, DBSPOpcode.DIV, postVar.field(0), postVar.field(1));
            DBSPExpression postBody = new DBSPIfExpression(node,
                    ExpressionCompiler.makeBinaryExpression(node,
                            DBSPTypeBool.create(false), DBSPOpcode.NEQ, postVar.field(1), typedZero),
                    div.cast(this.node, this.nullableResultType, false), postZero);
            post = postBody.closure(postVar);
            map = mapBody.closure(this.v);
            return new LinearAggregate(node, map, post, postZero);
        } else {
            DBSPType aggregatedValueType = this.getAggregatedValueType();
            DBSPType intermediateResultType = aggregatedValueType.withMayBeNull(true);
            DBSPExpression zero = new DBSPTupleExpression(
                    DBSPLiteral.none(intermediateResultType), DBSPLiteral.none(intermediateResultType));
            DBSPType pairType = zero.getType();
            DBSPExpression count, sum;
            DBSPVariablePath accumulator = pairType.var();
            final int sumIndex = 0;
            final int countIndex = 1;
            DBSPExpression countAccumulator = accumulator.field(countIndex);
            DBSPExpression sumAccumulator = accumulator.field(sumIndex);
            DBSPExpression aggregatedValue = this.getAggregatedValue().cast(this.node, intermediateResultType, false);
            DBSPType intermediateResultTypeNonNull = intermediateResultType.withMayBeNull(false);
            DBSPExpression plusOne = intermediateResultTypeNonNull.to(IsNumericType.class).getOne();

            if (aggregatedValueType.mayBeNull)
                plusOne = ExpressionCompiler.makeIndicator(
                        node, intermediateResultTypeNonNull, aggregatedValue);
            DBSPExpression weightedCount = new DBSPBinaryExpression(
                    node, intermediateResultType.withMayBeNull(plusOne.getType().mayBeNull),
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
            DBSPExpression increment = new DBSPTupleExpression(sum, count);

            DBSPVariablePath a = pairType.var();
            DBSPExpression divide = ExpressionCompiler.makeBinaryExpression(
                    node, this.resultType, DBSPOpcode.DIV,
                    a.field(sumIndex), a.field(countIndex));
            divide = divide.cast(this.node, this.nullableResultType, false);
            DBSPClosureExpression post = new DBSPClosureExpression(
                    node, divide, a.asParameter());
            DBSPType semigroup = new DBSPTypeUser(node, SEMIGROUP, "PairSemigroup", false,
                    intermediateResultType, intermediateResultType,
                    new DBSPTypeUser(node, SEMIGROUP, "DefaultOptSemigroup", false, intermediateResultTypeNonNull),
                    new DBSPTypeUser(node, SEMIGROUP, "DefaultOptSemigroup", false, intermediateResultTypeNonNull));
            return new NonLinearAggregate(
                    node, zero, this.makeRowClosure(increment, accumulator), post, postZero, semigroup);
        }
    }

    NonLinearAggregate doStddev(SqlAvgAggFunction function) {
        Utilities.enforce(function.getKind() == SqlKind.STDDEV_POP || function.getKind() == SqlKind.STDDEV_SAMP);
        boolean isSamp = function.getKind() == SqlKind.STDDEV_SAMP;
        DBSPType aggregatedValueType = this.getAggregatedValueType();
        // TODO: linear implementation
        DBSPType intermediateResultType = aggregatedValueType.withMayBeNull(true);

        if (intermediateResultType.is(DBSPTypeDecimal.class)) {
            intermediateResultType = new DBSPTypeRuntimeDecimal(
                    intermediateResultType.getNode(), intermediateResultType.mayBeNull);
        }

        // Compute 3 sums: Sum(value^2), Sum(value), Count(value)
        DBSPExpression zero = new DBSPTupleExpression(
                DBSPLiteral.none(intermediateResultType),
                DBSPLiteral.none(intermediateResultType),
                DBSPLiteral.none(intermediateResultType));
        DBSPType tripleType = zero.getType();
        DBSPExpression count, sum, sumSquares;
        DBSPVariablePath accumulator = tripleType.var();
        final int sumIndex = 0;
        final int countIndex = 1;
        final int sumSquaresIndex = 2;
        DBSPExpression countAccumulator = accumulator.field(countIndex);
        DBSPExpression sumAccumulator = accumulator.field(sumIndex);
        DBSPExpression sumSquaresAccumulator = accumulator.field(sumSquaresIndex);

        DBSPExpression aggregatedValue = this.getAggregatedValue().cast(this.node, intermediateResultType, false);
        DBSPType intermediateResultTypeNonNull = intermediateResultType.withMayBeNull(false);
        DBSPExpression plusOne = intermediateResultTypeNonNull.to(IsNumericType.class).getOne();

        if (aggregatedValueType.mayBeNull)
            plusOne = ExpressionCompiler.makeIndicator(node, intermediateResultTypeNonNull, aggregatedValue);
        DBSPExpression weightedCount = new DBSPBinaryExpression(
                node, intermediateResultType.withMayBeNull(plusOne.getType().mayBeNull),
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
        DBSPExpression increment = new DBSPTupleExpression(sum, count, sumSquares);

        DBSPVariablePath a = tripleType.var();
        DBSPExpression sumSquared = ExpressionCompiler.makeBinaryExpression(
                node, intermediateResultType, DBSPOpcode.MUL,
                a.field(sumIndex), a.field(sumIndex));
        DBSPExpression normalized = ExpressionCompiler.makeBinaryExpression(
                node, intermediateResultType, DBSPOpcode.DIV,
                sumSquared, a.field(countIndex));
        DBSPExpression sub = ExpressionCompiler.makeBinaryExpression(
                node, intermediateResultType, DBSPOpcode.SUB,
                a.field(sumSquaresIndex), normalized);

        DBSPExpression denom = isSamp ? ExpressionCompiler.makeBinaryExpression(
                node, intermediateResultType, DBSPOpcode.SUB,
                a.field(countIndex), intermediateResultType.to(IsNumericType.class).getOne()) :
                a.field(countIndex);
        // We need to call sqrt, which only works for doubles.
        DBSPType sqrtType = new DBSPTypeDouble(node, intermediateResultType.mayBeNull);
        DBSPExpression div = ExpressionCompiler.makeBinaryExpression(
                node, intermediateResultType, DBSPOpcode.DIV_NULL,
                sub, denom).cast(this.node, sqrtType, false);
        // Prevent sqrt from negative values if computations are unstable
        DBSPExpression max = ExpressionCompiler.makeBinaryExpression(
                node, sqrtType, DBSPOpcode.MAX,
                div, sqrtType.to(IsNumericType.class).getZero());
        DBSPExpression sqrt = ExpressionCompiler.compilePolymorphicFunction(
                false, "sqrt", node, sqrtType, Linq.list(max), 1);
        sqrt = sqrt.cast(this.node, this.nullableResultType, false);
        DBSPClosureExpression post = new DBSPClosureExpression(node, sqrt, a.asParameter());
        DBSPExpression postZero = DBSPLiteral.none(this.nullableResultType);
        DBSPType semigroup = new DBSPTypeUser(node, SEMIGROUP, "TripleSemigroup", false,
                intermediateResultType, intermediateResultType, intermediateResultType,
                new DBSPTypeUser(node, SEMIGROUP, "DefaultOptSemigroup", false, intermediateResultTypeNonNull),
                new DBSPTypeUser(node, SEMIGROUP, "DefaultOptSemigroup", false, intermediateResultTypeNonNull),
                new DBSPTypeUser(node, SEMIGROUP, "DefaultOptSemigroup", false, intermediateResultTypeNonNull));
        return new NonLinearAggregate(
                node, zero, this.makeRowClosure(increment, accumulator), post, postZero, semigroup);
    }

    void processAvg(SqlAvgAggFunction function) {
        AggregateBase implementation = switch (function.getKind()) {
            case AVG -> this.doAverage(function);
            case STDDEV_POP, STDDEV_SAMP -> this.doStddev(function);
            default -> throw new UnimplementedException("Statistical aggregate function not yet implemented", 172, node);
        };
        this.setResult(implementation);
    }

    public AggregateBase compile() {
        boolean success =
                this.process(this.aggFunction, SqlCountAggFunction.class, this::processCount) ||
                this.process(this.aggFunction, SqlBasicAggFunction.class, this::processBasic) || // arg_max or array_agg
                this.process(this.aggFunction, SqlMinMaxAggFunction.class, this::processMinMax) ||
                this.process(this.aggFunction, SqlSumAggFunction.class, this::processSum) ||
                this.process(this.aggFunction, SqlSumEmptyIsZeroAggFunction.class, this::processSumZero) ||
                this.process(this.aggFunction, SqlAvgAggFunction.class, this::processAvg) || // handles stddev too
                this.process(this.aggFunction, SqlBitOpAggFunction.class, this::processBitOp) ||
                this.process(this.aggFunction, SqlSingleValueAggFunction.class, this::processSingle) ||
                this.process(this.aggFunction, SqlAbstractGroupFunction.class, this::processGrouping);
        if (!success || this.result == null)
            throw new UnimplementedException("Aggregate function " + this.aggFunction + " not yet implemented", 172,
                    CalciteObject.create(this.aggregateNode, this.call));
        return this.result;
    }

    @Override
    public DBSPCompiler compiler() {
        return this.compiler;
    }
}
