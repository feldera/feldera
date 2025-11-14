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

package org.dbsp.sqlCompiler.compiler.frontend.aggregates;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rex.RexLiteral;
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
import org.dbsp.sqlCompiler.compiler.frontend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.SqlUserDefinedAggregationFunction;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.ir.aggregate.IAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.LinearAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.MinMaxAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.NonLinearAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConditionalIncrementExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBinary;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeFP;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeNull;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeVec;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeWeight;
import org.dbsp.util.ICastable;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.INT64;
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
    private IAggregate result;
    /** The type used to accumulate partial results.
     * For integers this is a wider type than nullableResultType, for other types
     * it is nullableResultType. */
    public final DBSPType partialResultType;

    /** Expression that stands as a reference to the whole input row in the input zset. */
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
    private final ExpressionCompiler eComp;

    public AggregateCompiler(
            RelNode node,
            DBSPCompiler compiler,
            AggregateCall call,
            List<RexLiteral> constants,
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
        this.partialResultType = this.computePartialResultType(this.nullableResultType);
        List<Integer> argList = call.getArgList();
        this.eComp = new ExpressionCompiler(this.aggregateNode, this.v, constants, this.compiler);
        if (argList.isEmpty()) {
            this.aggArgument = null;
        } else if (argList.size() == 1) {
            int fieldNumber = call.getArgList().get(0);
            this.aggArgument = this.eComp.inputIndex(this.node, fieldNumber).applyCloneIfNeeded();
            if (this.aggArgument.getType().is(DBSPTypeNull.class)) {
                throw new CompilationError("Argument of aggregate has NULL type",
                        CalciteObject.create(call.getParserPosition()));
            }
        } else {
            List<DBSPExpression> fields = Linq.map(call.getArgList(),
                    a -> this.eComp.inputIndex(this.node, a).applyCloneIfNeeded());
            this.aggArgument = new DBSPTupleExpression(fields, false);
        }
    }

    DBSPType computePartialResultType(DBSPType type) {
        if (type.is(DBSPTypeInteger.class)) {
            DBSPTypeCode code = DBSPTypeInteger.largerSigned(type.code);
            return DBSPTypeInteger.getType(type.getNode(), code, type.mayBeNull);
        } else if (type.is(DBSPTypeDecimal.class)) {
            DBSPTypeDecimal decimal = type.to(DBSPTypeDecimal.class);
            int precision = decimal.precision;
            int scale = decimal.scale;
            int availableBits = DBSPTypeDecimal.MAX_PRECISION - precision;
            if (availableBits >= 2 * precision) {
                precision = 3 * precision;
                scale = Math.min(3 * scale, DBSPTypeDecimal.MAX_SCALE);
            } else {
                // Allocate proportionally
                if (scale != 0) {
                    scale = Math.min(
                            (int)(scale * (double)DBSPTypeDecimal.MAX_PRECISION / precision),
                            DBSPTypeDecimal.MAX_SCALE);
                }
                precision = DBSPTypeDecimal.MAX_PRECISION;
            }
            return new DBSPTypeDecimal(type.getNode(), precision, scale, type.mayBeNull);
        }
        return type;
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
        increment = this.incrementOperation(this.node, opcode,
                this.nullableResultType, accumulator, aggregatedValue, this.filterArgument());
        DBSPTypeUser semigroup = new DBSPTypeUser(CalciteObject.EMPTY, SEMIGROUP, "UnimplementedSemigroup",
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
        DBSPTypeUser semigroup = new DBSPTypeUser(CalciteObject.EMPTY, SEMIGROUP, "UnimplementedSemigroup",
                false, accumulator.getType());
        // Always non-linear (result can not be zero).
        this.setResult(new NonLinearAggregate(
                this.node, zero, this.makeRowClosure(increment, accumulator), zero, semigroup));
    }

    boolean fp() {
        return this.resultType.is(DBSPTypeFP.class);
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
            increment = this.incrementOperation(
                    node, DBSPOpcode.AGG_ADD, this.resultType,
                    accumulator, weighted, this.filterArgument());
            DBSPTypeUser semigroup = new DBSPTypeUser(node, SEMIGROUP, "DefaultSemigroup", false, this.resultType);
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
        // The result type is ARRAY, but the accumulator is just Vec.
        DBSPTypeArray arrayType = this.resultType.to(DBSPTypeArray.class).to(DBSPTypeArray.class);
        DBSPType elementType = arrayType.getElementType();
        DBSPTypeTuple rowType = this.v.getType().deref().to(DBSPTypeTuple.class);

        // ORDER BY information for the ARRAY_AGG
        List<RelFieldCollation> collations = this.call.getCollation().getFieldCollations();
        // The accumulator array will only contain some fields from the source collection.
        // Rewrite the collations to be relative to these fields.
        List<RelFieldCollation> compressedCollations = new ArrayList<>();

        // We need to preserve all the fields that are being sorted on or which are in the output
        DBSPType vecElementType;
        DBSPExpression aggregatedValue;
        // Tracks the fields of the accumulator that have to be emitted in the output
        List<Integer> fieldProj = new ArrayList<>();
        if (collations.isEmpty()) {
            // No sorting: just accumulate the field that is in the result.
            vecElementType = arrayType.innerType().getElementType();
            aggregatedValue = this.getAggregatedValue();
        } else {
            Map<Integer, Integer> fieldMap = new HashMap<>();
            List<Integer> accumulatorFields = new ArrayList<>();
            List<DBSPType> fieldTypes = new ArrayList<>();
            // First add to accumulator the fields that are sorted on
            for (RelFieldCollation col: collations) {
                int index = col.getFieldIndex();
                if (fieldMap.containsKey(index))
                    // Sorting the second time on a field is a no-op
                    continue;

                int newIndex = fieldMap.size();
                RelFieldCollation newCollation = new RelFieldCollation(newIndex, col.direction, col.nullDirection);
                compressedCollations.add(newCollation);
                Utilities.putNew(fieldMap, index, newIndex);

                accumulatorFields.add(index);
                fieldTypes.add(rowType.tupFields[index]);
            }

            // Then add the fields that are in the output, if they aren't there already
            for (int index: call.getArgList()) {
                if (fieldMap.containsKey(index)) {
                    fieldProj.add(Utilities.getExists(fieldMap, index));
                    continue;
                }

                int newIndex = fieldMap.size();
                Utilities.putNew(fieldMap, index, newIndex);
                accumulatorFields.add(index);
                fieldTypes.add(rowType.tupFields[index]);
                fieldProj.add(newIndex);
            }

            vecElementType = new DBSPTypeTuple(fieldTypes);
            List<DBSPExpression> eFields = Linq.map(accumulatorFields,
                    a -> this.eComp.inputIndex(this.node, a).applyCloneIfNeeded());
            aggregatedValue = new DBSPTupleExpression(eFields, false);
        }

        DBSPTypeVec accumulatorType = new DBSPTypeVec(vecElementType, arrayType.mayBeNull);
        DBSPExpression empty = DBSPArrayExpression.emptyWithElementType(elementType, this.resultType.mayBeNull);
        DBSPExpression zero = accumulatorType.emptyVector();
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
        arguments[1] = ExpressionCompiler.expandTuple(this.node, aggregatedValue);
        arguments[2] = this.compiler.weightVar;
        arguments[3] = new DBSPBoolLiteral(distinct);
        arguments[4] = this.filterArgument >= 0 ? this.filterArgument() : new DBSPBoolLiteral(true);
        if (arguments.length == 6) {
            arguments[5] = new DBSPBoolLiteral(ignoreNulls);
        }
        DBSPExpression increment = new DBSPApplyExpression(
                node, functionName, DBSPTypeVoid.INSTANCE, arguments);
        DBSPTypeUser semigroup = new DBSPTypeUser(
                node, SEMIGROUP, "ConcatSemigroup", false, accumulatorType);
        DBSPVariablePath p = accumulatorType.var();
        DBSPClosureExpression post;
        if (collations.isEmpty()) {
            String convertName = "to_array";
            if (arrayType.mayBeNull)
                convertName += "N";
            post = new DBSPApplyExpression(convertName, arrayType, p).closure(p);
        } else {
            String convertName = "sort_to_array";
            if (arrayType.mayBeNull)
                convertName += "N";
            DBSPExpression comparator = CalciteToDBSPCompiler.generateComparator(
                    this.node, compressedCollations, vecElementType, false);
            DBSPVariablePath var = vecElementType.var();
            DBSPClosureExpression projector;
            Utilities.enforce(!fieldProj.isEmpty());
            if (fieldProj.size() > 1) {
                List<DBSPExpression> fields = new ArrayList<>();
                for (Integer f : fieldProj) {
                    fields.add(var.field(f).applyCloneIfNeeded());
                }
                projector = new DBSPTupleExpression(fields, elementType.mayBeNull).closure(var);
            } else {
                projector = var.field(fieldProj.get(0)).applyCloneIfNeeded().closure(var);
            }
            post = new DBSPApplyExpression(convertName, arrayType, p, comparator, projector).closure(p);
        }
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
        MinMaxAggregate.Operation operation;
        DBSPOpcode opcode;
        String semigroupName;
        switch (kind) {
            case ARG_MAX:
                opcode = DBSPOpcode.AGG_MAX1;
                operation = MinMaxAggregate.Operation.ArgMax;
                semigroupName = "MaxSemigroup";
                break;
            case ARG_MIN:
                opcode = DBSPOpcode.AGG_MIN1;
                operation = MinMaxAggregate.Operation.ArgMin;
                semigroupName = "MinSemigroup";
                break;
            default:
                throw new UnimplementedException("Aggregate function not yet implemented", node);
        }

        DBSPTupleExpression tuple = this.getAggregatedValue().to(DBSPTupleExpression.class);
        Utilities.enforce(tuple.fields != null && tuple.fields.length == 2,
                () -> "Expected 2 arguments for " + kind);
        NonLinearAggregate aggregate;
        // Must compare first on second field
        DBSPTypeTuple dataType = tuple.getTypeAsTuple();
        DBSPTypeRawTuple accumulatorType = new DBSPTypeRawTuple(
                tuple.getNode(),
                Linq.list(
                        dataType.getFieldType(1).withMayBeNull(true),
                        this.resultType));
        DBSPExpression zero = new DBSPRawTupleExpression(
                accumulatorType.tupFields[0].none(),
                accumulatorType.tupFields[1].defaultValue());
        DBSPExpression aggregatedValue =
            new DBSPRawTupleExpression(
                    ExpressionCompiler.expandTuple(node, tuple.fields[1]),
                    ExpressionCompiler.expandTuple(node, tuple.fields[0]));
        DBSPVariablePath accumulator = accumulatorType.var();
        DBSPExpression increment = this.incrementOperation(
                node, opcode, accumulatorType, accumulator, aggregatedValue, this.filterArgument());
        DBSPTypeUser semigroup = new DBSPTypeUser(node, SEMIGROUP, semigroupName, false, accumulatorType);
        DBSPClosureExpression postProcessing = ExpressionCompiler.expandTuple(node, accumulator.field(1))
                .closure(accumulator);

        if (this.filterArgument >= 0) {
            aggregate = new NonLinearAggregate(
                    node, zero,
                    this.makeRowClosure(increment, accumulator),
                    postProcessing,
                    this.resultType.defaultValue(),
                    semigroup);
        } else {
            aggregate = new MinMaxAggregate(
                    node, zero, this.makeRowClosure(increment, accumulator),
                    this.resultType.defaultValue(), semigroup,
                    aggregatedValue.closure(this.v), postProcessing, operation);
        }

        this.setResult(aggregate);
    }

    private DBSPExpression getAggregatedValue() {
        return Objects.requireNonNull(this.aggArgument);
    }

    private DBSPType getAggregatedValueType() {
        return this.getAggregatedValue().getType();
    }

    void setResult(IAggregate result) {
        this.result = result;
        if (!this.isWindowAggregate())
            this.result.validate();
    }

    public DBSPExpression incrementOperation(
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

        return new DBSPConditionalIncrementExpression(
                node, op, resultType, left.applyCloneIfNeeded(), right.applyCloneIfNeeded(), filter);
    }

    void processMinMax(SqlMinMaxAggFunction function) {
        DBSPOpcode opcode;
        MinMaxAggregate.Operation operation;
        String semigroupName = switch (function.getKind()) {
            case MIN -> {
                operation = MinMaxAggregate.Operation.Min;
                opcode = DBSPOpcode.AGG_MIN;
                yield "MinSemigroup";
            }
            case MAX -> {
                opcode = DBSPOpcode.AGG_MAX;
                operation = MinMaxAggregate.Operation.Max;
                yield "MaxSemigroup";
            }
            default -> throw new UnimplementedException("Aggregate function not yet implemented", node);
        };
        DBSPExpression zero = DBSPLiteral.none(this.nullableResultType);
        DBSPExpression aggregatedValue = ExpressionCompiler.expandTuple(node, this.getAggregatedValue());
        DBSPVariablePath accumulator = this.nullableResultType.var();
        DBSPExpression increment = this.incrementOperation(
                node, opcode, this.nullableResultType, accumulator, aggregatedValue, this.filterArgument());
        DBSPTypeUser semigroup = new DBSPTypeUser(node, SEMIGROUP, semigroupName, false, accumulator.getType());
        // If there is a filter, do not use a MinMaxAggregate
        NonLinearAggregate aggregate;
        if (this.filterArgument >= 0)
            aggregate = new NonLinearAggregate(
                    node, zero, this.makeRowClosure(increment, accumulator), zero, semigroup);
        else
            aggregate = new MinMaxAggregate(
                    node, zero, this.makeRowClosure(increment, accumulator),
                    zero, semigroup, aggregatedValue.closure(this.v), null, operation);
        this.setResult(aggregate);
    }

    void processSum(SqlSumAggFunction function) {
        DBSPExpression zero = DBSPLiteral.none(this.nullableResultType);

        if (this.linearAllowed && !this.fp()) {
            DBSPClosureExpression map;
            DBSPClosureExpression post;
            // map = |v| {
            //     ( if filter(v) && !v.field.is_null() { cast(v.field, intermediate_type) } else { 0 },
            //       if filter(v) && !v.field.is_null() { 1 } else { 0 },
            //       1 )}
            DBSPExpression one = new DBSPI64Literal(1);
            DBSPExpression realZero = new DBSPI64Literal(0);
            DBSPExpression typedZero = this.partialResultType
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
            Simplify simplify = new Simplify(this.compiler);
            mapBody = simplify.apply(mapBody).to(DBSPExpression.class);
            DBSPVariablePath postVar = mapBody.getType().var();
            // post = |x| if (x.1 != 0) { cast(Some(x.0), result_type) } else { None }
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
            DBSPVariablePath accumulator = this.partialResultType.var();

            DBSPExpression weighted = new DBSPBinaryExpression(node,
                    this.partialResultType, DBSPOpcode.MUL_WEIGHT,
                    aggregatedValue.cast(this.node, this.partialResultType, false), this.compiler.weightVar);
            increment = this.incrementOperation(
                    node, DBSPOpcode.AGG_ADD, this.partialResultType,
                    accumulator, weighted, this.filterArgument());
            DBSPTypeUser semigroup = new DBSPTypeUser(CalciteObject.EMPTY, SEMIGROUP, "DefaultOptSemigroup",
                    false, this.partialResultType.withMayBeNull(false));
            DBSPVariablePath postVar = this.partialResultType.var();
            DBSPClosureExpression postProcess = postVar.cast(this.node, this.nullableResultType, false).closure(postVar);
            this.setResult(new NonLinearAggregate(
                    node, DBSPLiteral.none(this.partialResultType), this.makeRowClosure(increment, accumulator), postProcess, zero, semigroup));
        }
    }

    void processSumZero(SqlSumEmptyIsZeroAggFunction function) {
        DBSPExpression zero = this.resultType.to(IsNumericType.class).getZero();
        DBSPExpression increment;
        DBSPExpression aggregatedValue = this.getAggregatedValue();
        if (this.linearAllowed && !this.fp()) {
            DBSPExpression typedZero = this.partialResultType
                    .withMayBeNull(false).to(IsNumericType.class).getZero();
            DBSPClosureExpression map;
            DBSPClosureExpression post;
            // map = |v| ( if filter(v) && !v.field.is_null() { cast(v.field, intermediate_type) } else { 0 }, 1)
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
                    this.node, condition, this.getAggregatedValue().cast(this.node, typedZero.getType(), false),
                    typedZero);
            DBSPExpression mapBody = new DBSPTupleExpression(first, one);
            DBSPVariablePath postVar = mapBody.getType().var();
            // post = |x| cast(x.0, result_type)
            post = postVar.field(0).cast(this.node, this.resultType, false).closure(postVar);
            map = mapBody.closure(this.v);
            this.setResult(new LinearAggregate(this.node, map, post, zero));
        } else {
            DBSPExpression typedZero = this.partialResultType
                    .withMayBeNull(false).to(IsNumericType.class).getZero();
            DBSPVariablePath accumulator = this.partialResultType.withMayBeNull(false).var();
            DBSPExpression weighted = new DBSPBinaryExpression(
                    this.node, this.partialResultType,
                    DBSPOpcode.MUL_WEIGHT, aggregatedValue.cast(this.node, this.partialResultType, false),
                    this.compiler.weightVar);
            increment = this.incrementOperation(
                    this.node, DBSPOpcode.AGG_ADD_NON_NULL, accumulator.getType(),
                    accumulator, weighted, this.filterArgument());
            String semigroupName = "DefaultSemigroup";
            if (accumulator.getType().mayBeNull)
                semigroupName = "DefaultOptSemigroup";
            DBSPTypeUser semigroup = new DBSPTypeUser(this.node, SEMIGROUP, semigroupName, false,
                    accumulator.getType().withMayBeNull(false));
            DBSPVariablePath postVar = accumulator.getType().var();
            // post = |x| cast(accumulator, result_type)
            DBSPClosureExpression post = postVar.cast(this.node, this.resultType, false).closure(postVar);
            this.setResult(new NonLinearAggregate(
                    this.node, typedZero, this.makeRowClosure(increment, accumulator), post, zero, semigroup));
        }
    }

    void processSingle(SqlSingleValueAggFunction function) {
        DBSPExpression postZero = DBSPLiteral.none(this.nullableResultType);
        // (false, null)
        DBSPExpression zero = new DBSPTupleExpression(new DBSPBoolLiteral(false), postZero);
        DBSPExpression aggregatedValue = this.getAggregatedValue();
        if (this.filterArgument >= 0) {
            throw new UnimplementedException("SINGLE aggregation function with FILTER not yet implemented", node);
        }
        DBSPTypeBool bool = DBSPTypeBool.create(false);
        DBSPVariablePath accumulator = new DBSPTypeTuple(bool, this.nullableResultType).var();
        // Single is supposed to be applied to a single value, and should return a runtime
        // error otherwise.
        DBSPExpression increment = aggregatedValue.applyCloneIfNeeded();
        if (!increment.getType().mayBeNull)
            increment = increment.some();
        DBSPExpression panic = DBSPApplyExpression.panic(this.node, bool,"More than one value in subquery");
        // accumulator = Tup2::new(if (accumulator.0 || weight > 1) { panic } else { true }, aggregatedValue)
        DBSPExpression compareWeight = new DBSPBinaryExpression(
                this.node, bool, DBSPOpcode.GT, this.compiler.weightVar, DBSPTypeWeight.INSTANCE.one());
        DBSPExpression condition = new DBSPBinaryExpression(this.node, bool,
                DBSPOpcode.OR, compareWeight, accumulator.field(0));
        DBSPExpression ifExpr = new DBSPIfExpression(this.node, condition,
                panic, new DBSPBoolLiteral(true));
        increment = new DBSPTupleExpression(ifExpr, increment);
        DBSPVariablePath postVar = increment.type.var();
        DBSPClosureExpression post = postVar.field(1).applyCloneIfNeeded().closure(postVar);

        DBSPTypeUser semigroup = new DBSPTypeUser(CalciteObject.EMPTY, SEMIGROUP, "SingleSemigroup",
                false, accumulator.getType());
        var nonLinear = new NonLinearAggregate(
                node, zero, this.makeRowClosure(increment, accumulator), post, postZero, semigroup);
        this.setResult(nonLinear);
    }

    IAggregate doAverage(SqlAvgAggFunction function) {
        Utilities.enforce(function.getKind() == SqlKind.AVG);
        DBSPExpression postZero = DBSPLiteral.none(this.nullableResultType);

        if (this.linearAllowed && !this.fp()) {
            DBSPClosureExpression map;
            DBSPClosureExpression post;
            // map = |v| {
            //     ( if filter(v) && !v.field.is_null() { cast(v.field, intermediate_type) } else { 0 },
            //       if filter(v) && !v.field.is_null() { 1 } else { 0 },
            //       1 )}
            IsNumericType nonNullAggregateType = this.partialResultType
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
            // post = |x| if (x.1 != 0) { cast(Some(x.0 / x.1), result_type) } else { None }
            DBSPExpression div = ExpressionCompiler.makeBinaryExpression(node,
                    this.partialResultType, DBSPOpcode.DIV, postVar.field(0), postVar.field(1));
            DBSPExpression postBody = new DBSPIfExpression(node,
                    ExpressionCompiler.makeBinaryExpression(node,
                            DBSPTypeBool.create(false), DBSPOpcode.NEQ, postVar.field(1), typedZero),
                    div.cast(this.node, this.nullableResultType, false), postZero);
            post = postBody.closure(postVar);
            map = mapBody.closure(this.v);
            return new LinearAggregate(node, map, post, postZero);
        } else {
            DBSPType aggregatedValueType = this.partialResultType;
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
            count = this.incrementOperation(
                    node, DBSPOpcode.AGG_ADD, intermediateResultType,
                    countAccumulator, weightedCount, this.filterArgument());
            DBSPExpression weightedSum = new DBSPBinaryExpression(
                    node, intermediateResultType, DBSPOpcode.MUL_WEIGHT,
                    aggregatedValue, this.compiler.weightVar);
            sum = this.incrementOperation(
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
            DBSPTypeUser semigroup = new DBSPTypeUser(node, SEMIGROUP, "PairSemigroup", false,
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
        DBSPType intermediateResultType = this.partialResultType.withMayBeNull(true);

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
            plusOne = ExpressionCompiler.makeIndicator(this.node, intermediateResultTypeNonNull, aggregatedValue);
        DBSPExpression weightedCount = new DBSPBinaryExpression(
                this.node, intermediateResultType.withMayBeNull(plusOne.getType().mayBeNull),
                DBSPOpcode.MUL_WEIGHT, plusOne,
                this.compiler.weightVar);
        count = this.incrementOperation(
                this.node, DBSPOpcode.AGG_ADD, intermediateResultType,
                countAccumulator, weightedCount, this.filterArgument());

        DBSPExpression weightedElement = new DBSPBinaryExpression(
                this.node, intermediateResultType, DBSPOpcode.MUL_WEIGHT,
                aggregatedValue, this.compiler.weightVar);
        sum = this.incrementOperation(
                this.node, DBSPOpcode.AGG_ADD, intermediateResultType,
                sumAccumulator, weightedElement, this.filterArgument());

        DBSPExpression weightedSq = ExpressionCompiler.makeBinaryExpression(
                this.node, intermediateResultType, DBSPOpcode.MUL,
                aggregatedValue, weightedElement);
        sumSquares = this.incrementOperation(
                this.node, DBSPOpcode.AGG_ADD,
                intermediateResultType, sumSquaresAccumulator, weightedSq,
                this.filterArgument());
        DBSPExpression increment = new DBSPTupleExpression(sum, count, sumSquares);

        DBSPVariablePath a = tripleType.var();
        DBSPExpression sumSquared = ExpressionCompiler.makeBinaryExpression(
                this.node, intermediateResultType, DBSPOpcode.MUL,
                a.field(sumIndex), a.field(sumIndex));
        DBSPExpression normalized = ExpressionCompiler.makeBinaryExpression(
                this.node, intermediateResultType, DBSPOpcode.DIV,
                sumSquared, a.field(countIndex));
        DBSPExpression sub = ExpressionCompiler.makeBinaryExpression(
                this.node, intermediateResultType, DBSPOpcode.SUB,
                a.field(sumSquaresIndex), normalized);

        DBSPExpression denom = isSamp ? ExpressionCompiler.makeBinaryExpression(
                this.node, intermediateResultType, DBSPOpcode.SUB,
                a.field(countIndex), intermediateResultType.to(IsNumericType.class).getOne()) :
                a.field(countIndex);
        // We need to call sqrt, which only works for doubles.
        DBSPType sqrtType = new DBSPTypeDouble(node, intermediateResultType.mayBeNull);
        DBSPExpression div = ExpressionCompiler.makeBinaryExpression(
                this.node, intermediateResultType, DBSPOpcode.DIV_NULL,
                sub, denom).cast(this.node, sqrtType, false);
        // Prevent sqrt from negative values if computations are unstable
        DBSPExpression max = ExpressionCompiler.makeBinaryExpression(
                this.node, sqrtType, DBSPOpcode.MAX,
                div, sqrtType.to(IsNumericType.class).getZero());
        DBSPExpression sqrt = ExpressionCompiler.compilePolymorphicFunction(
                false, "sqrt", this.node, sqrtType, Linq.list(max), 1);
        sqrt = sqrt.cast(this.node, this.nullableResultType, false);
        DBSPClosureExpression post = new DBSPClosureExpression(node, sqrt, a.asParameter());
        DBSPExpression postZero = DBSPLiteral.none(this.nullableResultType);
        DBSPTypeUser semigroup = new DBSPTypeUser(node, SEMIGROUP, "TripleSemigroup", false,
                intermediateResultType, intermediateResultType, intermediateResultType,
                new DBSPTypeUser(this.node, SEMIGROUP, "DefaultOptSemigroup", false, intermediateResultTypeNonNull),
                new DBSPTypeUser(this.node, SEMIGROUP, "DefaultOptSemigroup", false, intermediateResultTypeNonNull),
                new DBSPTypeUser(this.node, SEMIGROUP, "DefaultOptSemigroup", false, intermediateResultTypeNonNull));
        return new NonLinearAggregate(
                this.node, zero, this.makeRowClosure(increment, accumulator), post, postZero, semigroup);
    }

    void processAvg(SqlAvgAggFunction function) {
        IAggregate implementation = switch (function.getKind()) {
            case AVG -> this.doAverage(function);
            case STDDEV_POP, STDDEV_SAMP -> this.doStddev(function);
            default -> throw new UnimplementedException("Statistical aggregate function not yet implemented", 172, node);
        };
        this.setResult(implementation);
    }

    // user-defined aggregates
    void processUDA(SqlUserDefinedAggregationFunction function) {
        ProgramIdentifier name = new ProgramIdentifier(function.getName());
        var aggregate = this.compiler.getCustomFunctions().getAggregate(name);
        if (aggregate == null)
            throw new CompilationError("Aggregate " + name.singleQuote() + " is unknown", node);
        IAggregate implementation;
        DBSPExpression postZero = DBSPLiteral.none(this.nullableResultType);
        if (function.isLinear()) {
            String mapFunctionName = LinearAggregate.userDefinedMapFunctionName(function.getName());
            String postFunctionName = LinearAggregate.userDefinedPostFunctionName(function.getName());
            DBSPType innerAccumType = LinearAggregate.userAccumulatorType(this.node, function.getName());
            DBSPTypeInteger i64 = DBSPTypeInteger.getType(this.node, INT64, false);
            DBSPType accumulatorType = new DBSPTypeTuple(innerAccumType, i64, i64);

            DBSPExpression isNull = this.getAggregatedValue().is_null();
            DBSPPath path = new DBSPPath(LinearAggregate.userDefinedAccumulatorTypeName(function.getName()), "zero");
            DBSPExpression accumulatorZero = new DBSPApplyExpression(new DBSPPathExpression(DBSPTypeAny.getDefault(), path), innerAccumType);
            DBSPExpression unwrappedAggregated = this.getAggregatedValue().unwrapIfNullable();
            DBSPExpression callMap =
                    new DBSPTupleExpression(
                            new DBSPIfExpression(this.node, isNull, accumulatorZero,
                                new DBSPApplyExpression(this.node, mapFunctionName, innerAccumType, unwrappedAggregated)),
                            new DBSPIfExpression(this.node, isNull, i64.getZero(), i64.getOne()),
                            i64.getOne());
            DBSPVariablePath postVar = accumulatorType.var();
            DBSPExpression callPost = new DBSPApplyExpression(
                    this.node, postFunctionName, this.resultType.withMayBeNull(false), postVar.field(0).applyClone()).some();
            DBSPExpression compare = new DBSPBinaryExpression(
                    this.node, DBSPTypeBool.create(false), DBSPOpcode.EQ, postVar.field(1), i64.getZero());
            DBSPExpression condition = new DBSPIfExpression(this.node, compare, postZero, callPost);
            implementation = new LinearAggregate(this.node, callMap.closure(this.v), condition.closure(postVar), postZero);
        } else {
            // TODO
            throw new UnimplementedException();
        }

        this.setResult(implementation);
    }

    public IAggregate compile() {
        boolean success =
                this.process(this.aggFunction, SqlCountAggFunction.class, this::processCount) ||
                this.process(this.aggFunction, SqlBasicAggFunction.class, this::processBasic) || // arg_max or array_agg
                this.process(this.aggFunction, SqlMinMaxAggFunction.class, this::processMinMax) ||
                this.process(this.aggFunction, SqlSumAggFunction.class, this::processSum) ||
                this.process(this.aggFunction, SqlSumEmptyIsZeroAggFunction.class, this::processSumZero) ||
                this.process(this.aggFunction, SqlAvgAggFunction.class, this::processAvg) || // handles stddev too
                this.process(this.aggFunction, SqlBitOpAggFunction.class, this::processBitOp) ||
                this.process(this.aggFunction, SqlSingleValueAggFunction.class, this::processSingle) ||
                this.process(this.aggFunction, SqlAbstractGroupFunction.class, this::processGrouping) ||
                this.process(this.aggFunction, SqlUserDefinedAggregationFunction.class, this::processUDA);
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
