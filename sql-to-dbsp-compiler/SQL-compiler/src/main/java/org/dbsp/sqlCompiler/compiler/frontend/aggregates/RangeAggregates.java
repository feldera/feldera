package org.dbsp.sqlCompiler.compiler.frontend.aggregates;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowExclusion;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.dbsp.sqlCompiler.circuit.annotation.NoChain;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustSqlRuntimeLibrary;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IsNumericLiteral;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedWrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPWindowBoundExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDate;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTime;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.INT128;

/**
 * Implements a window aggregate with a RANGE
 */
public class RangeAggregates extends WindowAggregates {
    final int orderColumnIndex;
    final RelFieldCollation collation;

    protected RangeAggregates(CalciteToDBSPCompiler compiler, Window window, Window.Group group, int windowFieldIndex) {
        super(compiler, window, group, windowFieldIndex);

        List<RelFieldCollation> orderKeys = this.group.orderKeys.getFieldCollations();
        if (orderKeys.isEmpty())
            throw new CompilationError("Missing ORDER BY in OVER", node);
        if (orderKeys.size() > 1)
            throw new UnimplementedException("ORDER BY in OVER requires exactly 1 column", 457, node);
        if (group.exclude != RexWindowExclusion.EXCLUDE_NO_OTHER)
            throw new UnimplementedException("EXCLUDE BY in OVER", 457, node);

        this.collation = orderKeys.get(0);
        this.orderColumnIndex = this.collation.getFieldIndex();
    }

    DBSPWindowBoundExpression compileWindowBound(
            RexWindowBound bound, DBSPType sortType, DBSPType unsignedType, ExpressionCompiler eComp) {
        CalciteObject node = CalciteObject.create(this.window);
        IsNumericType numType = unsignedType.as(IsNumericType.class);
        if (numType == null) {
            throw new UnimplementedException("Currently windows must use integer values, so "
                    + unsignedType + " is not legal", 457, node);
        }
        DBSPExpression numericBound;
        if (bound.isUnbounded())
            numericBound = numType.getMaxValue();
        else if (bound.isCurrentRow())
            numericBound = numType.getZero();
        else {
            DBSPExpression value = eComp.compile(Objects.requireNonNull(bound.getOffset()));
            Simplify simplify = new Simplify(this.compiler.compiler());
            IDBSPInnerNode simplified = simplify.apply(value);
            if (!simplified.is(DBSPLiteral.class)) {
                throw new UnsupportedException("Currently window bounds must be constant values: " +
                        simplified, node);
            }
            DBSPLiteral literal = simplified.to(DBSPLiteral.class);
            if (!literal.to(IsNumericLiteral.class).gt0()) {
                throw new UnsupportedException("Window bounds must be positive: " + literal.toSqlString(), node);
            }
            if (literal.getType().is(DBSPTypeInteger.class)) {
                numericBound = value.cast(node, unsignedType, false);
            } else {
                RustSqlRuntimeLibrary.FunctionDescription desc =
                        RustSqlRuntimeLibrary.getWindowBound(node, unsignedType, sortType, literal.getType());
                numericBound = new DBSPApplyExpression(desc.function, unsignedType, literal.borrow());
            }
        }
        return new DBSPWindowBoundExpression(node, bound.isPreceding(), numericBound);
    }

    @Override
    public DBSPSimpleOperator implement(DBSPSimpleOperator input, DBSPSimpleOperator lastOperator, boolean isLast) {
        // The final result is accumulated using join operators, which just keep adding columns to
        // the "lastOperator".
        List<RelFieldCollation> orderKeys = this.group.orderKeys.getFieldCollations();
        if (orderKeys.isEmpty())
            throw new CompilationError("Missing ORDER BY in OVER", this.node);
        if (orderKeys.size() > 1)
            throw new UnimplementedException("ORDER BY in OVER requires exactly 1 column", 457, this.node);

        DBSPType sortType, originalSortType;
        DBSPType unsignedSortType;
        DBSPSimpleOperator mapIndex;
        boolean ascending = this.collation.getDirection() == RelFieldCollation.Direction.ASCENDING;
        boolean nullsLast = this.collation.nullDirection != RelFieldCollation.NullDirection.FIRST;
        DBSPType partitionType;
        DBSPType partitionAndRowType;
        DBSPTypeTuple lastTupleType = lastOperator.getOutputZSetElementType().to(DBSPTypeTuple.class);

        {
            DBSPTupleExpression partitionKeys = this.partitionKeys();
            partitionType = partitionKeys.getType();

            DBSPExpression originalOrderField = this.inputRowRefVar.deref().field(orderColumnIndex);
            sortType = originalOrderField.getType();
            originalSortType = sortType;
            // Original scale if the sort field is a DECIMAL
            if (sortType.is(DBSPTypeDecimal.class)) {
                // Scale decimal to make it an integer by multiplying with 10^scale
                DBSPTypeDecimal dec = sortType.to(DBSPTypeDecimal.class);

                DBSPTypeInteger intType;
                DBSPTypeCode code = DBSPTypeInteger.smallestInteger(dec.precision);
                if (code != null) {
                    intType = DBSPTypeInteger.getType(this.node, code, dec.mayBeNull);
                    DBSPType i128 = DBSPTypeInteger.getType(this.node, INT128, dec.mayBeNull);
                    // directly build the expression, no casts are needed
                    originalOrderField = new DBSPUnaryExpression(
                            this.node, i128, DBSPOpcode.DECIMAL_TO_INTEGER, originalOrderField);
                    originalOrderField = originalOrderField.cast(CalciteObject.EMPTY, intType, false);
                    sortType = intType;
                }
            }

            if (!sortType.is(DBSPTypeInteger.class) &&
                    !sortType.is(DBSPTypeTimestamp.class) &&
                    !sortType.is(DBSPTypeDate.class) &&
                    !sortType.is(DBSPTypeTime.class))
                throw new UnimplementedException("OVER currently cannot sort on columns with type "
                        + Utilities.singleQuote(sortType.asSqlString()), 457, node);

            // This only works if the order field is unsigned.
            DBSPExpression orderField = new DBSPUnsignedWrapExpression(
                    this.node, originalOrderField, ascending, nullsLast);
            unsignedSortType = orderField.getType();

            // Map each row to an expression of the form: |t| (order, Tup2(partition, (*t).clone()))
            DBSPExpression partitionAndRow = new DBSPTupleExpression(
                    partitionKeys, inputRowRefVar.deref().applyClone());
            partitionAndRowType = partitionAndRow.getType();
            DBSPExpression indexExpr = new DBSPRawTupleExpression(orderField, partitionAndRow);
            DBSPClosureExpression indexClosure = indexExpr.closure(inputRowRefVar);
            mapIndex = new DBSPMapIndexOperator(this.node, indexClosure,
                    TypeCompiler.makeIndexedZSet(orderField.getType(), partitionAndRow.getType()), input.outputPort());
            this.compiler.addOperator(mapIndex);
        }

        DBSPSimpleOperator windowAgg;
        DBSPTypeTuple aggResultType;
        {
            // Compute the window aggregate

            // This operator is always incremental, so create the non-incremental version
            // of it by adding a Differentiator and an Integrator around it.
            DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(node, mapIndex.outputPort());
            this.compiler.addOperator(diff);

            // Create window description
            DBSPWindowBoundExpression lb = this.compileWindowBound(group.lowerBound, sortType, unsignedSortType, eComp);
            DBSPWindowBoundExpression ub = this.compileWindowBound(group.upperBound, sortType, unsignedSortType, eComp);

            List<DBSPType> types = Linq.map(
                    this.aggregateCalls, c -> this.compiler.convertType(c.type, false));
            DBSPTypeTuple tuple = new DBSPTypeTuple(types);
            DBSPAggregateList list = this.compiler.createAggregates(this.compiler.compiler(),
                    this.window, this.aggregateCalls, this.window.constants, tuple, this.inputRowType, 0,
                    ImmutableBitSet.of(), false);

            // This function is always the same: |Tup2(x, y)| (x, y)
            DBSPVariablePath pr = new DBSPVariablePath(partitionAndRowType.ref());
            DBSPClosureExpression partitioningFunction =
                    new DBSPRawTupleExpression(
                            pr.deref().field(0).applyCloneIfNeeded(),
                            pr.deref().field(1).applyCloneIfNeeded())
                            .closure(pr);

            aggResultType = list.getEmptySetResultType().to(DBSPTypeTuple.class);
            // Prepare a type that will make the operator following the window aggregate happy
            // (that operator is a map_index).  Currently, the compiler cannot represent
            // exactly the output type of the WindowAggregateOperator, so it lies about the actual type.
            DBSPTypeIndexedZSet windowOutputType =
                    TypeCompiler.makeIndexedZSet(partitionType,
                            new DBSPTypeTuple(
                                    unsignedSortType,
                                    aggResultType.withMayBeNull(true)));

            // Compute aggregates for the window
            windowAgg = new DBSPPartitionedRollingAggregateOperator(
                    node, partitioningFunction, null, list,
                    lb, ub, windowOutputType, diff.outputPort());
            this.compiler.addOperator(windowAgg);
        }

        DBSPMapIndexOperator index;
        {
            // Index the produced result
            // map_index(|(key_ts_agg)| (
            //         Tup2::new(key_ts_agg.0.0,
            //                   UnsignedWrapper::to_signed::<i32, i32, i64, u64>(key_ts_agg.1.0, true, true)),
            //         key_ts_agg.1.1.unwrap_or_default() ))
            DBSPVariablePath var = new DBSPVariablePath("key_ts_agg",
                    new DBSPTypeRawTuple(
                            partitionType.ref(),
                            new DBSPTypeTuple(
                                    unsignedSortType,  // not the sortType, but the wrapper type around it
                                    aggResultType.withMayBeNull(true)).ref()));
            // new DBSPTypeOption(aggResultType)).ref()));
            DBSPExpression ts = var.field(1).deref().field(0);
            DBSPExpression agg = var.field(1).deref().field(1).applyCloneIfNeeded();
            DBSPExpression unwrap = new DBSPUnsignedUnwrapExpression(
                    this.node, ts, sortType, ascending, nullsLast);
            if (originalSortType.is(DBSPTypeDecimal.class)) {
                // convert back to decimal and rescale
                DBSPType i128 = DBSPTypeInteger.getType(this.node, INT128, originalSortType.mayBeNull);
                unwrap = unwrap.cast(this.node, i128, false);
                unwrap = new DBSPUnaryExpression(this.node, originalSortType,
                        DBSPOpcode.INTEGER_TO_DECIMAL, unwrap);
            }

            DBSPExpression ixKey = var.field(0).deref();
            List<DBSPExpression> keyFields = new ArrayList<>(Arrays.asList(
                    Objects.requireNonNull(DBSPTupleExpression.flatten(ixKey).fields)));
            keyFields.add(unwrap);
            DBSPExpression body = new DBSPRawTupleExpression(
                    new DBSPTupleExpression(keyFields, false),
                    new DBSPApplyMethodExpression(this.node, "unwrap_or_default", aggResultType, agg));
            index = new DBSPMapIndexOperator(this.node, body.closure(var), windowAgg.outputPort())
                    .addAnnotation(new NoChain(), DBSPMapIndexOperator.class);
            this.compiler.addOperator(index);
        }

        DBSPIntegrateOperator integral = new DBSPIntegrateOperator(this.node, index.outputPort());
        this.compiler.addOperator(integral);

        // Join the previous result with the aggregate
        DBSPSimpleOperator indexInput;
        DBSPType lastPartAndOrderType;
        DBSPType lastCopiedFieldsType;
        {
            // Index the lastOperator
            DBSPVariablePath previousRowRefVar = lastTupleType.ref().var();
            List<DBSPExpression> expressions = Linq.map(partitionKeys,
                    f -> previousRowRefVar.deref().field(f).applyCloneIfNeeded());

            DBSPExpression originalOrderField = previousRowRefVar.deref().field(orderColumnIndex);
            expressions.add(originalOrderField.applyCloneIfNeeded());
            DBSPExpression partAndOrder = new DBSPTupleExpression(expressions, false);
            lastPartAndOrderType = partAndOrder.getType();
            // Copy all the fields from the previousRowRefVar except the partition fields.
            List<DBSPExpression> fields = new ArrayList<>();
            for (int i = 0; i < lastTupleType.size(); i++) {
                if (partitionKeys.contains(i))
                    continue;
                if (orderColumnIndex == i)
                    continue;
                fields.add(previousRowRefVar.deref().field(i).applyCloneIfNeeded());
            }
            DBSPExpression copiedFields = new DBSPTupleExpression(fields, false);
            lastCopiedFieldsType = copiedFields.getType();
            DBSPExpression indexedInput = new DBSPRawTupleExpression(partAndOrder, copiedFields);
            DBSPClosureExpression partAndOrderClo = indexedInput.closure(previousRowRefVar);

            indexInput = new DBSPMapIndexOperator(node, partAndOrderClo,
                    TypeCompiler.makeIndexedZSet(partAndOrder.getType(), copiedFields.getType()),
                    lastOperator.isMultiset, lastOperator.outputPort());
            this.compiler.addOperator(indexInput);
        }

        {
            // Join the results
            DBSPVariablePath key = lastPartAndOrderType.ref().var();
            DBSPVariablePath left = lastCopiedFieldsType.ref().var();
            DBSPVariablePath right = aggResultType.ref().var();
            DBSPExpression[] allFields = new DBSPExpression[
                    lastTupleType.size() + aggResultType.size()];
            int indexField = 0;
            for (int i = 0; i < lastTupleType.size(); i++) {
                if (partitionKeys.contains(i)) {
                    int keyIndex = partitionKeys.indexOf(i);
                    // If the field is in the index, use it from the index
                    allFields[i] = key
                            .deref()
                            .field(keyIndex)
                            .applyCloneIfNeeded();
                    indexField++;
                } else if (orderColumnIndex == i) {
                    // If the field is the order key, use it from the index too; it's the last one
                    allFields[i] = key
                            .deref()
                            .field(this.partitionKeys.size())
                            .applyCloneIfNeeded();
                    indexField++;
                } else {
                    allFields[i] = left.deref().field(i - indexField).applyCloneIfNeeded();
                }
            }
            for (int i = 0; i < aggResultType.size(); i++) {
                // Calcite is very smart and sometimes infers non-nullable result types
                // for these aggregates.  So we have to cast the results to whatever
                // Calcite says they will be.
                allFields[i + lastTupleType.size()] = right.deref().field(i).applyCloneIfNeeded().cast(
                        this.node, this.windowResultType.getFieldType(this.windowFieldIndex + i), false);
            }
            DBSPTupleExpression addExtraFieldBody = new DBSPTupleExpression(allFields);
            DBSPClosureExpression addExtraField =
                    addExtraFieldBody.closure(key, left, right);
            CalciteRelNode n = node;
            if (isLast)
                n = node.getFinal();
            return new DBSPStreamJoinOperator(n, TypeCompiler.makeZSet(addExtraFieldBody.getType()),
                    addExtraField, indexInput.isMultiset || windowAgg.isMultiset,
                    indexInput.outputPort(), integral.outputPort());
        }
    }

    @Override
    public boolean isCompatible(AggregateCall call) {
        SqlKind kind = call.getAggregation().getKind();
        return kind != SqlKind.LAG &&
                kind != SqlKind.LEAD &&
                kind != SqlKind.FIRST_VALUE &&
                kind != SqlKind.LAST_VALUE;
    }
}
