/*
 * Copyright 2023 VMware, Inc.
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

package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPPartialCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPControlledFilterOperator;
import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNowOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.IRTransform;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPWindowBoundExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPItem;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Predicate;

/** Applies a function (this.transform) to every function within an operator,
 * and to every type within the operator. */
public class CircuitRewriter extends CircuitCloneVisitor {
    public final IRTransform transform;
    /** Only optimize functions for nodes where this predicate returns 'true'.
     * By default optimize all nodes. */
    Predicate<DBSPOperator> toOptimize = o -> true;

    public CircuitRewriter(IErrorReporter reporter, IRTransform transform) {
        super(reporter, false);
        this.transform = transform;
    }

    /** Create a CircuitRewriter.
     *
     * @param reporter    Error reporter.
     * @param transform   Function to apply to optimize each node's functions.
     * @param toOptimize  Predicate which returns 'true' for the nodes to optimize. */
    public CircuitRewriter(IErrorReporter reporter, IRTransform transform, Predicate<DBSPOperator> toOptimize) {
        super(reporter, false);
        this.transform = transform;
        this.toOptimize = toOptimize;
    }

    public DBSPExpression transform(DBSPExpression expression) {
        if (!this.toOptimize.test(this.getCurrent().to(DBSPOperator.class))) {
            return expression;
        }
        IDBSPInnerNode result = this.transform.apply(expression);
        return result.to(DBSPExpression.class);
    }

    @Nullable public DBSPExpression transformN(@Nullable DBSPExpression expression) {
        if (expression == null)
            return null;
        return this.transform(expression);
    }

    public DBSPType transform(DBSPType type) {
        return this.transform.apply(type).to(DBSPType.class);
    }

    // Default implementation, used for
    // - DBSPDifferentialOperator
    // - DBSPDistinctOperator
    // - DBSPDistinctIncrementalOperator
    // - DBSPFilterOperator
    // - DBSPIncrementalDistinctOperator
    // - DBSPIntegralOperator
    // - DBSPNegateOperator
    // - DBSPNoopOperator
    // - DBSPSubtractOperator
    // - DBSPSumOperator
    // - DBSPDeindexOperator
    // - DBSPApplyOperator
    // - DBSPApply2Operator
    @Override
    public void replace(DBSPOperator operator) {
        if (!this.toOptimize.test(operator)) {
            super.replace(operator);
            return;
        }
        DBSPExpression function = null;
        if (operator.function != null)
            function = this.transform(operator.function);
        DBSPType outputType = this.transform(operator.outputType);
        List<DBSPOperator> sources = Linq.map(operator.inputs, this::mapped);
        if (function != operator.function ||
            outputType != operator.outputType ||
            Linq.different(sources, operator.inputs)) {
            DBSPOperator result = operator.withFunction(function, outputType).withInputs(sources, false);
            this.map(operator, result);
        } else {
            super.replace(operator);
        }
    }

    @Override
    public void postorder(DBSPSourceMultisetOperator operator) {
        DBSPTypeStruct originalRowType = this.transform(operator.originalRowType).to(DBSPTypeStruct.class);
        DBSPType outputType = this.transform(operator.outputType);
        DBSPOperator result = operator;
        if (!originalRowType.sameType(operator.originalRowType)
                || !outputType.sameType(operator.outputType)) {
            result = new DBSPSourceMultisetOperator(operator.getNode(), operator.sourceName,
                    outputType.to(DBSPTypeZSet.class), originalRowType,
                    operator.metadata, operator.getTableName(), operator.comment)
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPSourceMapOperator operator) {
        DBSPTypeStruct originalRowType = this.transform(operator.originalRowType).to(DBSPTypeStruct.class);
        DBSPType outputType = this.transform(operator.outputType);
        DBSPOperator result = operator;
        if (!originalRowType.sameType(operator.originalRowType)
                || !outputType.sameType(operator.outputType)) {
            result = new DBSPSourceMapOperator(operator.getNode(), operator.sourceName,
                    operator.keyFields, outputType.to(DBSPTypeIndexedZSet.class), originalRowType,
                    operator.metadata, operator.getTableName(), operator.comment)
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPSinkOperator operator) {
        if (!this.toOptimize.test(operator)) {
            super.replace(operator);
            return;
        }
        DBSPOperator input = this.mapped(operator.input());
        DBSPTypeStruct originalRowType = this.transform(operator.originalRowType).to(DBSPTypeStruct.class);
        DBSPType outputType = this.transform(operator.outputType);
        DBSPOperator result = operator;
        if (!originalRowType.sameType(operator.originalRowType)
                || !outputType.sameType(operator.outputType)
                || input != operator.input()) {
            result = new DBSPSinkOperator(operator.getNode(), operator.viewName, operator.query,
                    originalRowType, operator.metadata, input)
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPViewOperator operator) {
        if (!this.toOptimize.test(operator)) {
            super.replace(operator);
            return;
        }
        DBSPOperator input = this.mapped(operator.input());
        DBSPTypeStruct originalRowType = this.transform(operator.originalRowType).to(DBSPTypeStruct.class);
        DBSPType outputType = this.transform(operator.outputType);
        DBSPOperator result = operator;
        if (!originalRowType.sameType(operator.originalRowType)
                || !outputType.sameType(operator.outputType)
                || input != operator.input()) {
            result = new DBSPViewOperator(operator.getNode(), operator.viewName, operator.query,
                    originalRowType, operator.metadata, input)
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPStreamAggregateOperator operator) {
        DBSPType outputType = this.transform(operator.outputType);
        @Nullable DBSPExpression function = this.transformN(operator.function);
        @Nullable DBSPAggregate aggregate = null;
        if (operator.aggregate != null) {
            IDBSPInnerNode transformed = this.transform.apply(operator.aggregate);
            aggregate = transformed.to(DBSPAggregate.class);
        }
        DBSPOperator input = this.mapped(operator.input());
        DBSPOperator result = operator;
        if (!outputType.sameType(operator.outputType)
                || DBSPExpression.same(function, operator.function)
                || aggregate != operator.aggregate
                || input != operator.input()) {
            result = new DBSPStreamAggregateOperator(operator.getNode(),
                    outputType.to(DBSPTypeIndexedZSet.class),
                    function, aggregate, input)
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPFlatMapOperator operator) {
        DBSPType resultType = this.transform(operator.outputType);
        DBSPOperator input = this.mapped(operator.input());
        DBSPExpression function = this.transform(operator.getFunction());
        DBSPOperator result = operator;
        if (!resultType.sameType(operator.outputType)
                || input != operator.input()
                || function != operator.getFunction()) {
            result = new DBSPFlatMapOperator(
                    operator.getNode(), function,
                    resultType.to(DBSPTypeZSet.class), input)
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPAggregateOperator operator) {
        DBSPType outputType = this.transform(operator.outputType);
        @Nullable DBSPExpression function = this.transformN(operator.function);
        @Nullable DBSPAggregate aggregate = null;
        if (operator.aggregate != null)
            aggregate = this.transform.apply(operator.aggregate).to(DBSPAggregate.class);
        DBSPOperator input = this.mapped(operator.input());

        DBSPOperator result = operator;
        if (!outputType.sameType(operator.outputType)
                || input != operator.input()
                || aggregate != operator.aggregate
                || function != operator.function) {
            result = new DBSPAggregateOperator(operator.getNode(),
                    outputType.to(DBSPTypeIndexedZSet.class), function, aggregate, input)
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPAggregateLinearPostprocessOperator operator) {
        DBSPType outputType = this.transform(operator.outputType);
        DBSPExpression function = this.transform(operator.getFunction());
        DBSPExpression postProcess = this.transform(operator.postProcess);
        DBSPOperator input = this.mapped(operator.input());

        DBSPOperator result = operator;
        if (!outputType.sameType(operator.outputType)
                || input != operator.input()
                || postProcess != operator.postProcess
                || function != operator.function) {
            result = new DBSPAggregateLinearPostprocessOperator(operator.getNode(),
                    outputType.to(DBSPTypeIndexedZSet.class), function, postProcess.to(DBSPClosureExpression.class), input)
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPConstantOperator operator) {
        DBSPExpression value = this.transform(operator.getFunction());
        DBSPOperator result = operator;
        if (value != operator.getFunction()) {
            result = new DBSPConstantOperator(operator.getNode(), value,
                    operator.incremental, operator.isMultiset)
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPNowOperator operator) {
        this.map(operator, operator);
    }

    @Override
    public void postorder(DBSPJoinOperator operator) {
        DBSPType outputType = this.transform(operator.outputType);
        DBSPExpression function = this.transform(operator.getFunction());
        List<DBSPOperator> sources = Linq.map(operator.inputs, this::mapped);
        DBSPOperator result = operator;
        if (!outputType.sameType(operator.outputType)
                || function != operator.function
                || Linq.different(sources, operator.inputs)) {
            result = new DBSPJoinOperator(operator.getNode(),
                    outputType.to(DBSPTypeZSet.class), function, operator.isMultiset,
                    sources.get(0), sources.get(1))
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPAsofJoinOperator operator) {
        DBSPType outputType = this.transform(operator.outputType);
        DBSPExpression function = this.transform(operator.getFunction());
        DBSPComparatorExpression comparator = this.transform(operator.comparator).to(DBSPComparatorExpression.class);
        DBSPClosureExpression leftTimestamp = this.transform(operator.leftTimestamp).to(DBSPClosureExpression.class);
        DBSPClosureExpression rightTimestamp = this.transform(operator.rightTimestamp).to(DBSPClosureExpression.class);
        List<DBSPOperator> sources = Linq.map(operator.inputs, this::mapped);
        DBSPOperator result = operator;
        if (!outputType.sameType(operator.outputType)
                || function != operator.function
                || comparator != operator.comparator
                || leftTimestamp != operator.leftTimestamp
                || rightTimestamp != operator.rightTimestamp
                || Linq.different(sources, operator.inputs)) {
            result = new DBSPAsofJoinOperator(operator.getNode(),
                    outputType.to(DBSPTypeZSet.class), function,
                    leftTimestamp, rightTimestamp, comparator,
                    operator.isMultiset, operator.isLeft,
                    sources.get(0), sources.get(1))
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPWindowOperator operator) {
        List<DBSPOperator> sources = Linq.map(operator.inputs, this::mapped);
        DBSPOperator result = operator;
        if (Linq.different(sources, operator.inputs))
            result = new DBSPWindowOperator(operator.getNode(), operator.lowerInclusive, operator.upperInclusive,
                    sources.get(0), sources.get(1))
                    .copyAnnotations(operator);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPControlledFilterOperator operator) {
        DBSPType outputType = this.transform(operator.outputType);
        List<DBSPOperator> sources = Linq.map(operator.inputs, this::mapped);
        DBSPExpression function = this.transform(operator.getFunction());
        DBSPOperator result = operator;
        if (!outputType.sameType(operator.outputType)
                || Linq.different(sources, operator.inputs)
                || function != operator.getFunction()) {
            result = new DBSPControlledFilterOperator(operator.getNode(), function,
                    sources.get(0), sources.get(1))
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
        DBSPType outputType = this.transform(operator.outputType);
        List<DBSPOperator> sources = Linq.map(operator.inputs, this::mapped);
        DBSPExpression function = this.transform(operator.getFunction());
        DBSPOperator result = operator;
        if (!outputType.sameType(operator.outputType)
                || Linq.different(sources, operator.inputs)
                || function != operator.getFunction()) {
            result = new DBSPIntegrateTraceRetainKeysOperator(operator.getNode(), function,
                    sources.get(0), sources.get(1))
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPStreamJoinOperator operator) {
        DBSPType outputType = this.transform(operator.outputType);
        DBSPExpression function = this.transform(operator.getFunction());
        List<DBSPOperator> sources = Linq.map(operator.inputs, this::mapped);
        DBSPOperator result = operator;
        if (!outputType.sameType(operator.outputType)
                || function != operator.function
                || Linq.different(sources, operator.inputs)) {
            result = new DBSPStreamJoinOperator(operator.getNode(), outputType.to(DBSPTypeZSet.class),
                    function, operator.isMultiset, sources.get(0), sources.get(1))
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPJoinFilterMapOperator operator) {
        DBSPType outputType = this.transform(operator.outputType);
        DBSPExpression function = this.transform(operator.getFunction());
        DBSPExpression filter = this.transformN(operator.filter);
        DBSPExpression map = this.transformN(operator.map);
        List<DBSPOperator> sources = Linq.map(operator.inputs, this::mapped);
        DBSPOperator result = operator;
        if (!outputType.sameType(operator.outputType)
                || function != operator.function
                || filter != operator.filter
                || map != operator.map
                || Linq.different(sources, operator.inputs)) {
            result = new DBSPJoinFilterMapOperator(operator.getNode(), outputType.to(DBSPTypeZSet.class),
                    function, filter, map, operator.isMultiset, sources.get(0), sources.get(1))
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        DBSPType outputType = this.transform(operator.outputType);
        DBSPOperator input = this.mapped(operator.input());
        DBSPExpression function = this.transform(operator.getFunction());
        DBSPOperator result = operator;
        if (!outputType.sameType(operator.outputType)
                || input != operator.input()
                || function != operator.getFunction()) {
            result = new DBSPMapIndexOperator(operator.getNode(), function,
                    outputType.to(DBSPTypeIndexedZSet.class), input)
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPLagOperator operator) {
        DBSPType type = this.transform(operator.getType());
        DBSPOperator input = this.mapped(operator.input());
        DBSPExpression function = this.transform(operator.getFunction());
        DBSPComparatorExpression comparator = this.transform(operator.comparator)
                .to(DBSPComparatorExpression.class);
        DBSPExpression projection = this.transform(operator.projection);
        DBSPOperator result = operator;
        if (!type.sameType(operator.getType())
                || input != operator.input()
                || projection != operator.projection
                || function != operator.getFunction()
                || comparator != operator.comparator) {
            result = new DBSPLagOperator(operator.getNode(), operator.offset,
                    projection, function, comparator,
                    type.to(DBSPTypeIndexedZSet.class), input)
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        DBSPType type = this.transform(operator.getType());
        DBSPOperator input = this.mapped(operator.input());
        DBSPExpression function = this.transform(operator.getFunction());
        DBSPOperator result = operator;
        if (!type.sameType(operator.getType())
                || input != operator.input()
                || function != operator.getFunction()) {
            result = new DBSPMapOperator(operator.getNode(), function, type.to(DBSPTypeZSet.class), input)
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateOperator operator) {
        DBSPTypeIndexedZSet type = this.transform(operator.getType()).to(DBSPTypeIndexedZSet.class);
        DBSPOperator input = this.mapped(operator.input());
        @Nullable DBSPExpression function = this.transformN(operator.function);
        DBSPExpression partitioningFunction = this.transform(operator.partitioningFunction);
        @Nullable DBSPAggregate aggregate = null;
        if (operator.aggregate != null) {
            IDBSPInnerNode transformed = this.transform.apply(operator.aggregate);
            aggregate = transformed.to(DBSPAggregate.class);
        }
        DBSPWindowBoundExpression lower = this.transform(operator.lower).to(DBSPWindowBoundExpression.class);
        DBSPWindowBoundExpression upper = this.transform(operator.upper).to(DBSPWindowBoundExpression.class);
        DBSPOperator result = operator;
        if (!type.sameType(operator.getType())
                || input != operator.input()
                || partitioningFunction != operator.partitioningFunction
                || function != operator.function
                || aggregate != operator.aggregate
                || lower != operator.lower
                || upper != operator.upper) {
            result = new DBSPPartitionedRollingAggregateOperator(
                    operator.getNode(), partitioningFunction, function, aggregate, lower, upper, type, input)
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateWithWaterlineOperator operator) {
        DBSPTypeIndexedZSet type = this.transform(operator.getType()).to(DBSPTypeIndexedZSet.class);
        DBSPOperator left = this.mapped(operator.left());
        DBSPOperator right = this.mapped(operator.right());
        @Nullable DBSPExpression function = this.transformN(operator.function);
        DBSPExpression partitioningFunction = this.transform(operator.partitioningFunction);
        @Nullable DBSPAggregate aggregate = null;
        if (operator.aggregate != null) {
            IDBSPInnerNode transformed = this.transform.apply(operator.aggregate);
            aggregate = transformed.to(DBSPAggregate.class);
        }
        DBSPWindowBoundExpression lower = this.transform(operator.lower).to(DBSPWindowBoundExpression.class);
        DBSPWindowBoundExpression upper = this.transform(operator.upper).to(DBSPWindowBoundExpression.class);
        DBSPOperator result = operator;
        if (!type.sameType(operator.getType())
                || left != operator.left()
                || right != operator.right()
                || partitioningFunction != operator.partitioningFunction
                || function != operator.function
                || aggregate != operator.aggregate
                || lower != operator.lower
                || upper != operator.upper) {
            result = new DBSPPartitionedRollingAggregateWithWaterlineOperator(
                    operator.getNode(), partitioningFunction, function, aggregate,
                    lower, upper, type, left, right)
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPDeclaration decl) {
        DBSPItem rewritten = this.transform.apply(decl.item).to(DBSPItem.class);
        this.getResult().declarations.add(new DBSPDeclaration(rewritten));
    }

    @Override
    public VisitDecision preorder(DBSPPartialCircuit circuit) {
        for (DBSPDeclaration node : circuit.declarations)
            node.accept(this);
        for (DBSPOperator node : circuit.getAllOperators())
            node.accept(this);
        return VisitDecision.STOP;
    }

    @Override
    public String toString() {
        return super.toString() + "-" + this.transform;
    }
}
