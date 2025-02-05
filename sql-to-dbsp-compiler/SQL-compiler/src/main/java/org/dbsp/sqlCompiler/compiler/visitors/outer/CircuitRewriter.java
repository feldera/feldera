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

import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.operator.DBSPControlledKeyFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNowOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperatorWithError;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.IRTransform;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPWindowBoundExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPItem;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
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
     * By default, optimize all nodes. */
    final Predicate<DBSPOperator> toOptimize;
    final boolean processDeclarations;

    public CircuitRewriter(DBSPCompiler compiler, IRTransform transform, boolean processDeclarations) {
        this(compiler, transform, processDeclarations, o -> true);
    }

    /** Create a CircuitRewriter.
     *
     * @param compiler    Compiler.
     * @param transform   Function to apply to optimize each node's functions.
     * @param processDeclarations  If true, process declarations as well.
     * @param toOptimize  Predicate which returns 'true' for the nodes to optimize. */
    public CircuitRewriter(DBSPCompiler compiler, IRTransform transform,
                           boolean processDeclarations,
                           Predicate<DBSPOperator> toOptimize) {
        super(compiler, false);
        this.transform = transform;
        this.toOptimize = toOptimize;
        this.processDeclarations = processDeclarations;
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
    public void replace(DBSPSimpleOperator operator) {
        if (!this.toOptimize.test(operator)) {
            super.replace(operator);
            return;
        }
        DBSPExpression function = null;
        if (operator.function != null)
            function = this.transform(operator.function);
        DBSPType outputType = this.transform(operator.outputType);
        List<OutputPort> sources = Linq.map(operator.inputs, this::mapped);
        if (function != operator.function ||
            !outputType.sameType(operator.outputType) ||
            Linq.different(sources, operator.inputs)) {
            DBSPSimpleOperator result = operator.withFunction(function, outputType).withInputs(sources, false);
            this.map(operator, result);
        } else {
            super.replace(operator);
        }
    }

    @Override
    public void postorder(DBSPSourceMultisetOperator operator) {
        DBSPTypeStruct originalRowType = this.transform(operator.originalRowType).to(DBSPTypeStruct.class);
        DBSPType outputType = this.transform(operator.outputType);
        DBSPSimpleOperator result = operator;
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
        DBSPSimpleOperator result = operator;
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
        OutputPort input = this.mapped(operator.input());
        DBSPTypeStruct originalRowType = this.transform(operator.originalRowType).to(DBSPTypeStruct.class);
        DBSPType outputType = this.transform(operator.outputType);
        DBSPSimpleOperator result = operator;
        if (!originalRowType.sameType(operator.originalRowType)
                || !outputType.sameType(operator.outputType)
                || !input.equals(operator.input())) {
            result = new DBSPSinkOperator(operator.getNode(), operator.viewName, operator.query,
                    originalRowType, operator.metadata, input)
                    .copyAnnotations(operator);
        }
        this.map(operator.outputPort(), result.outputPort());
    }

    @Override
    public void postorder(DBSPViewOperator operator) {
        if (!this.toOptimize.test(operator)) {
            super.replace(operator);
            return;
        }
        OutputPort input = this.mapped(operator.input());
        DBSPTypeStruct originalRowType = this.transform(operator.originalRowType).to(DBSPTypeStruct.class);
        DBSPType outputType = this.transform(operator.outputType);
        DBSPSimpleOperator result = operator;
        if (!originalRowType.sameType(operator.originalRowType)
                || !outputType.sameType(operator.outputType)
                || !input.equals(operator.input())) {
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
        OutputPort input = this.mapped(operator.input());
        DBSPSimpleOperator result = operator;
        if (!outputType.sameType(operator.outputType)
                || !DBSPExpression.same(function, operator.function)
                || aggregate != operator.aggregate
                || !input.equals(operator.input())) {
            result = new DBSPStreamAggregateOperator(operator.getNode(),
                    outputType.to(DBSPTypeIndexedZSet.class),
                    function, aggregate, input)
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPChainAggregateOperator operator) {
        DBSPType outputType = this.transform(operator.outputType);
        DBSPExpression function = this.transform(operator.getFunction());
        DBSPExpression init = this.transform(operator.init);
        OutputPort input = this.mapped(operator.input());
        DBSPSimpleOperator result = operator;
        if (!outputType.sameType(operator.outputType)
                || !DBSPExpression.same(function, operator.function)
                || !DBSPExpression.same(init, operator.init)
                || !input.equals(operator.input())) {
            result = new DBSPChainAggregateOperator(operator.getNode(), init.to(DBSPClosureExpression.class),
                    function.to(DBSPClosureExpression.class), outputType, input)
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPFlatMapOperator operator) {
        DBSPType resultType = this.transform(operator.outputType);
        OutputPort input = this.mapped(operator.input());
        DBSPExpression function = this.transform(operator.getFunction());
        DBSPSimpleOperator result = operator;
        if (!resultType.sameType(operator.outputType)
                || !input.equals(operator.input())
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
        OutputPort input = this.mapped(operator.input());

        DBSPSimpleOperator result = operator;
        if (!outputType.sameType(operator.outputType)
                || !input.equals(operator.input())
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
        OutputPort input = this.mapped(operator.input());

        DBSPSimpleOperator result = operator;
        if (!outputType.sameType(operator.outputType)
                || !input.equals(operator.input())
                || postProcess != operator.postProcess
                || function != operator.function) {
            result = new DBSPAggregateLinearPostprocessOperator(operator.getNode(),
                    outputType.to(DBSPTypeIndexedZSet.class), function, postProcess.to(DBSPClosureExpression.class), input)
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPAggregateLinearPostprocessRetainKeysOperator operator) {
        DBSPType outputType = this.transform(operator.outputType);
        DBSPExpression function = this.transform(operator.getFunction());
        DBSPExpression postProcess = this.transform(operator.postProcess);
        DBSPExpression retainKeysFunction = this.transform(operator.retainKeysFunction);
        OutputPort left = this.mapped(operator.left());
        OutputPort right = this.mapped(operator.right());
        DBSPSimpleOperator result = operator;
        if (!outputType.sameType(operator.outputType)
                || !left.equals(operator.left())
                || !right.equals(operator.right())
                || postProcess != operator.postProcess
                || retainKeysFunction != operator.retainKeysFunction
                || function != operator.function) {
            result = new DBSPAggregateLinearPostprocessRetainKeysOperator(operator.getNode(),
                    outputType.to(DBSPTypeIndexedZSet.class), function,
                    postProcess.to(DBSPClosureExpression.class),
                    retainKeysFunction.to(DBSPClosureExpression.class),
                    left, right)
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPConstantOperator operator) {
        DBSPExpression value = this.transform(operator.getFunction());
        DBSPSimpleOperator result = operator;
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
        List<OutputPort> sources = Linq.map(operator.inputs, this::mapped);
        DBSPSimpleOperator result = operator;
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
        List<OutputPort> sources = Linq.map(operator.inputs, this::mapped);
        DBSPSimpleOperator result = operator;
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
        List<OutputPort> sources = Linq.map(operator.inputs, this::mapped);
        DBSPSimpleOperator result = operator;
        if (Linq.different(sources, operator.inputs))
            result = new DBSPWindowOperator(operator.getNode(), operator.lowerInclusive, operator.upperInclusive,
                    sources.get(0), sources.get(1))
                    .copyAnnotations(operator);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPControlledKeyFilterOperator operator) {
        List<OutputPort> sources = Linq.map(operator.inputs, this::mapped);
        DBSPExpression function = this.transform(operator.function);
        DBSPExpression error = this.transform(operator.error);
        DBSPOperatorWithError result = operator;
        if (Linq.different(sources, operator.inputs)
                || function != operator.function
                || error != operator.error) {
            result = new DBSPControlledKeyFilterOperator(operator.getNode(),
                    function.to(DBSPClosureExpression.class), error.to(DBSPClosureExpression.class),
                    sources.get(0), sources.get(1))
                    .copyAnnotations(operator);
        }
        this.map(operator, result, true);
    }

    @Override
    public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
        DBSPType outputType = this.transform(operator.outputType);
        List<OutputPort> sources = Linq.map(operator.inputs, this::mapped);
        DBSPExpression function = this.transform(operator.getFunction());
        DBSPSimpleOperator result = operator;
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
        List<OutputPort> sources = Linq.map(operator.inputs, this::mapped);
        DBSPSimpleOperator result = operator;
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
        List<OutputPort> sources = Linq.map(operator.inputs, this::mapped);
        DBSPSimpleOperator result = operator;
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
        OutputPort input = this.mapped(operator.input());
        DBSPExpression function = this.transform(operator.getFunction());
        DBSPSimpleOperator result = operator;
        if (!outputType.sameType(operator.outputType)
                || !input.equals(operator.input())
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
        OutputPort input = this.mapped(operator.input());
        DBSPExpression function = this.transform(operator.getFunction());
        DBSPComparatorExpression comparator = this.transform(operator.comparator)
                .to(DBSPComparatorExpression.class);
        DBSPExpression projection = this.transform(operator.projection);
        DBSPSimpleOperator result = operator;
        if (!type.sameType(operator.getType())
                || !input.equals(operator.input())
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
        OutputPort input = this.mapped(operator.input());
        DBSPExpression function = this.transform(operator.getFunction());
        DBSPSimpleOperator result = operator;
        if (!type.sameType(operator.getType())
                || !input.equals(operator.input())
                || function != operator.getFunction()) {
            result = new DBSPMapOperator(operator.getNode(), function, type.to(DBSPTypeZSet.class), input)
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPWaterlineOperator operator) {
        OutputPort input = this.mapped(operator.input());
        DBSPClosureExpression function = this.transform(operator.getFunction()).to(DBSPClosureExpression.class);
        DBSPClosureExpression init = this.transform(operator.init).to(DBSPClosureExpression.class);
        DBSPClosureExpression extractTs = this.transform(operator.extractTs).to(DBSPClosureExpression.class);
        DBSPSimpleOperator result = operator;
        if (!input.equals(operator.input())
                || extractTs != operator.extractTs
                || init != operator.init
                || function != operator.function) {
            result = new DBSPWaterlineOperator(
                    operator.getNode(), init, extractTs, function, input)
                    .copyAnnotations(operator);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateOperator operator) {
        DBSPTypeIndexedZSet type = this.transform(operator.getType()).to(DBSPTypeIndexedZSet.class);
        OutputPort input = this.mapped(operator.input());
        @Nullable DBSPExpression function = this.transformN(operator.function);
        DBSPExpression partitioningFunction = this.transform(operator.partitioningFunction);
        @Nullable DBSPAggregate aggregate = null;
        if (operator.aggregate != null) {
            IDBSPInnerNode transformed = this.transform.apply(operator.aggregate);
            aggregate = transformed.to(DBSPAggregate.class);
        }
        DBSPWindowBoundExpression lower = this.transform(operator.lower).to(DBSPWindowBoundExpression.class);
        DBSPWindowBoundExpression upper = this.transform(operator.upper).to(DBSPWindowBoundExpression.class);
        DBSPSimpleOperator result = operator;
        if (!type.sameType(operator.getType())
                || !input.equals(operator.input())
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
        OutputPort left = this.mapped(operator.left());
        OutputPort right = this.mapped(operator.right());
        @Nullable DBSPExpression function = this.transformN(operator.function);
        DBSPExpression partitioningFunction = this.transform(operator.partitioningFunction);
        @Nullable DBSPAggregate aggregate = null;
        if (operator.aggregate != null) {
            IDBSPInnerNode transformed = this.transform.apply(operator.aggregate);
            aggregate = transformed.to(DBSPAggregate.class);
        }
        DBSPWindowBoundExpression lower = this.transform(operator.lower).to(DBSPWindowBoundExpression.class);
        DBSPWindowBoundExpression upper = this.transform(operator.upper).to(DBSPWindowBoundExpression.class);
        DBSPSimpleOperator result = operator;
        if (!type.sameType(operator.getType())
                || !left.equals(operator.left())
                || !right.equals(operator.right())
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
        DBSPItem rewritten = decl.item;
        if (this.processDeclarations)
            rewritten = this.transform.apply(decl.item).to(DBSPItem.class);
        this.getUnderConstruction().addDeclaration(new DBSPDeclaration(rewritten));
    }

    @Override
    public String toString() {
        return super.toString() + "-" + this.transform;
    }
}
