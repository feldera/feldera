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
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.compiler.visitors.inner.IRTransform;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeZSet;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Applies a function (this.transform) to every function within an operator,
 * and to every type within the operator.
 */
public class CircuitRewriter extends CircuitCloneVisitor {
    public final IRTransform transform;

    public CircuitRewriter(IErrorReporter reporter, IRTransform transform) {
        super(reporter, false);
        this.transform = transform;
    }

    public DBSPExpression transform(DBSPExpression expression) {
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
    // - DBSPFilterOperator
    // - DBSPIncrementalDistinctOperator
    // - DBSPIntegralOperator
    // - DBSPNegateOperator
    // - DBSPNoopOperator
    // - DBSPSubtractOperator
    // - DBSPSumOperator
    // - DBSPDeindexOperator
    @Override
    public void replace(DBSPOperator operator) {
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
                    outputType.to(DBSPTypeZSet.class), originalRowType, operator.comment,
                    operator.metadata, operator.outputName);
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
                    operator.comment, operator.metadata, operator.outputName);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPSinkOperator operator) {
        DBSPOperator input = this.mapped(operator.input());
        DBSPTypeStruct originalRowType = this.transform(operator.originalRowType).to(DBSPTypeStruct.class);
        DBSPType outputType = this.transform(operator.outputType);
        DBSPOperator result = operator;
        if (!originalRowType.sameType(operator.originalRowType)
                || !outputType.sameType(operator.outputType)
                || input != operator.input()) {
            result = new DBSPSinkOperator(operator.getNode(), operator.viewName,
                    operator.outputName, operator.query,
                    originalRowType, operator.comment, input);
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
                    function, aggregate, input, operator.isLinear);
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
                    resultType.to(DBSPTypeZSet.class), input);
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
                    outputType.to(DBSPTypeIndexedZSet.class), function, aggregate, input, operator.isLinear);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPConstantOperator operator) {
        DBSPExpression value = this.transform(operator.getFunction());
        DBSPOperator result = operator;
        if (value != operator.getFunction()) {
            result = new DBSPConstantOperator(operator.getNode(), value, operator.isMultiset);
        }
        this.map(operator, result);
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
                    sources.get(0), sources.get(1));
        }
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
                    sources.get(0), sources.get(1));
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
                    sources.get(0), sources.get(1));
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPIndexOperator operator) {
        DBSPType outputType = this.transform(operator.outputType);
        DBSPOperator input = this.mapped(operator.input());
        DBSPExpression function = this.transform(operator.getFunction());
        DBSPOperator result = operator;
        if (!outputType.sameType(operator.outputType)
                || input != operator.input()
                || function != operator.getFunction()) {
            result = new DBSPIndexOperator(operator.getNode(), function,
                    outputType.to(DBSPTypeIndexedZSet.class), operator.isMultiset, input);
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
                    function, operator.isMultiset, sources.get(0), sources.get(1));
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
                    outputType.to(DBSPTypeIndexedZSet.class), input);
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
            result = new DBSPMapOperator(operator.getNode(), function, type.to(DBSPTypeZSet.class), input);
        }
        this.map(operator, result);
    }

    @Override
    public VisitDecision preorder(DBSPPartialCircuit circuit) {
        for (DBSPOperator node : circuit.getAllOperators())
            node.accept(this);
        return VisitDecision.STOP;
    }

    @Override
    public String toString() {
        return super.toString() + ":" + this.transform;
    }
}
