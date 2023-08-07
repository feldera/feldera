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
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceOperator;
import org.dbsp.sqlCompiler.compiler.visitors.inner.IRTransform;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIncrementalAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIncrementalJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStruct;
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
        return this.transform.apply(expression).to(DBSPExpression.class);
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
    public void postorder(DBSPSourceOperator operator) {
        DBSPTypeStruct originalRowType = this.transform(operator.originalRowType).to(DBSPTypeStruct.class);
        DBSPType outputType = this.transform(operator.outputType);
        DBSPOperator result = operator;
        if (!originalRowType.sameType(operator.originalRowType)
                || !outputType.sameType(operator.outputType)) {
            result = new DBSPSourceOperator(operator.getNode(), operator.sourceName,
                    outputType, originalRowType, operator.comment, operator.outputName);
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
            result = new DBSPSinkOperator(operator.getNode(), operator.outputName, operator.query,
                    originalRowType, operator.comment, input);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPAggregateOperator operator) {
        DBSPType keyType = this.transform(operator.keyType);
        DBSPType outputElementType = this.transform(operator.outputElementType);
        DBSPType weightType = this.transform(operator.weightType);
        @Nullable DBSPExpression function = this.transformN(operator.function);
        @Nullable DBSPAggregate aggregate = null;
        if (operator.aggregate != null) {
            IDBSPInnerNode transformed = this.transform.apply(operator.aggregate);
            aggregate = transformed.to(DBSPAggregate.class);
        }
        DBSPOperator input = this.mapped(operator.input());
        DBSPOperator result = operator;
        if (!keyType.sameType(operator.keyType)
                || !outputElementType.sameType(operator.outputElementType)
                || !weightType.sameType(operator.weightType)
                || DBSPExpression.same(function, operator.function)
                || aggregate != operator.aggregate
                || input != operator.input()) {
            result = new DBSPAggregateOperator(operator.getNode(),
                    keyType, outputElementType, weightType,
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
            result = new DBSPFlatMapOperator(operator.getNode(), function, resultType, input);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPIncrementalAggregateOperator operator) {
        DBSPType keyType = this.transform(operator.keyType);
        DBSPType outputElementType = this.transform(operator.outputElementType);
        DBSPType weightType = this.transform(operator.weightType);
        @Nullable DBSPExpression function = this.transformN(operator.function);
        @Nullable DBSPAggregate aggregate = null;
        if (operator.aggregate != null)
            aggregate = this.transform.apply(operator.aggregate).to(DBSPAggregate.class);
        DBSPOperator input = this.mapped(operator.input());

        DBSPOperator result = operator;
        if (!keyType.sameType(operator.keyType)
                || outputElementType != operator.outputElementType
                || weightType != operator.weightType
                || input != operator.input()
                || aggregate != operator.aggregate
                || function != operator.function) {
            result = new DBSPIncrementalAggregateOperator(operator.getNode(),
                    keyType, outputElementType, weightType, function, aggregate, input, operator.isLinear);
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
    public void postorder(DBSPIncrementalJoinOperator operator) {
        DBSPType elementResultType = this.transform(operator.elementResultType);
        DBSPType weightType = this.transform(operator.weightType);
        DBSPExpression function = this.transform(operator.getFunction());
        List<DBSPOperator> sources = Linq.map(operator.inputs, this::mapped);
        DBSPOperator result = operator;
        if (!elementResultType.sameType(operator.elementResultType)
                || !weightType.sameType(operator.weightType)
                || function != operator.function
                || Linq.different(sources, operator.inputs)) {
            result = new DBSPIncrementalJoinOperator(operator.getNode(),
                    elementResultType, weightType, function, operator.isMultiset,
                    sources.get(0), sources.get(1));
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPIndexOperator operator) {
        DBSPType keyType = this.transform(operator.keyType);
        DBSPType elementType = this.transform(operator.elementType);
        DBSPType weightType = this.transform(operator.weightType);
        DBSPOperator input = this.mapped(operator.input());
        DBSPExpression function = this.transform(operator.getFunction());
        DBSPOperator result = operator;
        if (!keyType.sameType(operator.keyType)
                || !elementType.sameType(operator.elementType)
                || !weightType.sameType(operator.weightType)
                || input != operator.input()
                || function != operator.getFunction()) {
            result = new DBSPIndexOperator(operator.getNode(), function,
                    keyType, elementType, weightType, operator.isMultiset, input);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPJoinOperator operator) {
        DBSPType elementResultType = this.transform(operator.elementResultType);
        DBSPType weightType = this.transform(operator.weightType);
        DBSPExpression function = this.transform(operator.getFunction());
        List<DBSPOperator> sources = Linq.map(operator.inputs, this::mapped);
        DBSPOperator result = operator;
        if (!elementResultType.sameType(operator.elementResultType)
                || !weightType.sameType(operator.weightType)
                || function != operator.function
                || Linq.different(sources, operator.inputs)) {
            result = new DBSPJoinOperator(operator.getNode(), elementResultType, weightType,
                    function, operator.isMultiset, sources.get(0), sources.get(1));
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        DBSPType keyType = this.transform(operator.keyType);
        DBSPType valueType = this.transform(operator.valueType);
        DBSPType weightType = this.transform(operator.weightType);
        DBSPOperator input = this.mapped(operator.input());
        DBSPExpression function = this.transform(operator.getFunction());
        DBSPOperator result = operator;
        if (!keyType.sameType(operator.keyType)
                || !valueType.sameType(operator.valueType)
                || !weightType.sameType(operator.weightType)
                || input != operator.input()
                || function != operator.getFunction()) {
            result = new DBSPMapIndexOperator(operator.getNode(), function,
                    keyType, valueType, weightType, input);
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        DBSPType outputElementType = this.transform(operator.outputElementType);
        DBSPType weightType = this.transform(operator.weightType);
        DBSPOperator input = this.mapped(operator.input());
        DBSPExpression function = this.transform(operator.getFunction());
        DBSPOperator result = operator;
        if (!outputElementType.sameType(operator.outputElementType)
                || !weightType.sameType(operator.weightType)
                || input != operator.input()
                || function != operator.getFunction()) {
            result = new DBSPMapOperator(operator.getNode(), function,
                    outputElementType, weightType, input);
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
