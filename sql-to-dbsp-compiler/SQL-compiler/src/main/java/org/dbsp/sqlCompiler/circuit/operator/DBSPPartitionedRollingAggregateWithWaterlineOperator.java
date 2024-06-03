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

package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;

import javax.annotation.Nullable;
import java.util.List;

public final class DBSPPartitionedRollingAggregateWithWaterlineOperator extends DBSPOperator {
    @Nullable
    public final DBSPAggregate aggregate;
    public final DBSPExpression partitioningFunction;
    public final DBSPExpression window;

    // TODO: support the linear version of this operator.
    public DBSPPartitionedRollingAggregateWithWaterlineOperator(
            CalciteObject node,
            DBSPExpression partitioningFunction,
            // Initially 'function' is null, and the 'aggregate' is not.
            // After lowering 'aggregate' is not null, and 'function' has its expected shape
            @Nullable DBSPExpression function,
            @Nullable DBSPAggregate aggregate,
            DBSPExpression window,
            // The output type of partitioned_rolling_aggregate_with_waterline cannot actually be represented
            // using the current IR, so this type is a lie.
            // See DBSPPartitionedRollingAggregateOperator.
            DBSPTypeIndexedZSet outputType,
            DBSPOperator dataInput,
            DBSPOperator waterlineInput) {
        super(node, "partitioned_rolling_aggregate_with_waterline",
                function, outputType, true);
        this.addInput(dataInput);
        this.addInput(waterlineInput);
        this.aggregate = aggregate;
        this.window = window;
        this.partitioningFunction = partitioningFunction;
        assert partitioningFunction.is(DBSPClosureExpression.class);
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPPartitionedRollingAggregateWithWaterlineOperator(
                this.getNode(),
                this.partitioningFunction,
                expression, this.aggregate, this.window,
                outputType.to(DBSPTypeIndexedZSet.class),
                this.inputs.get(0), this.inputs.get(1));
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        assert newInputs.size() == 2: "Expected 2 inputs, got " + newInputs.size();
        if (force || this.inputsDiffer(newInputs))
            return new DBSPPartitionedRollingAggregateWithWaterlineOperator(
                    this.getNode(),
                    this.partitioningFunction, this.function, this.aggregate, this.window,
                    this.getOutputIndexedZSetType(),
                    newInputs.get(0), newInputs.get(1));
        return this;
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPPartitionedRollingAggregateWithWaterlineOperator otherOperator = other.as(DBSPPartitionedRollingAggregateWithWaterlineOperator.class);
        if (otherOperator == null)
            return false;
        return EquivalenceContext.equiv(this.aggregate, otherOperator.aggregate) &&
                this.partitioningFunction.equivalent(otherOperator.partitioningFunction) &&
                this.window.equivalent(otherOperator.window);
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop())
            visitor.postorder(this);
        visitor.pop(this);
    }
}
