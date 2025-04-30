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

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPWindowBoundExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

public final class DBSPPartitionedRollingAggregateWithWaterlineOperator
        extends DBSPBinaryOperator {
    @Nullable
    public final DBSPAggregateList aggregate;
    public final DBSPClosureExpression partitioningFunction;
    public final DBSPWindowBoundExpression lower;
    public final DBSPWindowBoundExpression upper;

    // TODO: support the linear version of this operator.
    public DBSPPartitionedRollingAggregateWithWaterlineOperator(
            CalciteRelNode node,
            DBSPClosureExpression partitioningFunction,
            // Initially 'function' is null, and the 'aggregate' is not.
            // After lowering 'aggregate' is not null, and 'function' has its expected shape
            @Nullable DBSPExpression function,
            @Nullable DBSPAggregateList aggregate,
            DBSPWindowBoundExpression lower,
            DBSPWindowBoundExpression upper,
            // The output type of partitioned_rolling_aggregate_with_waterline cannot actually be represented
            // using the current IR, so this type is a lie.
            // See DBSPPartitionedRollingAggregateOperator.
            DBSPTypeIndexedZSet outputType,
            OutputPort dataInput, OutputPort waterlineInput) {
        super(node, "partitioned_rolling_aggregate_with_waterline",
                function, outputType, true, dataInput, waterlineInput, true);
        this.aggregate = aggregate;
        this.lower = lower;
        this.upper = upper;
        this.partitioningFunction = partitioningFunction;
        Utilities.enforce(partitioningFunction.is(DBSPClosureExpression.class));
    }

    @Override
    public DBSPSimpleOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPPartitionedRollingAggregateWithWaterlineOperator(
                this.getRelNode(),
                this.partitioningFunction,
                expression, this.aggregate, this.lower, this.upper,
                outputType.to(DBSPTypeIndexedZSet.class),
                this.left(), this.right()).copyAnnotations(this);
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        Utilities.enforce(newInputs.size() == 2, "Expected 2 inputs, got " + newInputs.size());
        if (force || this.inputsDiffer(newInputs))
            return new DBSPPartitionedRollingAggregateWithWaterlineOperator(
                    this.getRelNode(),
                    this.partitioningFunction, this.function, this.aggregate,
                    this.lower, this.upper, this.getOutputIndexedZSetType(),
                    newInputs.get(0), newInputs.get(1)).copyAnnotations(this);
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
                EquivalenceContext.equiv(this.function, otherOperator.function) &&
                this.partitioningFunction.equivalent(otherOperator.partitioningFunction) &&
                this.lower.equivalent(otherOperator.lower) &&
                this.upper.equivalent(otherOperator.upper);
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop())
            visitor.postorder(this);
        visitor.pop(this);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        super.accept(visitor);
        visitor.property("partitioningFunction");
        this.partitioningFunction.accept(visitor);
        if (this.aggregate != null) {
            visitor.property("aggregate");
            this.aggregate.accept(visitor);
        }
        visitor.property("lower");
        this.lower.accept(visitor);
        visitor.property("upper");
        this.upper.accept(visitor);
    }

    @SuppressWarnings("unused")
    public static DBSPPartitionedRollingAggregateWithWaterlineOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        DBSPClosureExpression partitioningFunction = fromJsonInner(
                node, "partitioningFunction", decoder, DBSPClosureExpression.class);
        DBSPAggregateList aggregate = null;
        if (node.has("aggregate"))
            aggregate = fromJsonInner(node, "aggregate", decoder, DBSPAggregateList.class);
        DBSPWindowBoundExpression lower = fromJsonInner(node, "lower", decoder, DBSPWindowBoundExpression.class);
        DBSPWindowBoundExpression upper = fromJsonInner(node, "upper", decoder, DBSPWindowBoundExpression.class);
        return new DBSPPartitionedRollingAggregateWithWaterlineOperator(
                CalciteEmptyRel.INSTANCE, partitioningFunction, info.function(),
                aggregate, lower, upper, info.getIndexedZsetType(), info.getInput(0), info.getInput(1))
                .addAnnotations(info.annotations(), DBSPPartitionedRollingAggregateWithWaterlineOperator.class);
    }

    @Override
    public DBSPType outputStreamType(int outputNo, boolean outerCircuit) {
        Utilities.enforce(outputNo == 0);
        Utilities.enforce(outerCircuit);
        DBSPType[] args = new DBSPType[3];
        DBSPTypeRawTuple pfOut = this.partitioningFunction.getResultType().to(DBSPTypeRawTuple.class);
        args[0] = pfOut.tupFields[0];
        args[1] = this.lower.type;
        if (this.aggregate != null) {
            args[2] = this.aggregate.getType();
        } else {
            DBSPExpression expr = this.getFunction();
            args[2] = expr.getType().to(DBSPTypeFunction.class).resultType;
        }
        return new DBSPTypeUser(this.getRelNode(), DBSPTypeCode.USER,
                "OrdPartitionedOverStream", false, args);
    }
}
