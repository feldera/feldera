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

import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.*;

import javax.annotation.Nullable;
import java.util.List;

/**
 * This operator does not correspond to any standard DBSP operator currently.
 * It is implemented as a sequence of 2 DBSP operators: partitioned_rolling_aggregate and
 * map_index.
 */
public class DBSPWindowAggregateOperator extends DBSPAggregateOperatorBase {
    public final DBSPType partitionKeyType;
    public final DBSPType timestampType;
    public final DBSPType aggregateType;
    public final DBSPType weightType;
    public final DBSPExpression window;

    public DBSPWindowAggregateOperator(
            @Nullable Object node,
            @Nullable DBSPExpression function, @Nullable DBSPAggregate aggregate,
            DBSPExpression window,
            DBSPType partitionKeyType, DBSPType timestampType, DBSPType aggregateType, DBSPType weightType,
            DBSPOperator input) {
        super(node, "window_aggregate",
                new DBSPTypeIndexedZSet(node,
                        new DBSPTypeRawTuple(partitionKeyType, timestampType), aggregateType, weightType),
                function, aggregate,
                true, input);
        this.window = window;
        this.partitionKeyType = partitionKeyType;
        this.timestampType = timestampType;
        this.aggregateType = aggregateType;
        this.weightType = weightType;
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        DBSPTypeIndexedZSet ixOutputType = outputType.to(DBSPTypeIndexedZSet.class);
        DBSPTypeRawTuple outputTuple = ixOutputType.keyType.to(DBSPTypeRawTuple.class);
        if (outputTuple.tupFields.length != 2)
            throw new RuntimeException("Expected two fields in output element type " + outputTuple);
        return new DBSPWindowAggregateOperator(
                this.getNode(), expression, this.aggregate, this.window,
                outputTuple.tupFields[0], outputTuple.tupFields[1],
                ixOutputType.elementType, ixOutputType.weightType,
                this.input());
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPWindowAggregateOperator(
                    this.getNode(), this.function, this.aggregate, this.window,
                    this.partitionKeyType, this.timestampType, this.aggregateType, this.weightType,
                    newInputs.get(0));
        return this;
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        if (!visitor.preorder(this)) return;
        visitor.postorder(this);
    }
}
