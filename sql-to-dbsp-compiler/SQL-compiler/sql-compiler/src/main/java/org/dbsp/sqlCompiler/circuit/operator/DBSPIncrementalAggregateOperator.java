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

import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeIndexedZSet;

import javax.annotation.Nullable;
import java.util.List;

public class DBSPIncrementalAggregateOperator extends DBSPAggregateOperatorBase {
    public final DBSPType keyType;
    public final DBSPType outputElementType;
    public final DBSPType weightType;

    public DBSPIncrementalAggregateOperator(
            CalciteObject node,
            DBSPType keyType, DBSPType outputElementType, DBSPType weightType,
            @Nullable DBSPExpression function,
            @Nullable DBSPAggregate aggregate, DBSPOperator input, boolean isLinear) {
        super(node, isLinear ? "aggregate_linear" : "aggregate",
                new DBSPTypeIndexedZSet(node, keyType, outputElementType, weightType),
                function, aggregate, false, input, isLinear);
        this.keyType = keyType;
        this.outputElementType = outputElementType;
        this.weightType = weightType;
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.postorder(this);
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        DBSPType outputElementType = outputType.to(DBSPTypeIndexedZSet.class).elementType;
        return new DBSPIncrementalAggregateOperator(
                this.getNode(), this.keyType, outputElementType, this.weightType,
                expression, this.aggregate, this.input(), this.isLinear);
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPIncrementalAggregateOperator(
                    this.getNode(), this.keyType, this.outputElementType, this.weightType,
                    this.function, this.aggregate, newInputs.get(0), this.isLinear);
        return this;
    }
}
