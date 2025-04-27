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
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

public final class DBSPStreamAggregateOperator extends DBSPAggregateOperatorBase {
    public DBSPStreamAggregateOperator(CalciteRelNode node,
                                       DBSPTypeIndexedZSet outputType,
                                       @Nullable DBSPExpression function,
                                       @Nullable DBSPAggregate aggregate,
                                       OutputPort input) {
        super(node, "stream_aggregate",
                outputType, function, aggregate, false, input, false);
        Utilities.enforce(aggregate == null || !aggregate.isLinear());
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
    public DBSPSimpleOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        DBSPTypeIndexedZSet ixOutputType = outputType.to(DBSPTypeIndexedZSet.class);
        return new DBSPStreamAggregateOperator(this.getRelNode(),
                ixOutputType, expression, this.aggregate, this.input())
                .copyAnnotations(this);
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPStreamAggregateOperator(
                    this.getRelNode(), this.getOutputIndexedZSetType(),
                    this.function, this.aggregate, newInputs.get(0))
                    .copyAnnotations(this);
        return this;
    }

    // equivalent inherited from base class

    @SuppressWarnings("unused")
    public static DBSPStreamAggregateOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        DBSPExpression function = info.function();
        DBSPAggregate aggregate = null;
        if (node.has("aggregate"))
            aggregate = fromJsonInner(node, "aggregate", decoder, DBSPAggregate.class);
        return new DBSPStreamAggregateOperator(CalciteEmptyRel.INSTANCE,
                info.getIndexedZsetType(), function, aggregate, info.getInput(0))
                .addAnnotations(info.annotations(), DBSPStreamAggregateOperator.class);
    }
}
