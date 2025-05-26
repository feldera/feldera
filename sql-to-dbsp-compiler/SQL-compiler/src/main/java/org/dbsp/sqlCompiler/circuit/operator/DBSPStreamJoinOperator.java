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
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public final class DBSPStreamJoinOperator extends DBSPJoinBaseOperator {
    public DBSPStreamJoinOperator(CalciteRelNode node, DBSPTypeZSet outputType,
                                  // Closure from key, valueLeft, valueRight to result type
                                  DBSPExpression function, boolean isMultiset,
                                  OutputPort left, OutputPort right) {
        super(node, "stream_join", function, outputType, isMultiset, left, right);
        DBSPType elementResultType = this.getOutputZSetElementType();
        this.checkResultType(function, elementResultType);
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
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType)) {
            return new DBSPStreamJoinOperator(
                    this.getRelNode(), outputType.to(DBSPTypeZSet.class),
                    Objects.requireNonNull(function),
                    this.isMultiset, newInputs.get(0), newInputs.get(1)).copyAnnotations(this);
        }
        return this;
    }

    // equivalent inherited from base class

    @SuppressWarnings("unused")
    public static DBSPStreamJoinOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        return new DBSPStreamJoinOperator(CalciteEmptyRel.INSTANCE, info.getZsetType(),
                info.getFunction(), info.isMultiset(), info.getInput(0), info.getInput(1))
                .addAnnotations(info.annotations(), DBSPStreamJoinOperator.class);
    }
}
