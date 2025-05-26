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
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.List;

public final class DBSPSumOperator extends DBSPSimpleOperator {
    public DBSPSumOperator(CalciteRelNode node, List<OutputPort> inputs) {
        super(node, "sum", null, inputs.get(0).outputType(), true, false);
        for (OutputPort op: inputs) {
            this.addInput(op);
            if (!op.outputType().sameType(this.outputType)) {
                throw new InternalCompilerError("Sum operator input type " + op.outputType() +
                        " does not match output type " + this.outputType, this);
            }
        }
    }

    public DBSPSumOperator(CalciteRelNode node, OutputPort... inputs) {
        this(node, Linq.list(inputs));
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
            @Nullable DBSPExpression unused, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        boolean different = force;
        if (newInputs.size() != this.inputs.size())
            // Sum can have any number of inputs
            different = true;
        if (!different) {
            different = this.inputsDiffer(newInputs);
        }
        if (different)
            return new DBSPSumOperator(this.getRelNode(), newInputs).copyAnnotations(this);
        return this;
    }

    @SuppressWarnings("unused")
    public static DBSPSumOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        return new DBSPSumOperator(CalciteEmptyRel.INSTANCE, info.inputs())
                .addAnnotations(info.annotations(), DBSPSumOperator.class);
    }
}
