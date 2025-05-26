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

public final class DBSPFlatMapOperator extends DBSPUnaryOperator {
    public DBSPFlatMapOperator(CalciteRelNode node, DBSPExpression expression,
                               DBSPTypeZSet outputType, boolean isMultiset, OutputPort input) {
        super(node, "flat_map", expression, outputType, isMultiset, input);
        checkArgumentFunctionType(expression, input);
    }

    // When implementing UNNEST, initially the expression is DBSPFlatmap, but later it is lowered
    // into a lambda of the form
    // |x| -> {
    //   let array: &Vec<i32> = &(*x).A;
    //   let array_clone: Vec<i32> = (*array).clone();
    //   array_clone.clone().into_iter().map(move |e: X| ... )
    // }
    public DBSPFlatMapOperator(CalciteRelNode node, DBSPExpression expression,
                               DBSPTypeZSet outputType, OutputPort input) {
        this(node, expression, outputType, true, input);
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
            return new DBSPFlatMapOperator(
                    this.getRelNode(), Objects.requireNonNull(function),
                    outputType.to(DBSPTypeZSet.class), newInputs.get(0))
                    .copyAnnotations(this);
        }
        return this;
    }

    @SuppressWarnings("unused")
    public static DBSPFlatMapOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        return new DBSPFlatMapOperator(CalciteEmptyRel.INSTANCE, info.getFunction(),
                info.getZsetType(), info.isMultiset(), info.getInput(0))
                .addAnnotations(info.annotations(), DBSPFlatMapOperator.class);
    }
}
