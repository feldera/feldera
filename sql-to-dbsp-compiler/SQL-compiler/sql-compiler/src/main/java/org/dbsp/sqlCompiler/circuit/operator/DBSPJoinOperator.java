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
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeZSet;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class DBSPJoinOperator extends DBSPOperator {
    public final DBSPType elementResultType;
    public final DBSPType weightType;

    public DBSPJoinOperator(CalciteObject node, DBSPType elementResultType, DBSPType weightType,
                            // Closure from key, valueLeft, valueRight to result type
                            DBSPExpression function, boolean isMultiset,
                            DBSPOperator left, DBSPOperator right) {
        super(node, "stream_join", function, TypeCompiler.makeZSet(elementResultType, weightType), isMultiset);
        this.addInput(left);
        this.addInput(right);
        this.elementResultType = elementResultType;
        this.checkResultType(function, elementResultType);
        this.weightType = weightType;
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.postorder(this);
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        DBSPTypeZSet zOutputType = outputType.to(DBSPTypeZSet.class);
        return new DBSPJoinOperator(
                this.getNode(), zOutputType.elementType, zOutputType.weightType,
                Objects.requireNonNull(expression),
                this.isMultiset, this.inputs.get(0), this.inputs.get(1));
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPJoinOperator(
                    this.getNode(), this.elementResultType, this.weightType,
                    this.getFunction(), this.isMultiset, newInputs.get(0), newInputs.get(1));
        return this;
    }
}
