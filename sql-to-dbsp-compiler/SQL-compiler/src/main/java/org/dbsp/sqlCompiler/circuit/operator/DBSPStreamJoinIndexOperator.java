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
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** Currently there is no corespondent operator in DBSP, so an attept to generate
 * Rust for this operator will fail.   See {@link DBSPStreamJoinOperator} for
 * the function signature. */
public final class DBSPStreamJoinIndexOperator extends DBSPBinaryOperator {
    public DBSPStreamJoinIndexOperator(
            CalciteObject node, DBSPTypeIndexedZSet outputType,
            DBSPExpression function, boolean isMultiset,
            DBSPOperator left, DBSPOperator right) {
        super(node, "full_join_index", function, outputType, isMultiset, left, right);
        assert left.getOutputIndexedZSetType().keyType.sameType(right.getOutputIndexedZSetType().keyType);
    }

    public DBSPType getKeyType() {
        return left().getOutputIndexedZSetType().keyType;
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPStreamJoinIndexOperator(
                this.getNode(), outputType.to(DBSPTypeIndexedZSet.class),
                Objects.requireNonNull(expression),
                this.isMultiset, this.left(), this.right()).copyAnnotations(this);
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPStreamJoinIndexOperator(
                    this.getNode(), this.getOutputIndexedZSetType(),
                    this.getFunction(), this.isMultiset, newInputs.get(0), newInputs.get(1))
                    .copyAnnotations(this);
        return this;
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
