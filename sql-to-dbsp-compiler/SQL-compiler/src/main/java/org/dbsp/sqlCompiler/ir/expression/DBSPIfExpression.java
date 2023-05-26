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

package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;

import javax.annotation.Nullable;

public class DBSPIfExpression extends DBSPExpression {
    public final DBSPExpression condition;
    public final DBSPExpression positive;
    public final DBSPExpression negative;

    public DBSPIfExpression(@Nullable Object node, DBSPExpression condition, DBSPExpression positive, DBSPExpression negative) {
        super(node, positive.getNonVoidType());
        this.condition = condition;
        this.positive = positive;
        this.negative = negative;
        if (!this.condition.getNonVoidType().is(DBSPTypeBool.class))
            throw new RuntimeException("Expected a boolean condition type " + condition + " got " +
                    this.condition.getNonVoidType());
        if (this.condition.getNonVoidType().mayBeNull)
            throw new RuntimeException("Nullable condition in if expression " + condition);
        if (!this.positive.getNonVoidType().sameType(this.negative.getNonVoidType()))
            throw new RuntimeException("Mismatched types in conditional expression " + this.positive +
                    "/" + this.positive.getType() + " vs" + this.negative + "/" + this.negative.getType());
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        if (this.type != null)
            this.type.accept(visitor);
        this.condition.accept(visitor);
        this.positive.accept(visitor);
        this.negative.accept(visitor);
        visitor.postorder(this);
    }

}
