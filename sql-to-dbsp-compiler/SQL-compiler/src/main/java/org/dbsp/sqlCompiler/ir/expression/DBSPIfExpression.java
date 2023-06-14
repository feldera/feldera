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

import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;

import javax.annotation.Nullable;

@NonCoreIR
public class DBSPIfExpression extends DBSPExpression {
    public final DBSPExpression condition;
    public final DBSPExpression positive;
    public final DBSPExpression negative;

    public DBSPIfExpression(@Nullable Object node, DBSPExpression condition, DBSPExpression positive, DBSPExpression negative) {
        super(node, positive.getType());
        this.condition = condition;
        this.positive = positive;
        this.negative = negative;
        if (!this.condition.getType().is(DBSPTypeBool.class))
            throw new RuntimeException("Expected a boolean condition type " + condition + " got " +
                    this.condition.getType());
        if (this.condition.getType().mayBeNull)
            throw new RuntimeException("Nullable condition in if expression " + condition);
        if (!this.positive.getType().sameType(this.negative.getType()))
            throw new RuntimeException("Mismatched types in conditional expression " + this.positive +
                    "/" + this.positive.getType() + " vs" + this.negative + "/" + this.negative.getType());
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.push(this);
        this.type.accept(visitor);
        this.condition.accept(visitor);
        this.positive.accept(visitor);
        this.negative.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPIfExpression o = other.as(DBSPIfExpression.class);
        if (o == null)
            return false;
        return this.condition == o.condition &&
                this.positive == o.positive &&
                this.negative == o.negative &&
                this.hasSameType(o);
    }
}
