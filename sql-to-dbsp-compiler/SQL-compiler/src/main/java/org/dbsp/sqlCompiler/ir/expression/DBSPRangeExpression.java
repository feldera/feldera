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
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeUser;

import javax.annotation.Nullable;

/**
 * A rust expression that represents a range left .. right
 */
@SuppressWarnings("GrazieInspection")
public class DBSPRangeExpression extends DBSPExpression {
    @Nullable
    public final DBSPExpression left;
    @Nullable
    public final DBSPExpression right;
    public final boolean endInclusive;

    public DBSPRangeExpression(@Nullable DBSPExpression left, @Nullable DBSPExpression right, boolean endInclusive, DBSPType type) {
        super(null, new DBSPTypeUser(null, "Range", false, type));
        this.left = left;
        this.right = right;
        this.endInclusive = endInclusive;
        if (this.left != null && !this.left.getNonVoidType().sameType(type))
            throw new RuntimeException("Range expression type mismatch " + this.left + " vs " + type);
        if (this.right != null && !this.right.getNonVoidType().sameType(type))
            throw new RuntimeException("Range expression type mismatch " + this.right + " vs " + type);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        if (this.type != null)
            this.type.accept(visitor);
        if (this.left != null)
            this.left.accept(visitor);
        if (this.right != null)
            this.right.accept(visitor);
        visitor.postorder(this);
    }
}
