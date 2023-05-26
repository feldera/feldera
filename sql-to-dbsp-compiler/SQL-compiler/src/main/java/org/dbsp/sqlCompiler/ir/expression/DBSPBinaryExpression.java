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

import javax.annotation.Nullable;

public class DBSPBinaryExpression extends DBSPExpression {
    public final DBSPExpression left;
    public final DBSPExpression right;
    public final DBSPOpcode operation;
    /**
     * If 'true' it means that this is a native arithmetic operation (e.g., +).
     * Otherwise this operation generates a runtime call (e.g., plus_i64_i64n).
     * Primitive operations are only used to generate the sources for the genlib
     * runtime library.
     */
    public final boolean primitive;

    public DBSPBinaryExpression(@Nullable Object node, DBSPType type, DBSPOpcode operation,
                                DBSPExpression left, DBSPExpression right, boolean primitive) {
        super(node, type);
        this.operation = operation;
        this.left = left;
        this.right = right;
        this.primitive = primitive;
    }

    public DBSPBinaryExpression(@Nullable Object node, DBSPType type, DBSPOpcode operation,
                                DBSPExpression left, DBSPExpression right) {
        this(node, type, operation, left, right, false);
    }

    public DBSPBinaryExpression(DBSPType type, DBSPOpcode operation,
                                DBSPExpression left, DBSPExpression right, boolean primitive) {
        this(null, type, operation, left, right, primitive);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        if (this.type != null)
            this.type.accept(visitor);
        this.left.accept(visitor);
        this.right.accept(visitor);
        visitor.postorder(this);
    }
}
