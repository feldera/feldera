/*
 * Copyright 2023 VMware, Inc.
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

import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeVec;
import org.dbsp.util.UnsupportedException;

import javax.annotation.Nullable;

/**
 * Index within an array.
 */
public class DBSPIndexExpression extends DBSPExpression {
    public final DBSPExpression array;
    public final DBSPExpression index;
    // SQL indexes start at 1!
    public final boolean startsAtOne;

    static DBSPType getVecElementType(DBSPType type) {
        if (type.is(DBSPTypeAny.class))
            return type;
        if (type.is(DBSPTypeVec.class)) {
            return type.to(DBSPTypeVec.class).getElementType();
        }
        throw new UnsupportedException(type);
    }

    public DBSPIndexExpression(@Nullable Object node, DBSPExpression array, DBSPExpression index, boolean startsAtOne) {
        super(node, getVecElementType(array.getNonVoidType()));
        this.array = array;
        this.index = index;
        this.startsAtOne = startsAtOne;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.push(this);
        if (this.type != null)
            this.type.accept(visitor);
        this.array.accept(visitor);
        this.index.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }
}
