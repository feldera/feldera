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

package org.dbsp.sqlCompiler.ir.type;

import org.dbsp.sqlCompiler.ir.InnerVisitor;

import javax.annotation.Nullable;

public class DBSPTypeZSet extends DBSPTypeUser implements ICollectionType {
    public final DBSPType elementType;
    public final DBSPType weightType;

    public DBSPTypeZSet(@Nullable Object node, DBSPType elementType, DBSPType weightType) {
        super(node, "OrdZSet", false, elementType, weightType);
        this.elementType = elementType;
        this.weightType = weightType;
    }

    public DBSPTypeZSet(DBSPType elementType, DBSPType weightType) {
        this(null, elementType, weightType);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        this.elementType.accept(visitor);
        this.weightType.accept(visitor);
        visitor.postorder(this);
    }

    @Override
    public DBSPType getElementType() {
        return this.elementType;
    }

    // sameType and hashCode inherited from TypeUser
}
