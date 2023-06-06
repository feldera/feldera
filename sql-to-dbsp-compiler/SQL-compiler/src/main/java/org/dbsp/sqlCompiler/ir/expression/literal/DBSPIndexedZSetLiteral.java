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

package org.dbsp.sqlCompiler.ir.expression.literal;

import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.IDBSPContainer;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeIndexedZSet;
import org.dbsp.util.Unimplemented;

import javax.annotation.Nullable;

/**
 * Represents a (constant) IndexedZSet described by its elements.
 * An IndexedZSet is a map from keys to tuples to integer weights.
 * Currently, we only support empty indexed zsets since we found no
 * need for other constants yet.
 */
public class DBSPIndexedZSetLiteral extends DBSPLiteral implements IDBSPContainer {
    public final DBSPTypeIndexedZSet indexedZSetType;

    public DBSPIndexedZSetLiteral(@Nullable Object node, DBSPType type) {
        super(node, type, false);
        this.indexedZSetType = this.getNonVoidType().to(DBSPTypeIndexedZSet.class);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        visitor.postorder(this);
    }

    @Override
    public DBSPLiteral getNonNullable() {
        return this;
    }

    public int size() {
        return 0;
    }

    public DBSPType getElementType() {
        return this.indexedZSetType.elementType;
    }

    @Override
    public boolean sameValue(@Nullable DBSPLiteral o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPIndexedZSetLiteral that = (DBSPIndexedZSetLiteral) o;
        if (!this.indexedZSetType.sameType(that.indexedZSetType)) return false;
        return true;
    }

    @Override
    public void add(DBSPExpression expression) {
        throw new Unimplemented();
    }
}
