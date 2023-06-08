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
import org.dbsp.sqlCompiler.ir.type.DBSPTypeVec;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents a (constant) vector described by its elements.
 */
public class DBSPVecLiteral extends DBSPLiteral implements IDBSPContainer {
    @Nullable
    public final List<DBSPExpression> data;
    public final DBSPTypeVec vecType;

    public DBSPVecLiteral(DBSPType elementType) {
        super(null, new DBSPTypeVec(elementType), false);
        this.data = new ArrayList<>();
        this.vecType = this.getNonVoidType().to(DBSPTypeVec.class);
    }

    public DBSPVecLiteral(DBSPType elementType, boolean isNull) {
        super(null, new DBSPTypeVec(elementType), isNull);
        this.data = null;
        this.vecType = this.getNonVoidType().to(DBSPTypeVec.class);
    }

    public DBSPVecLiteral(@Nullable Object node, DBSPType type, @Nullable List<DBSPExpression> data) {
        super(node, type, false);
        this.vecType = this.getNonVoidType().to(DBSPTypeVec.class);
        this.data = data;
        if (data != null) {
            for (DBSPExpression e : data) {
                if (!e.getNonVoidType().sameType(data.get(0).getNonVoidType()))
                    throw new RuntimeException("Not all values of set have the same type:" +
                            e.getType() + " vs " + data.get(0).getType());
                this.add(e);
            }
        }
    }

    public DBSPVecLiteral(DBSPExpression... data) {
        super(null, new DBSPTypeVec(data[0].getNonVoidType()), false);
        this.vecType = this.getNonVoidType().to(DBSPTypeVec.class);
        this.data = new ArrayList<>();
        for (DBSPExpression e: data) {
            if (!e.getNonVoidType().sameType(data[0].getNonVoidType()))
                throw new RuntimeException("Not all values of set have the same type:" +
                    e.getType() + " vs " + data[0].getType());
            this.add(e);
        }
    }

    public DBSPType getElementType() {
        return this.vecType.getTypeArg(0);
    }

    public void add(DBSPExpression expression) {
        // We expect the expression to be a constant value (a literal)
        if (!expression.getNonVoidType().sameType(this.getElementType()))
            throw new RuntimeException("Added element " + expression + " type " +
                    expression.getType() + " does not match vector type " + this.getElementType());
        Objects.requireNonNull(this.data).add(expression);
    }

    public void add(DBSPVecLiteral other) {
        if (!this.getNonVoidType().sameType(other.getNonVoidType()))
            throw new RuntimeException("Added vectors do not have the same type " +
                    this.getElementType() + " vs " + other.getElementType());
        Objects.requireNonNull(other.data).forEach(this::add);
    }

    public int size() {
        return Objects.requireNonNull(this.data).size();
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        visitor.push(this);
        for (DBSPExpression expr: Objects.requireNonNull(this.data))
            expr.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public DBSPLiteral getNonNullable() {
        return this;
    }

    @Override
    public boolean sameValue(@Nullable DBSPLiteral o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPVecLiteral that = (DBSPVecLiteral) o;
        if (!Objects.equals(data, that.data)) return false;
        return vecType.equals(that.vecType);
    }
}
