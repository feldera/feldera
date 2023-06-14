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

package org.dbsp.sqlCompiler.ir.type.primitive;

import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;

import javax.annotation.Nullable;
import java.util.Objects;

public class DBSPTypeDouble extends DBSPTypeFP implements IsNumericType {
    protected DBSPTypeDouble(@Nullable Object node, boolean mayBeNull) { super(node, mayBeNull); }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return new DBSPTypeDouble(this.getNode(), mayBeNull);
    }

    @Override
    public String shortName() {
        return "d";
    }

    public static final DBSPTypeDouble INSTANCE = new DBSPTypeDouble(null,false);
    public static final DBSPTypeDouble NULLABLE_INSTANCE = new DBSPTypeDouble(null,true);

    @Override
    public boolean sameType(DBSPType type) {
        if (!super.sameNullability(type))
            return false;
        return type.is(DBSPTypeDouble.class);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.mayBeNull, 4);
    }

    @Override
    public int getWidth() {
        return 64;
    }

    @Override
    public DBSPLiteral getZero() {
        return new DBSPDoubleLiteral(0D, this.mayBeNull);
    }

    @Override
    public DBSPLiteral getOne() {
        return new DBSPDoubleLiteral(1D, this.mayBeNull);
    }

    @Override
    public DBSPLiteral getMaxValue() {
        return new DBSPDoubleLiteral(Double.MAX_VALUE, this.mayBeNull);
    }

    @Override
    public DBSPLiteral getMinValue() {
        return new DBSPDoubleLiteral(Double.MIN_VALUE, this.mayBeNull);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }
}
