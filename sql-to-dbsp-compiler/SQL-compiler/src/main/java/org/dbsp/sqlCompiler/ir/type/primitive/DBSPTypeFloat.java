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

import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPFloatLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;

import java.util.Objects;

public class DBSPTypeFloat extends DBSPTypeFP implements IsNumericType {
    protected DBSPTypeFloat(CalciteObject node, boolean mayBeNull) { super(node, mayBeNull); }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return new DBSPTypeFloat(this.getNode(), mayBeNull);
    }

    @Override
    public String shortName() {
        return "f";
    }

    public static final DBSPTypeFloat INSTANCE =new DBSPTypeFloat(new CalciteObject(),false);
    public static final DBSPTypeFloat NULLABLE_INSTANCE = new DBSPTypeFloat(new CalciteObject(),true);

    @Override
    public boolean sameType(DBSPType type) {
        if (!super.sameNullability(type))
            return false;
        return type.is(DBSPTypeFloat.class);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.mayBeNull, 5);
    }

    @Override
    public int getWidth() {
        return 32;
    }

    @Override
    public DBSPLiteral getZero() {
        return new DBSPFloatLiteral(0F, this.mayBeNull);
    }

    @Override
    public DBSPLiteral getOne() {
        return new DBSPFloatLiteral(1F, this.mayBeNull);
    }

    @Override
    public DBSPLiteral getMaxValue() {
        return new DBSPFloatLiteral(Float.MAX_VALUE, this.mayBeNull);
    }

    @Override
    public DBSPLiteral getMinValue() {
        return new DBSPFloatLiteral(Float.MIN_VALUE, this.mayBeNull);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }
}
