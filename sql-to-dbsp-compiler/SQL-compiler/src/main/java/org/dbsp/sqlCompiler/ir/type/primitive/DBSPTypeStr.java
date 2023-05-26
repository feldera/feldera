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

import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.UnsupportedException;

import javax.annotation.Nullable;
import java.util.Objects;

public class DBSPTypeStr extends DBSPTypeBaseType {
    public static final DBSPTypeStr INSTANCE =new DBSPTypeStr(null,false);

    protected DBSPTypeStr(@Nullable Object node, boolean mayBeNull) { super(node, mayBeNull); }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return new DBSPTypeStr(this.getNode(), mayBeNull);
    }

    @Override
    public String shortName() {
        return "str";
    }

    @Override
    public DBSPLiteral defaultValue() {
        throw new UnsupportedException("default value for 'str' type");
    }

    @Override
    public boolean sameType(@Nullable DBSPType type) {
        if (!super.sameType(type))
            return false;
        assert type != null;
        return type.is(DBSPTypeStr.class);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.mayBeNull, 11);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        visitor.postorder(this);
    }
}
