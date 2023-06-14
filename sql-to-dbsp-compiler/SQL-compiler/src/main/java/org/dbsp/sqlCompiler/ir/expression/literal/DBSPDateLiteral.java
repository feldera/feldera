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

import org.apache.calcite.util.DateString;
import org.dbsp.sqlCompiler.circuit.IDBSPNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDate;

import javax.annotation.Nullable;
import java.util.Objects;

public class DBSPDateLiteral extends DBSPLiteral {
    @Nullable public final Integer value;

    public DBSPDateLiteral(@Nullable Object node, DBSPType type, @Nullable Integer value) {
        super(node, type, value == null);
        this.value = value;
    }

    public DBSPDateLiteral(@Nullable Object node, DBSPType type, DateString value) {
        this(node, type, value.getDaysSinceEpoch());
    }

    public DBSPDateLiteral(String value, boolean mayBeNull) {
        this(null, DBSPTypeDate.INSTANCE.setMayBeNull(mayBeNull), new DateString(value).getDaysSinceEpoch());
    }

    public DBSPDateLiteral(String value) {
        this(value, false);
    }

    /**
     * A NULL date.
     */
    public DBSPDateLiteral() {
        this(null, DBSPTypeDate.NULLABLE_INSTANCE, (Integer)null);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public DBSPLiteral getNonNullable() {
        return new DBSPDateLiteral(this.getNode(), this.getNonVoidType().setMayBeNull(false), this.value);
    }

    @Override
    public boolean sameValue(@Nullable DBSPLiteral o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPDateLiteral that = (DBSPDateLiteral) o;
        return Objects.equals(value, that.value);
    }
}
