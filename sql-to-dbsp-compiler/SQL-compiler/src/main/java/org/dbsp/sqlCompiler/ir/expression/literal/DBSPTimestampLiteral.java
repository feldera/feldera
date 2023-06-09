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

import org.apache.calcite.util.TimestampString;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;

import javax.annotation.Nullable;
import java.util.Objects;

public class DBSPTimestampLiteral extends DBSPLiteral {
    @Nullable public final Long value;

    public DBSPTimestampLiteral(@Nullable Object node, DBSPType type, @Nullable Long value) {
        super(node, type, value == null);
        this.value = value;
    }

    public DBSPTimestampLiteral(@Nullable Object node, DBSPType type, TimestampString value) {
        this(node, type, value.getMillisSinceEpoch());
    }

    public DBSPTimestampLiteral(long value) {
        this(null, DBSPTypeTimestamp.INSTANCE, value);
    }

    public DBSPTimestampLiteral() {
        this(null, DBSPTypeTimestamp.NULLABLE_INSTANCE, (Long)null);
    }

    static TimestampString createTimestampString(String timestamp) {
        // TimestampString is too smart: it does not accept a number that ends in 0 if there is a decimal point
        String[] parts = timestamp.split("[.]");
        TimestampString result = new TimestampString(parts[0]);
        if (parts.length == 1)
            return result;
        return result.withFraction(parts[1]);
    }

    @Override
    public boolean sameValue(@Nullable DBSPLiteral o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPTimestampLiteral that = (DBSPTimestampLiteral) o;
        return Objects.equals(value, that.value);
    }

    public DBSPTimestampLiteral(String string, boolean mayBeNull) {
        this(null, DBSPTypeTimestamp.INSTANCE.setMayBeNull(mayBeNull), createTimestampString(string));
    }

    public DBSPTimestampLiteral(long value, boolean mayBeNull) {
        this(null, DBSPTypeTimestamp.INSTANCE.setMayBeNull(mayBeNull), value);
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
        return new DBSPTimestampLiteral(this.getNode(), this.getNonVoidType().setMayBeNull(false), this.value);
    }
}
