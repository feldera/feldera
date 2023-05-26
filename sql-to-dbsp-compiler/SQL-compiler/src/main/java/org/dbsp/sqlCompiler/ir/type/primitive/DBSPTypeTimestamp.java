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
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsDateType;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.util.UnsupportedException;

import javax.annotation.Nullable;
import java.util.Objects;

public class DBSPTypeTimestamp extends DBSPTypeBaseType
        implements IsNumericType, IsDateType {
    public static final DBSPTypeTimestamp INSTANCE =new DBSPTypeTimestamp(null, false);
    public static final DBSPTypeTimestamp NULLABLE_INSTANCE = new DBSPTypeTimestamp(null, true);

    protected DBSPTypeTimestamp(@Nullable Object node, boolean mayBeNull) {
        super(node, mayBeNull);
    }

    @Override
    public String shortName() {
        return "Timestamp";
    }

    @Override
    public DBSPLiteral defaultValue() {
        return this.getZero();
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        visitor.postorder(this);
    }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return new DBSPTypeTimestamp(this.getNode(), mayBeNull);
    }

    public String getRustString() {
        return "Timestamp";
    }

    @Override
    public DBSPLiteral getZero() {
        return new DBSPTimestampLiteral(0);
    }

    @Override
    public DBSPLiteral getOne() {
        throw new UnsupportedException(this);
    }

    @Override
    public DBSPLiteral getMaxValue() {
        return new DBSPTimestampLiteral(Long.MAX_VALUE);
    }

    @Override
    public DBSPLiteral getMinValue() {
        throw new UnsupportedException(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.mayBeNull, 14);
    }

    @Override
    public boolean sameType(@Nullable DBSPType other) {
        if (!super.sameType(other))
            return false;
        assert other != null;
        return other.is(DBSPTypeTimestamp.class);
    }
}
