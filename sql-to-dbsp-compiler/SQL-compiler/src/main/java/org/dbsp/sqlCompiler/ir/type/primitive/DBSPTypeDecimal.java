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
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.util.UnsupportedException;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.Objects;

public class DBSPTypeDecimal extends DBSPTypeBaseType
        implements IsNumericType {
    public static final int MAX_PRECISION = 38;  // Total digits. Rather arbitrary.
    public static final int MAX_SCALE = 10;       // Digits after decimal period.  Rather arbitrary.

    public final int precision;
    public final int scale;

    public DBSPTypeDecimal(@Nullable Object node, int precision, int scale, boolean mayBeNull) {
        super(node, mayBeNull);
        if (precision <= 0)
            throw new IllegalArgumentException("Precision must be positive: " + precision);
        if (scale < 0)
            // Postgres allows negative scales.
            throw new IllegalArgumentException("Scale must be positive: " + scale);
        if (precision > MAX_PRECISION)
            throw new UnsupportedException(precision + " larger than maximum supported precision " + MAX_PRECISION, node);
        if (scale > MAX_SCALE)
            throw new UnsupportedException(scale + " larger than maximum supported scale " + MAX_SCALE, node);
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public String getRustString() {
        return "Decimal";
    }

    @Override
    public DBSPLiteral getZero() {
        return new DBSPDecimalLiteral(null, this, new BigDecimal(0));
    }

    @Override
    public DBSPLiteral getOne() {
        return new DBSPDecimalLiteral(null, this, new BigDecimal(1));
    }

    @Override
    public DBSPLiteral getMaxValue() {
        throw new UnsupportedException(this);
    }

    @Override
    public DBSPLiteral getMinValue() {
        throw new UnsupportedException(this);
    }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        if (mayBeNull == this.mayBeNull)
            return this;
        return new DBSPTypeDecimal(this.getNode(), this.precision, this.scale, mayBeNull);
    }

    @Override
    public boolean hasCopy() {
        return false;
    }

    @Override
    public String shortName() {
        return "decimal";
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.precision, this.scale);
    }

    @Override
    public DBSPLiteral defaultValue() {
        return this.getZero();
    }

    @Override
    public boolean sameType(@Nullable DBSPType type) {
        if (!super.sameType(type))
            return false;
        assert type != null;
        if (!type.is(DBSPTypeDecimal.class))
            return false;
        DBSPTypeDecimal other = type.to(DBSPTypeDecimal.class);
        return this.scale == other.scale && this.precision == other.precision;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        visitor.postorder(this);
    }
}
