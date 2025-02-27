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

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.Objects;

/** Corresponds to the static DECIMAL(p, s) type in Rust. */
public class DBSPTypeDecimal extends DBSPTypeBaseType
        implements IsNumericType {
    public static final int MAX_PRECISION = 28;   // Total digits. This limit comes from the Rust decimal library
    public static final int MAX_SCALE = 10;       // Digits after decimal period.

    public final int precision;
    public final int scale;

    public DBSPTypeDecimal(CalciteObject node, int precision, int scale, boolean mayBeNull) {
        super(node, DBSPTypeCode.DECIMAL, mayBeNull);
        this.precision = precision;
        this.scale = scale;
        if (precision <= 0)
            throw new IllegalArgumentException(this + ": precision must be positive: " + precision);
        if (scale < 0)
            // Postgres allows negative scales.
            throw new IllegalArgumentException(this + ": scale must be positive: " + scale);
        if (precision > MAX_PRECISION)
            throw new UnsupportedException(this + ": precision " + precision +
                            " larger than maximum supported precision " + MAX_PRECISION, node);
        if (scale > MAX_SCALE)
            throw new UnsupportedException(this + ": scale " + scale
                    + " larger than maximum supported scale " + MAX_SCALE, node);
        if (scale > precision)
            throw new UnsupportedException(this + ": scale " + scale + " larger than precision "
                    + precision, node);
    }

    public static DBSPTypeDecimal getDefault() {
        return new DBSPTypeDecimal(CalciteObject.EMPTY, MAX_PRECISION, 10, false);
    }

    @Override
    public DBSPLiteral getZero() {
        return new DBSPDecimalLiteral(this.getNode(), this, new BigDecimal(0));
    }

    @Override
    public DBSPLiteral getOne() {
        return new DBSPDecimalLiteral(this.getNode(), this, new BigDecimal(1));
    }

    @Override
    public int getPrecision() {
        return this.precision;
    }

    String getMaxLiteral() {
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < this.precision; i++) {
            str.append("9");
            if (i == this.scale)
                str.append(".");
        }
        return str.toString();
    }

    @Override
    public DBSPExpression getMaxValue() {
        return new DBSPDecimalLiteral(this.getNode(), this, new BigDecimal(this.getMaxLiteral()));
    }

    @Override
    public DBSPExpression getMinValue() {
        return new DBSPDecimalLiteral(this.getNode(), this, new BigDecimal("-" + this.getMaxLiteral()));
    }

    @Override
    public DBSPType withMayBeNull(boolean mayBeNull) {
        if (mayBeNull == this.mayBeNull)
            return this;
        return new DBSPTypeDecimal(this.getNode(), this.precision, this.scale, mayBeNull);
    }

    @Override
    public boolean hasCopy() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.precision, this.scale);
    }

    @Override
    public DBSPExpression defaultValue() {
        if (this.mayBeNull)
            return this.none();
        return this.getZero();
    }

    @Override
    public boolean sameType(DBSPType type) {
        if (!super.sameNullability(type))
            return false;
        if (!type.is(DBSPTypeDecimal.class))
            return false;
        DBSPTypeDecimal other = type.to(DBSPTypeDecimal.class);
        return this.scale == other.scale && this.precision == other.precision;
    }

    @Nullable
    @Override
    public String asSqlString() {
        return "DECIMAL(" + this.precision + ", " + this.scale + ")";
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public String toString() {
        return "DECIMAL(" + this.precision + ", " + this.scale + ")" +
                (this.mayBeNull ? "?" : "");
    }

    @SuppressWarnings("unused")
    public static DBSPTypeDecimal fromJson(JsonNode node, JsonDecoder decoder) {
        boolean mayBeNull = DBSPType.fromJsonMayBeNull(node);
        int scale = Utilities.getIntProperty(node, "scale");
        int precision = Utilities.getIntProperty(node, "precision");
        return new DBSPTypeDecimal(CalciteObject.EMPTY, precision, scale, mayBeNull);
    }
}
