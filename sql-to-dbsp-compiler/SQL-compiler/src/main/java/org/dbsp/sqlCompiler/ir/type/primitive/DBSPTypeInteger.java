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

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI128Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI16Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI8Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU128Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU16Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU64Literal;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;

import java.math.BigInteger;
import java.util.Objects;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.*;

public class DBSPTypeInteger extends DBSPTypeBaseType
        implements IsNumericType {
    private final int width;
    public final boolean signed;

    public static DBSPTypeCode getCode(int width, boolean signed) {
        if (signed) {
            switch (width) {
                case 8: return INT8;
                case 16: return INT16;
                case 32: return INT32;
                case 64: return INT64;
                case 128: return INT128;
            }
        } else {
            switch (width) {
                case 16: return UINT16;
                case 32: return UINT32;
                case 64: return UINT64;
                case 128: return UINT128;
            }
        }
        throw new InternalCompilerError("Unexpected integer type: " +
                "width=" + width + " signed=" + signed);
    }

    UnsupportedException unsupported() {
        return new UnsupportedException("Unexpected type " + this, this);
    }

    public DBSPTypeInteger(CalciteObject node, int width, boolean signed, boolean mayBeNull) {
        super(node, getCode(width, signed), mayBeNull);
        this.width = width;
        this.signed = signed;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.width, this.signed);
    }

    @Override
    public DBSPLiteral getZero() {
        if (this.signed) {
            switch (this.width) {
                case 8: return new DBSPI8Literal((byte)0, this.mayBeNull);
                case 16: return new DBSPI16Literal((short)0, this.mayBeNull);
                case 32: return new DBSPI32Literal(0, this.mayBeNull);
                case 64: return new DBSPI64Literal(0L, this.mayBeNull);
                case 128: return new DBSPI128Literal(0, this.mayBeNull);
            }
        } else {
            switch (this.width) {
                case 16: return new DBSPU16Literal(0, this.mayBeNull);
                case 32: return new DBSPU32Literal(0L, this.mayBeNull);
                case 64: return new DBSPU64Literal(BigInteger.ZERO, this.mayBeNull);
                case 128: return new DBSPU128Literal(BigInteger.ZERO, this.mayBeNull);
            }
        }
        throw this.unsupported();
    }

    @Override
    public DBSPLiteral getOne() {
        if (this.signed) {
            switch (this.width) {
                case 8: return new DBSPI8Literal((byte)1, this.mayBeNull);
                case 16: return new DBSPI16Literal((short)1, this.mayBeNull);
                case 32: return new DBSPI32Literal(1, this.mayBeNull);
                case 64: return new DBSPI64Literal(1L, this.mayBeNull);
                case 128: return new DBSPI128Literal(1, this.mayBeNull);
            }
        } else {
            switch (this.width) {
                case 16: return new DBSPU16Literal(1, this.mayBeNull);
                case 32: return new DBSPU32Literal(1L, this.mayBeNull);
                case 64: return new DBSPU64Literal(BigInteger.ONE, this.mayBeNull);
                case 128: return new DBSPU128Literal(BigInteger.ONE, this.mayBeNull);
            }
        }
        throw this.unsupported();
    }

    @Override
    public DBSPLiteral getMaxValue() {
        if (this.signed) {
            switch (this.width) {
                case 8: return new DBSPI8Literal(Byte.MAX_VALUE, this.mayBeNull);
                case 16: return new DBSPI16Literal(Short.MAX_VALUE, this.mayBeNull);
                case 32: return new DBSPI32Literal(Integer.MAX_VALUE, this.mayBeNull);
                case 64: return new DBSPI64Literal(Long.MAX_VALUE, this.mayBeNull);
                case 128: return new DBSPI128Literal(
                        BigInteger.ONE.shiftLeft(127).subtract(BigInteger.ONE), this.mayBeNull);
            }
        } else {
            switch (this.width) {
                case 16: return new DBSPU16Literal(0xFFFF, this.mayBeNull);
                case 32: return new DBSPU32Literal(Integer.toUnsignedLong(-1), this.mayBeNull);
                case 64: return new DBSPU64Literal(
                        BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE), this.mayBeNull);
                case 128: return new DBSPU128Literal(
                        BigInteger.ONE.shiftLeft(128).subtract(BigInteger.ONE), this.mayBeNull);
            }
        }
        throw this.unsupported();
    }

    @Override
    public DBSPLiteral getMinValue() {
        if (this.signed) {
            switch (this.width) {
                case 8: return new DBSPI8Literal(Byte.MIN_VALUE, this.mayBeNull);
                case 16: return new DBSPI16Literal(Short.MIN_VALUE, this.mayBeNull);
                case 32: return new DBSPI32Literal(Integer.MIN_VALUE, this.mayBeNull);
                case 64: return new DBSPI64Literal(Long.MIN_VALUE, this.mayBeNull);
                case 128: return new DBSPI128Literal(
                        BigInteger.ONE.shiftLeft(128).negate(), this.mayBeNull);
            }
        } else {
            return this.getZero();
        }
        throw this.unsupported();
    }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        if (mayBeNull == this.mayBeNull)
            return this;
        return new DBSPTypeInteger(this.getNode(), this.width, this.signed, mayBeNull);
    }

    @Override
    public DBSPLiteral defaultValue() {
        return this.getZero();
    }

    public int getWidth() {
        return this.width;
    }

    @Override
    public boolean sameType(DBSPType type) {
        if (!super.sameNullability(type))
            return false;
        if (!type.is(DBSPTypeInteger.class))
            return false;
        DBSPTypeInteger other = type.to(DBSPTypeInteger.class);
        return this.width == other.width && this.signed == other.signed;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }
}
