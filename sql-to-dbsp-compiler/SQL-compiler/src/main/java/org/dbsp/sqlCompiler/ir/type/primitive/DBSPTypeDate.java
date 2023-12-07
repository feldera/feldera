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

import org.apache.calcite.util.DateString;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDateLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsDateType;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;

import java.util.Objects;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.DATE;

public class DBSPTypeDate extends DBSPTypeBaseType implements IsNumericType, IsDateType {
    public DBSPTypeDate(CalciteObject node, boolean mayBeNull) {
        super(node, DATE, mayBeNull);
    }

    @Override
    public DBSPLiteral defaultValue() {
        return new DBSPDateLiteral(this.getNode(), this, new DateString(1970, 1, 1));
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
    public DBSPType setMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return new DBSPTypeDate(this.getNode(), mayBeNull);
    }

    @Override
    public DBSPLiteral getZero() {
        // This matches the actual value represented in the runtime as "0".
        return new DBSPDateLiteral(CalciteObject.EMPTY, this, 0);
    }

    @Override
    public DBSPLiteral getOne() {
        throw new UnsupportedException(this.getNode());
    }

    @Override
    public DBSPLiteral getMaxValue() {
        return new DBSPDateLiteral(this.getNode(), this, new DateString("9999-12-12"));
    }

    @Override
    public DBSPLiteral getMinValue() {
        return new DBSPDateLiteral(this.getNode(), this, new DateString("0000-01-01"));
    }

    @Override
    public boolean sameType(DBSPType other) {
        if (!super.sameNullability(other))
            return false;
        return other.is(DBSPTypeDate.class);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.mayBeNull, 3);
    }
}
