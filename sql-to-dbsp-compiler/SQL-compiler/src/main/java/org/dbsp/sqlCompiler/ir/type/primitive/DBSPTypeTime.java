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

import org.apache.calcite.util.TimeString;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimeLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsDateType;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;

import java.util.Objects;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.TIME;

public class DBSPTypeTime extends DBSPTypeBaseType implements IsNumericType, IsDateType {
    public DBSPTypeTime(CalciteObject node, boolean mayBeNull) {
        super(node, TIME, mayBeNull);
    }

    @Override
    public DBSPLiteral defaultValue() {
        return new DBSPTimeLiteral(this.getNode(), this, new TimeString(0, 0, 0));
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
        return new DBSPTypeTime(this.getNode(), mayBeNull);
    }

    @Override
    public DBSPLiteral getZero() {
        throw new UnsupportedException(this.getNode());
    }

    @Override
    public DBSPLiteral getOne() {
        throw new UnsupportedException(this.getNode());
    }

    @Override
    public DBSPLiteral getMaxValue() {
        throw new UnsupportedException(this.getNode());
    }

    @Override
    public DBSPLiteral getMinValue() {
        throw new UnsupportedException(this.getNode());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.mayBeNull, 13);
    }

    @Override
    public boolean sameType(DBSPType other) {
        if (!super.sameNullability(other))
            return false;
        return other.is(DBSPTypeTime.class);
    }
}
