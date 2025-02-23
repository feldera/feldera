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
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPISizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;

import java.util.Objects;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.ISIZE;

/** Represents the usize Rust type. */
public class DBSPTypeISize extends DBSPTypeBaseType implements IsNumericType {
    DBSPTypeISize(boolean mayBeNull) {
        super(CalciteObject.EMPTY, ISIZE, mayBeNull);
    }

    public static DBSPTypeISize create(boolean mayBeNull) {
        if (mayBeNull)
            return NULLABLE_INSTANCE;
        else
            return INSTANCE;
    }

    public static final DBSPTypeISize INSTANCE = new DBSPTypeISize(false);
    public static final DBSPTypeISize NULLABLE_INSTANCE = new DBSPTypeISize(true);

    @Override
    public DBSPType withMayBeNull(boolean mayBeNull) {
        if (mayBeNull)
            throw new UnsupportedOperationException();
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.mayBeNull, 7);
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
        return type.is(DBSPTypeISize.class);
    }

    @Override
    public DBSPLiteral getZero() {
        return new DBSPISizeLiteral(0L, this.mayBeNull);
    }

    @Override
    public DBSPLiteral getOne() {
        return new DBSPISizeLiteral(1);
    }

    @Override
    public int getPrecision() {
        return 19;
    }

    @Override
    public DBSPExpression getMaxValue() {
        throw new UnsupportedException(this.getNode());
    }

    @Override
    public DBSPExpression getMinValue() {
        throw new UnsupportedException(this.getNode());
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @SuppressWarnings("unused")
    public static DBSPTypeISize fromJson(JsonNode node, JsonDecoder decoder) {
        boolean mayBeNull = DBSPType.fromJsonMayBeNull(node);
        return DBSPTypeISize.create(mayBeNull);
    }
}
