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
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPNullLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsBoundedType;

import java.util.Objects;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.NULL;

/** This type has a single value, NULL. */
public class DBSPTypeNull extends DBSPTypeBaseType implements IsBoundedType {
    public static final DBSPTypeNull INSTANCE = new DBSPTypeNull(CalciteObject.EMPTY);

    DBSPTypeNull(CalciteObject node) {
        super(node, NULL, true);
    }

    @Override
    public DBSPExpression defaultValue() {
        return DBSPNullLiteral.INSTANCE;
    }

    public static DBSPTypeNull getDefault() {
        return INSTANCE;
    }

    @Override
    public DBSPType withMayBeNull(boolean mayBeNull) {
        if (mayBeNull == this.mayBeNull)
            return this;
        throw new UnsupportedException("Null type must be nullable", this.getNode());
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
    public boolean sameFields(IDBSPInnerNode other) {
        return this.sameNullability(other) && other.is(DBSPTypeNull.class);
    }

    @Override
    public String toString() {
        return "null";
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.mayBeNull, 10);
    }

    @Override
    public boolean sameType(DBSPType other) {
        return other.is(DBSPTypeNull.class);
    }

    @SuppressWarnings("unused")
    public static DBSPTypeNull fromJson(JsonNode node, JsonDecoder decoder) {
        return INSTANCE;
    }

    @Override
    public DBSPExpression getMaxValue() {
        return DBSPNullLiteral.INSTANCE;
    }

    @Override
    public DBSPExpression getMinValue() {
        return DBSPNullLiteral.INSTANCE;
    }
}
