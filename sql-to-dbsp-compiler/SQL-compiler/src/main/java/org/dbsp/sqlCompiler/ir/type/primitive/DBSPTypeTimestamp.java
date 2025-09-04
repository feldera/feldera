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
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsDateType;
import org.dbsp.sqlCompiler.ir.type.IsTimeRelatedType;

import java.util.Objects;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.TIMESTAMP;

public class DBSPTypeTimestamp extends DBSPTypeBaseType
        implements IsDateType, IsTimeRelatedType {
    DBSPTypeTimestamp(CalciteObject node, boolean mayBeNull) {
        super(node, TIMESTAMP, mayBeNull);
    }

    public static final DBSPTypeTimestamp INSTANCE = new DBSPTypeTimestamp(CalciteObject.EMPTY, false);
    public static final DBSPTypeTimestamp NULLABLE_INSTANCE = new DBSPTypeTimestamp(CalciteObject.EMPTY, true);

    public static DBSPTypeTimestamp create(CalciteObject node, boolean mayBeNull) {
        if (node.isEmpty())
            return mayBeNull ? NULLABLE_INSTANCE : INSTANCE;
        return new DBSPTypeTimestamp(node, mayBeNull);
    }

    public static DBSPTypeTimestamp create(boolean mayBeNull) {
        return create(CalciteObject.EMPTY, mayBeNull);
    }

    @Override
    public DBSPExpression defaultValue() {
        if (this.mayBeNull)
            return this.none();
        return new DBSPTimestampLiteral(this.getNode(), this, 0L);
    }

    @Override
    public DBSPExpression getMaxValue() {
        return new DBSPTimestampLiteral("9999-12-31 23:59:59.99999999", this.mayBeNull);
    }

    @Override
    public DBSPExpression getMinValue() {
        return new DBSPTimestampLiteral("0001-01-01 00:00:00", this.mayBeNull);
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
    public DBSPType withMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return DBSPTypeTimestamp.create(this.getNode(), mayBeNull);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.mayBeNull, 14);
    }

    @Override
    public boolean sameType(DBSPType other) {
        if (!super.sameNullability(other))
            return false;
        return other.is(DBSPTypeTimestamp.class);
    }

    @SuppressWarnings("unused")
    public static DBSPTypeTimestamp fromJson(JsonNode node, JsonDecoder decoder) {
        boolean mayBeNull = DBSPType.fromJsonMayBeNull(node);
        return DBSPTypeTimestamp.create(mayBeNull);
    }
}
