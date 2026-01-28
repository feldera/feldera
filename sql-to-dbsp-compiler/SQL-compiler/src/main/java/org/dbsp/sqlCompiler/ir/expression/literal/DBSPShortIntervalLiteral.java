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

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.ISameValue;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.IsIntervalLiteral;
import org.dbsp.sqlCompiler.ir.IsNumericLiteral;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeShortInterval;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.Objects;

public final class DBSPShortIntervalLiteral
        extends DBSPLiteral
        implements IsNumericLiteral, IsIntervalLiteral {

    /** Canonical value, represented in microseconds */
    @Nullable public final Long value;

    public DBSPShortIntervalLiteral(DBSPTypeShortInterval.Units units) {
        this(CalciteObject.EMPTY, new DBSPTypeShortInterval(CalciteObject.EMPTY, units, true), null);
    }

    DBSPShortIntervalLiteral(CalciteObject node, DBSPType type, @Nullable Long value) {
        super(node, type, value == null);
        this.value = value;
    }

    public static DBSPShortIntervalLiteral fromMicroseconds(CalciteObject node, DBSPType type, @Nullable Long value) {
        return new DBSPShortIntervalLiteral(node, type, value);
    }

    public static DBSPShortIntervalLiteral fromMicroseconds(DBSPTypeShortInterval.Units units, long value, boolean mayBeNull) {
        return new DBSPShortIntervalLiteral(CalciteObject.EMPTY, new DBSPTypeShortInterval(CalciteObject.EMPTY, units, mayBeNull), value);
    }

    @Override
    public boolean gt0() {
        Utilities.enforce(this.value != null);
        return this.value > 0;
    }

    @Override
    public int compare(IsNumericLiteral other) {
        DBSPShortIntervalLiteral oi = other.to(DBSPShortIntervalLiteral.class);
        Utilities.enforce(this.value != null);
        Utilities.enforce(oi.value != null);
        return this.value.compareTo(oi.value);
    }

    @Override
    public IsNumericLiteral negate() {
        if (this.value == null)
            return this;
        return new DBSPShortIntervalLiteral(this.getNode(), this.type, Math.negateExact(this.value));
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
    public boolean sameValue(@Nullable ISameValue o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPShortIntervalLiteral that = (DBSPShortIntervalLiteral) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        return new DBSPShortIntervalLiteral(this.getNode(),
                this.getType().withMayBeNull(mayBeNull), this.checkIfNull(this.value, mayBeNull));
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append("(")
                .append(this.type)
                .append(")");
        if (this.value == null)
            return builder.append("null");
        else {
            return builder.append(this.wrapSome(this.value.toString()));
        }
    }

    @Override
    public IsIntervalLiteral multiply(@Nullable BigInteger value) {
        if (this.value == null)
            return this;
        if (value == null)
            return new DBSPShortIntervalLiteral(this.getNode(), this.type, null);
        BigInteger result = value.multiply(BigInteger.valueOf(this.value));
        return DBSPShortIntervalLiteral.fromMicroseconds(
                this.type.to(DBSPTypeShortInterval.class).units, result.longValueExact(), this.isNull);
    }

    @Override
    public String toSqlString() {
        if (this.value == null)
            return DBSPNullLiteral.NULL;
        long us = this.value % 1000000L;
        return "INTERVAL " + this.value / 1000000L +
                ((us > 0) ? "." + String.format("%06d", us) : "") +
                " SECONDS";
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.value);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPShortIntervalLiteral(this.getNode(), this.type, this.value);
    }

    @SuppressWarnings("unused")
    public static DBSPShortIntervalLiteral fromJson(JsonNode node, JsonDecoder decoder) {
        Long value = null;
        if (node.has("value")) {
            value = Utilities.getLongProperty(node, "value");
        }
        DBSPType type = getJsonType(node, decoder);
        return new DBSPShortIntervalLiteral(CalciteObject.EMPTY, type, value);
    }
}
