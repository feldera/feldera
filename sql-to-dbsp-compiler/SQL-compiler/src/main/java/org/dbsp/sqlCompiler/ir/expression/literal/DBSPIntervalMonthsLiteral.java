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
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMonthsInterval;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.Objects;

public final class DBSPIntervalMonthsLiteral
        extends DBSPLiteral
        implements IsNumericLiteral, IsIntervalLiteral {
    /** Expressed in months */
    @Nullable public final Integer value;

    public DBSPIntervalMonthsLiteral(CalciteObject node, DBSPType type, @Nullable Integer value) {
        super(node, type, value == null);
        assert type.is(DBSPTypeMonthsInterval.class);
        this.value = value;
    }

    public DBSPIntervalMonthsLiteral(DBSPTypeMonthsInterval.Units units, int value) {
        this(CalciteObject.EMPTY, new DBSPTypeMonthsInterval(CalciteObject.EMPTY, units,false), value);
    }

    public DBSPIntervalMonthsLiteral(DBSPTypeMonthsInterval.Units units, int value, boolean mayBeNull) {
        this(CalciteObject.EMPTY, new DBSPTypeMonthsInterval(CalciteObject.EMPTY, units, mayBeNull), value);
    }

    @Override
    public boolean gt0() {
        assert this.value != null;
        return this.value > 0;
    }

    @Override
    public IsNumericLiteral negate() {
        if (this.value == null)
            return this;
        return new DBSPIntervalMonthsLiteral(this.getNode(), this.type, Math.negateExact(this.value));
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
        DBSPIntervalMonthsLiteral that = (DBSPIntervalMonthsLiteral) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        return new DBSPIntervalMonthsLiteral(this.getNode(),
                this.getType().withMayBeNull(mayBeNull), this.checkIfNull(this.value, mayBeNull));
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append("(")
                .append(this.type)
                .append(")");
        if (this.value != null)
            return builder.append(this.value.toString());
        return builder.append(DBSPNullLiteral.NULL);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.value);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPIntervalMonthsLiteral(this.getNode(), this.type, this.value);
    }

    @Override
    public IsIntervalLiteral multiply(@Nullable BigInteger value) {
        if (this.value == null)
            return this;
        if (value == null)
            return new DBSPIntervalMonthsLiteral(this.getNode(), this.type, null);
        BigInteger result = value.multiply(BigInteger.valueOf(this.value));
        return new DBSPIntervalMonthsLiteral(this.getNode(), this.type, result.intValueExact());
    }

    @Override
    public String toSqlString() {
        if (this.value == null)
            return DBSPNullLiteral.NULL;
        return "INTERVAL " + this.value + " MONTHS";
    }

    @SuppressWarnings("unused")
    public static DBSPIntervalMonthsLiteral fromJson(JsonNode node, JsonDecoder decoder) {
        Integer value = null;
        if (node.has("value")) {
            value = Utilities.getIntProperty(node, "value");
        }
        DBSPType type = getJsonType(node, decoder);
        return new DBSPIntervalMonthsLiteral(CalciteObject.EMPTY, type, value);
    }
}
