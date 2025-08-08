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
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.ISameValue;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.IsNumericLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.Objects;

public final class DBSPI32Literal extends DBSPIntLiteral implements IsNumericLiteral {
    @Nullable
    public final Integer value;

    /** Create an I32 literal with a NULL value */
    public DBSPI32Literal() {
        this(null, true);
    }

    public DBSPI32Literal(int value) {
        this(value, false);
    }

    public DBSPI32Literal(CalciteObject node, DBSPType type , @Nullable Integer value) {
        super(node, type, value == null);
        this.value = value;
    }

    public DBSPI32Literal(@Nullable Integer value, boolean nullable) {
        this(CalciteObject.EMPTY, DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, nullable), value);
        if (value == null && !nullable)
            throw new InternalCompilerError("Null value with non-nullable type", this);
    }

    @Override
    public boolean gt0() {
        Utilities.enforce(this.value != null);
        return this.value > 0;
    }

    @Override
    public int compare(IsNumericLiteral other) {
        DBSPI32Literal oi = other.to(DBSPI32Literal.class);
        Utilities.enforce(this.value != null);
        Utilities.enforce(oi.value != null);
        return this.value.compareTo(oi.value);
    }

    @Override
    public IsNumericLiteral negate() {
        if (this.value == null)
            return this;
        return new DBSPI32Literal(this.getNode(), this.type, Math.negateExact(this.value));
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPI32Literal(this.getNode(), this.type, this.value);
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
        DBSPI32Literal that = (DBSPI32Literal) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        return new DBSPI32Literal(this.checkIfNull(this.value, mayBeNull), mayBeNull);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        if (this.value == null)
            return builder.append("(")
                    .append(this.type)
                    .append(")null");
        else
            return builder.append(this.value.toString());
    }

    @Override
    public String toSqlString() {
        if (this.value == null)
            return DBSPNullLiteral.NULL;
        return this.value.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.value);
    }

    @Override @Nullable
    public BigInteger getValue() {
        if (this.value == null)
            return null;
        return BigInteger.valueOf(this.value);
    }

    @SuppressWarnings("unused")
    public static DBSPI32Literal fromJson(JsonNode node, JsonDecoder decoder) {
        Integer value = null;
        if (node.has("value"))
            value = Utilities.getIntProperty(node, "value");
        DBSPType type = getJsonType(node, decoder);
        return new DBSPI32Literal(CalciteObject.EMPTY, type, value);
    }
}
