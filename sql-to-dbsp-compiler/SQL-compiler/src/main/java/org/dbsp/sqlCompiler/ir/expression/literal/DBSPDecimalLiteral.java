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
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.Objects;

public final class DBSPDecimalLiteral extends DBSPLiteral implements IsNumericLiteral {
    @Nullable
    public final BigDecimal value;

    public DBSPDecimalLiteral(CalciteObject node, DBSPType type, @Nullable BigDecimal value) {
        super(node, type, value == null);
        if (!type.is(DBSPTypeDecimal.class))
            throw new InternalCompilerError("Decimal literal cannot have type " + type, this);
        this.value = value;
    }

    public DBSPDecimalLiteral(DBSPType type, @Nullable BigDecimal value) {
        this(type.getNode(), type, value);
    }

    public DBSPDecimalLiteral(int value) {
        this(DBSPTypeDecimal.getDefault(), BigDecimal.valueOf(value));
    }

    public DBSPDecimalLiteral(int value, boolean mayBeNull) {
        this(DBSPTypeDecimal.getDefault().withMayBeNull(mayBeNull), BigDecimal.valueOf(value));
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
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        return new DBSPDecimalLiteral(this.getNode(), this.getType().withMayBeNull(mayBeNull),
                this.checkIfNull(this.value, mayBeNull));
    }

    @Override
    public IsNumericLiteral negate() {
        if (this.value == null)
            return this;
        return new DBSPDecimalLiteral(this.getNode(), this.type, this.value.negate());
    }

    @Override
    public String toSqlString() {
        if (this.value == null)
            return DBSPNullLiteral.NULL;
        return this.value.toString();
    }

    @Override
    public boolean sameValue(@Nullable ISameValue o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPDecimalLiteral that = (DBSPDecimalLiteral) o;
        return this.getType().sameType(that.type) && Objects.equals(value, that.value);
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
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.value);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPDecimalLiteral(this.getNode(), this.type, this.value);
    }

    @Override
    public boolean gt0() {
        Utilities.enforce(this.value != null);
        return this.value.compareTo(BigDecimal.ZERO) > 0;
    }

    @SuppressWarnings("unused")
    public static DBSPDecimalLiteral fromJson(JsonNode node, JsonDecoder decoder) {
        BigDecimal value = null;
        if (node.has("value")) {
            String encoded = Utilities.getStringProperty(node, "value");
            value = new BigDecimal(encoded);
        }
        DBSPType type = getJsonType(node, decoder);
        return new DBSPDecimalLiteral(CalciteObject.EMPTY, type, value);
    }
}
