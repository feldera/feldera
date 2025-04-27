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
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.ISameValue;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.IsNumericLiteral;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUSize;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.Objects;

public final class DBSPUSizeLiteral extends DBSPLiteral implements IsNumericLiteral {
    @Nullable
    public final BigInteger value;

    @Override
    public boolean sameValue(@Nullable ISameValue o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPUSizeLiteral that = (DBSPUSizeLiteral) o;
        return Objects.equals(value, that.value);
    }

    public DBSPUSizeLiteral(CalciteObject node, DBSPType type, @Nullable BigInteger value) {
        super(node, type, value == null);
        if (value != null && value.compareTo(BigInteger.ZERO) < 0)
            throw new CompilationError("Negative value for usize literal " + value);
        this.value = value;
    }

    public DBSPUSizeLiteral(CalciteObject node, DBSPType type, @Nullable Long value) {
        this(node, type, value == null ? null : BigInteger.valueOf(value));
    }

    @Override
    public boolean gt0() {
        Utilities.enforce(this.value != null);
        return this.value.compareTo(BigInteger.ZERO) > 0;
    }

    public DBSPUSizeLiteral(long value) {
        this(value, false);
    }

    public DBSPUSizeLiteral(@Nullable Long value, boolean nullable) {
        this(CalciteObject.EMPTY, DBSPTypeUSize.create(nullable), value);
        if (value == null && !nullable)
            throw new InternalCompilerError("Null value with non-nullable type", this);
        if (value != null && value < 0)
            throw new InternalCompilerError("Negative usize value " + value, this);
    }

    @Override
    public IsNumericLiteral negate() {
        throw new UnsupportedException("Negation of unsigned values", this.getNode());
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
        return new DBSPUSizeLiteral(this.getNode(), this.getType().withMayBeNull(mayBeNull),
                this.checkIfNull(this.value, mayBeNull));
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
        throw new InternalCompilerError("unreachable");
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.value);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPUSizeLiteral(this.getNode(), this.type, this.value);
    }

    @SuppressWarnings("unused")
    public static DBSPUSizeLiteral fromJson(JsonNode node, JsonDecoder decoder) {
        BigInteger value = null;
        if (node.has("value"))
            value = new BigInteger(Utilities.getStringProperty(node, "value"));
        DBSPType type = getJsonType(node, decoder);
        return new DBSPUSizeLiteral(CalciteObject.EMPTY, type, value);
    }
}
