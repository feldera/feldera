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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.ISameValue;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public final class DBSPStringLiteral extends DBSPLiteral {
    @Nullable
    public final String value;
    public final Charset charset;

    public DBSPStringLiteral(String value) {
        this(value, StandardCharsets.UTF_8, DBSPTypeString.varchar(false));
    }

    public DBSPStringLiteral(CalciteObject node, DBSPType type, @Nullable String value, Charset charset) {
        super(node, type, value == null);
        this.charset = charset;
        this.value = value;
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPStringLiteral(this.getNode(), this.type, this.value, this.charset);
    }

    @Override
    public boolean sameValue(@Nullable ISameValue o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPStringLiteral that = (DBSPStringLiteral) o;
        return Objects.equals(value, that.value);
    }

    public DBSPStringLiteral(@Nullable String value, Charset charset, DBSPType type) {
        this(CalciteObject.EMPTY, type, value, charset);
        if (value == null && !type.mayBeNull)
            throw new InternalCompilerError("Null value with non-nullable type", this);
    }

    public DBSPStringLiteral(@Nullable String value, boolean nullable) {
        this(CalciteObject.EMPTY, DBSPTypeString.varchar(nullable), value, StandardCharsets.UTF_8);
        if (value == null && !nullable)
            throw new InternalCompilerError("Null value with non-nullable type", this);
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
        return new DBSPStringLiteral(this.checkIfNull(this.value, mayBeNull), this.charset, this.type.withMayBeNull(true));
    }

    @Override
    public String toSqlString() {
        if (this.value == null)
            return DBSPNullLiteral.NULL;
        return Utilities.singleQuote(this.value);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        if (this.value == null)
            return builder.append("(")
                    .append(this.type)
                    .append(")null");
        else
            return builder.append(Utilities.doubleQuote(this.value));
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.value);
    }

    public DBSPStrLiteral toStr() {
        Utilities.enforce(this.value != null);
        return new DBSPStrLiteral(this.value);
    }

    @SuppressWarnings("unused")
    public static DBSPStringLiteral fromJson(JsonNode node, JsonDecoder decoder) {
        String value = null;
        if (node.has("value")) {
            value = Utilities.getStringProperty(node, "value");
        }
        String charset = Utilities.getStringProperty(node, "charset");
        DBSPType type = getJsonType(node, decoder);
        return new DBSPStringLiteral(CalciteObject.EMPTY, type, value, Charset.forName(charset));
    }
}
