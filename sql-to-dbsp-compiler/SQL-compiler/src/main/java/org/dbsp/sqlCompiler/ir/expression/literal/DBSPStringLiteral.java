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
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utf8String;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.Objects;

public final class DBSPStringLiteral extends DBSPLiteral {
    @Nullable
    public final Utf8String value;

    public DBSPStringLiteral(String value) {
        this(CalciteObject.EMPTY, DBSPTypeString.varchar(false), Utf8String.fromJava(value));
    }

    public DBSPStringLiteral(CalciteObject node, DBSPType type, @Nullable Utf8String value) {
        super(node, type, value == null);
        this.value = value;
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPStringLiteral(this.getNode(), this.type, this.value);
    }

    @Override
    public boolean sameValue(@Nullable ISameValue o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPStringLiteral that = (DBSPStringLiteral) o;
        return Objects.equals(value, that.value);
    }

    public DBSPStringLiteral(@Nullable String value, boolean nullable) {
        this(CalciteObject.EMPTY, DBSPTypeString.varchar(nullable), value == null ? null : Utf8String.fromJava(value));
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
        return new DBSPStringLiteral(CalciteObject.EMPTY, this.type.withMayBeNull(true), this.checkIfNull(this.value, mayBeNull));
    }

    @Override
    public String toSqlString() {
        if (this.value == null)
            return DBSPNullLiteral.NULL;
        return Utilities.singleQuote(this.value.toJavaString());
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        if (this.value == null)
            return builder.append("(")
                    .append(this.type)
                    .append(")null");
        else
            return builder.append(this.wrapSome(Utilities.doubleQuote(this.value.toJavaString(), true)));
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.value);
    }

    @SuppressWarnings("unused")
    public static DBSPStringLiteral fromJson(JsonNode node, JsonDecoder decoder) {
        Utf8String value = null;
        if (node.has("value")) {
            value = Utf8String.fromJava(Utilities.getStringProperty(node, "value"));
        }
        DBSPType type = getJsonType(node, decoder);
        return new DBSPStringLiteral(CalciteObject.EMPTY, type, value);
    }
}
