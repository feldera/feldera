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
import org.apache.calcite.util.TimestampString;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.ISameValue;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public final class DBSPTimestampLiteral extends DBSPLiteral {
    /** Milliseconds since 1970-01-01 */
    @Nullable public final Long value;

    public DBSPTimestampLiteral(CalciteObject node, DBSPType type, @Nullable Long value) {
        super(node, type, value == null);
        this.value = value;
    }

    public DBSPTimestampLiteral(CalciteObject node, DBSPType type, TimestampString value) {
        this(node, type, value.getMillisSinceEpoch());
    }

    public DBSPTimestampLiteral(long value) {
        this(value, false);
    }

    public DBSPTimestampLiteral(long value, boolean mayBeNull) {
        this(CalciteObject.EMPTY, DBSPTypeTimestamp.create(mayBeNull), value);
    }

    public DBSPTimestampLiteral() {
        this(CalciteObject.EMPTY, DBSPTypeTimestamp.NULLABLE_INSTANCE, (Long)null);
    }

    public DBSPTimestampLiteral(String string, boolean mayBeNull) {
        this(CalciteObject.EMPTY, DBSPTypeTimestamp.create(mayBeNull), createTimestampString(string));
    }

    private static final String INSTANT_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    public DBSPTimestampLiteral(Instant instant, boolean mayBeNull) {
        this(DateTimeFormatter.ofPattern(INSTANT_FORMAT).format(instant), mayBeNull);
    }

    static TimestampString createTimestampString(String timestamp) {
        // TimestampString is too smart: it does not accept a number that ends in 0 if there is a decimal point
        String[] parts = timestamp.split("[.]");
        TimestampString result = new TimestampString(parts[0]);
        if (parts.length == 1)
            return result;
        return result.withFraction(parts[1]);
    }

    @Override
    public boolean sameValue(@Nullable ISameValue o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPTimestampLiteral that = (DBSPTimestampLiteral) o;
        return Objects.equals(value, that.value);
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
        return new DBSPTimestampLiteral(this.getNode(), this.getType().withMayBeNull(mayBeNull),
                this.checkIfNull(this.value, mayBeNull));
    }

    @Nullable
    public TimestampString getTimestampString() {
        if (this.isNull)
            return null;
        return TimestampString.fromMillisSinceEpoch(Objects.requireNonNull(this.value));
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        if (this.value == null)
            return builder.append("(")
                    .append(this.type)
                    .append(")null");
        else
            return builder.append(TimestampString.fromMillisSinceEpoch(this.value).toString());
    }

    @Override
    public String toSqlString() {
        if (this.value == null)
            return DBSPNullLiteral.NULL;
        return "TIMESTAMP " + Utilities.singleQuote(Objects.requireNonNull(this.getTimestampString()).toString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.value);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPTimestampLiteral(this.getNode(), this.type, this.value);
    }

    @SuppressWarnings("unused")
    public static DBSPTimestampLiteral fromJson(JsonNode node, JsonDecoder decoder) {
        Long value = null;
        if (node.has("value"))
            value = Utilities.getLongProperty(node, "value");
        DBSPType type = getJsonType(node, decoder);
        return new DBSPTimestampLiteral(CalciteObject.EMPTY, type, value);
    }
}
