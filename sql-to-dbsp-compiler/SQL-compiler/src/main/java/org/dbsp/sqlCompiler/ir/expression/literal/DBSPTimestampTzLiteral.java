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
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.TimestampWithTimeZoneString;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.ISameValue;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestampTz;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public final class DBSPTimestampTzLiteral extends DBSPLiteral {
    // Note: this is NOT a canonical representation.
    @Nullable public final TimestampWithTimeZoneString value;

    public DBSPTimestampTzLiteral(CalciteObject node, DBSPType type, @Nullable TimestampWithTimeZoneString value) {
        super(node, type, value == null);
        this.value = value;
    }

    public DBSPTimestampTzLiteral() {
        this(CalciteObject.EMPTY, DBSPTypeTimestampTz.NULLABLE_INSTANCE, null);
    }

    @Override
    public boolean sameValue(@Nullable ISameValue o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPTimestampTzLiteral that = (DBSPTimestampTzLiteral) o;
        return Objects.equals(this.getMicros(), that.getMicros());
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
        return new DBSPTimestampTzLiteral(this.getNode(), this.getType().withMayBeNull(mayBeNull),
                this.checkIfNull(this.value, mayBeNull));
    }

    @Nullable
    public TimestampWithTimeZoneString getTimestampString() {
        return this.value;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        if (this.value == null)
            return builder.append("(")
                    .append(this.type)
                    .append(")null");
        else
            return builder.append(this.wrapSome(Objects.requireNonNull(this.getTimestampString()).toString()));
    }

    @Override
    public String toSqlString() {
        if (this.value == null)
            return DBSPNullLiteral.NULL;
        return "TIMESTAMP WITH TIME ZONE " + Utilities.singleQuote(Objects.requireNonNull(this.getTimestampString()).toString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.value);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPTimestampTzLiteral(this.getNode(), this.type, this.value);
    }

    @SuppressWarnings("unused")
    public static DBSPTimestampTzLiteral fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPType type = getJsonType(node, decoder);
        if (node.has("value")) {
            String value = Utilities.getStringProperty(node, "value");
            return new DBSPTimestampTzLiteral(CalciteObject.EMPTY, type, new TimestampWithTimeZoneString(value));
        } else {
            return new DBSPTimestampTzLiteral(CalciteObject.EMPTY, type, null);
        }
    }

    @Nullable
    public Long getMicros() {
        if (this.value == null)
            return null;
        long micros = DBSPTimestampLiteral.getMicrosSinceEpoch(this.value.getLocalTimestampString());
        LocalDateTime dt = LocalDateTime.ofEpochSecond(
                micros / 1_000_000L,
                (int)(micros % 1_000_000L) * 1000,
                ZoneOffset.UTC);
        ZonedDateTime z = dt.atZone(this.value.getTimeZone().toZoneId());
        Instant instant = z.toInstant();
        return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000;
    }
}
