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
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public final class DBSPTimestampLiteral extends DBSPLiteral {
    /** Microseconds since 1970-01-01 */
    @Nullable public final Long value;

    /** Returns the number of microseconds since the epoch. */
    static long getMicrosSinceEpoch(TimestampString ts) {
        var v = ts.toString();
        final int year = Integer.parseInt(v.substring(0, 4));
        final int month = Integer.parseInt(v.substring(5, 7));
        final int day = Integer.parseInt(v.substring(8, 10));
        final long h = Long.parseLong(v.substring(11, 13));
        final long m = Long.parseLong(v.substring(14, 16));
        final long s = Long.parseLong(v.substring(17, 19));
        long micros = 0;
        if (v.length() >= 20) {
            // "1999-12-31 12:34:56" has 19 characters
            // "1999-12-31 12:34:56.789123" has 26, our maximum precision
            // "1999-12-31 12:34:56.789123456" has 29, but we ignore the last 3
            String fraction = v.substring(20, Math.min(26, v.length()));
            micros = Long.parseLong(fraction);
            for (int i = v.length(); i < 26; i++)
                micros *= 10;
        }
        final int d = DateTimeUtils.ymdToUnixDate(year, month, day);
        return (d * DateTimeUtils.MILLIS_PER_DAY
                + h * DateTimeUtils.MILLIS_PER_HOUR
                + m * DateTimeUtils.MILLIS_PER_MINUTE
                + s * DateTimeUtils.MILLIS_PER_SECOND) * 1000
                + micros;
    }

    DBSPTimestampLiteral(CalciteObject node, DBSPType type, @Nullable Long value) {
        super(node, type, value == null);
        this.value = value;
    }

    public static DBSPTimestampLiteral fromMicroseconds(CalciteObject node, DBSPType type, @Nullable Long value) {
        return new DBSPTimestampLiteral(node, type, value);
    }

    public DBSPTimestampLiteral(CalciteObject node, DBSPType type, TimestampString value) {
        this(node, type, getMicrosSinceEpoch(value));
    }

    DBSPTimestampLiteral(long value) {
        this(value, false);
    }

    public static DBSPTimestampLiteral fromMilliseconds(long value) {
        return new DBSPTimestampLiteral(value * 1000);
    }

    public static DBSPTimestampLiteral fromMicroseconds(long value) {
        return new DBSPTimestampLiteral(value);
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

    private static final DateTimeFormatter INSTANT_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    public DBSPTimestampLiteral(Instant instant, boolean mayBeNull) {
        this(INSTANT_FORMAT.format(instant), mayBeNull);
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
        long micros = Objects.requireNonNull(this.value);
        Instant instant = Instant.ofEpochSecond(
                micros / 1_000_000,
                (micros % 1_000_000) * 1_000);
        return new TimestampString(instant
                .atZone(ZoneId.of("UTC"))
                .format(INSTANT_FORMAT));
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
