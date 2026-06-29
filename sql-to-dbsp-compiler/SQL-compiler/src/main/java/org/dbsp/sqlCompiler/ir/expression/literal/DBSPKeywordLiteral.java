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

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.ISameValue;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeKeyword;
import org.dbsp.util.IIndentStream;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;

import javax.annotation.Nullable;
import java.util.Objects;

/** SQL contains a large number of keywords that appear in various places. */
public final class DBSPKeywordLiteral extends DBSPLiteral {
    public final String keyword;

    @Override
    public boolean sameValue(@Nullable ISameValue o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPKeywordLiteral that = (DBSPKeywordLiteral) o;
        return keyword.equals(that.keyword);
    }

    public DBSPKeywordLiteral(CalciteObject node, String keyword) {
        super(node, DBSPTypeKeyword.INSTANCE, false);
        switch (keyword.toLowerCase()) {
            case "sql_tsi_year":
                keyword = "year";
                break;
            case "sql_tsi_quarter":
                keyword = "quarter";
                break;
            case "sql_tsi_month":
                keyword = "month";
                break;
            case "sql_tsi_week":
                keyword = "week";
                break;
            case "sql_tsi_day":
                keyword = "day";
                break;
            case "sql_tsi_hour":
                keyword = "hour";
                break;
            case "sql_tsi_second":
                keyword = "second";
                break;
            case "sql_tsi_minute":
                keyword = "minute";
                break;
            case "dow":
            case "epoch":
            case "isodow":
            case "year":
            case "month":
            case "day":
            case "decade":
            case "quarter":
            case "century":
            case "millennium":
            case "isoyear":
            case "week":
            case "doy":
            case "second":
            case "minute":
            case "hour":
            case "millisecond":
            case "microsecond":
            case "nanosecond":
            case "null_on_null":
            case "both":
            case "leading":
            case "trailing":
                break;
            default:
                throw new UnimplementedException("Not yet implemented support for Calcite construct " + keyword, node);
        }
        this.keyword = keyword.toLowerCase();
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPKeywordLiteral(this.getNode(), this.keyword);
    }

    @Override
    public String toString() {
        return this.keyword;
    }

    @Override
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        if (mayBeNull)
            throw new InternalCompilerError("Nullable keyword");
        return this;
    }

    @Override
    public String toSqlString() {
        throw new InternalCompilerError("unreachable");
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.keyword);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.keyword);
    }
}
