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

package org.dbsp.sqlCompiler.ir.type.primitive;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.STRING;

public class DBSPTypeString extends DBSPTypeBaseType implements IHasPrecision {
    /** If true the width is fixed, i.e., this is a CHAR type.
     * Otherwise, this is a VARCHAR. */
    public final boolean fixed;
    /** Number of characters.  If {@link IHasPrecision#UNLIMITED_PRECISION} it means "unlimited".
     * This is the size specified by CHAR or VARCHAR. */
    public final int precision;

    private static final DBSPTypeString VARCHAR = new DBSPTypeString(CalciteObject.EMPTY, UNLIMITED_PRECISION, false, false);
    private static final DBSPTypeString VARCHAR_NULLABLE = new DBSPTypeString(CalciteObject.EMPTY, UNLIMITED_PRECISION, false, true);

    private DBSPTypeString(CalciteObject node, int precision, boolean fixed, boolean mayBeNull) {
        super(node, STRING, mayBeNull);
        this.precision = precision;
        this.fixed = fixed;
    }

    public static DBSPTypeString create(CalciteObject node, int precision, boolean fixed, boolean mayBeNull) {
        if (node.isEmpty() && precision == UNLIMITED_PRECISION && !fixed)
            return varchar(mayBeNull);
        return new DBSPTypeString(node, precision, fixed, mayBeNull);
    }

    public static DBSPTypeString varchar(boolean mayBeNull) {
        if (mayBeNull)
            return VARCHAR_NULLABLE;
        return VARCHAR;
    }

    @Override
    public DBSPType withMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return new DBSPTypeString(this.getNode(), this.precision, this.fixed, mayBeNull);
    }

    @Override
    public DBSPExpression defaultValue() {
        if (this.mayBeNull)
            return this.none();
        return new DBSPStringLiteral("", StandardCharsets.UTF_8, this);
    }

    @Override
    public boolean sameType(DBSPType type) {
        DBSPTypeString other = type.as(DBSPTypeString.class);
        if (other == null)
            return false;
        if (!super.sameNullability(type))
            return false;
        return this.fixed == other.fixed && this.precision == other.precision;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (fixed ? 1 : 0);
        result = 31 * result + precision;
        return result;
    }

    @Override
    public boolean hasCopy() {
        return false;
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
    public IIndentStream toString(IIndentStream builder) {
        if (this.precision == UNLIMITED_PRECISION)
            return super.toString(builder);
        return builder.append(this.shortName())
                .append("(")
                .append(this.precision)
                .append(",")
                .append(Boolean.toString(this.fixed))
                .append(")")
                .append(this.mayBeNull ? "?" : "");
    }

    @Override
    public int getPrecision() {
        return this.precision;
    }

    @Nullable
    @Override
    public String asSqlString() {
        String result;
        if (this.fixed)
            result = "CHAR";
        else
            result = "VARCHAR";
        if (this.precision != UNLIMITED_PRECISION)
            result += "(" + this.precision + ")";
        return result;
    }

    @SuppressWarnings("unused")
    public static DBSPTypeString fromJson(JsonNode node, JsonDecoder decoder) {
        boolean mayBeNull = DBSPType.fromJsonMayBeNull(node);
        boolean fixed = Utilities.getBooleanProperty(node, "fixed");
        int precision = Utilities.getIntProperty(node, "precision");
        return DBSPTypeString.create(CalciteObject.EMPTY, precision, fixed, mayBeNull);
    }
}
