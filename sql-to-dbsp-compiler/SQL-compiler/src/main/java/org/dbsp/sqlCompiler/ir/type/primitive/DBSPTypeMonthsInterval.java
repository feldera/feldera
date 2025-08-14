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
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMonthsLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.IHasZero;
import org.dbsp.sqlCompiler.ir.type.IsIntervalType;
import org.dbsp.sqlCompiler.ir.type.IsTimeRelatedType;
import org.dbsp.util.Utilities;

import java.util.Objects;

/** Models the SQL Interval type for months-years.
 * Always stores the interval value in days. */
public class DBSPTypeMonthsInterval
        extends DBSPTypeBaseType
        implements IsTimeRelatedType, IHasZero, IsIntervalType {
    public enum Units {
        MONTHS,
        YEARS,
        YEARS_TO_MONTHS
    }

    public final Units units;

    public DBSPTypeMonthsInterval(CalciteObject node, Units units, boolean mayBeNull) {
        super(node, DBSPTypeCode.INTERVAL_LONG, mayBeNull);
        this.units = units;
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
    public DBSPExpression getMinValue() {
        return new DBSPIntervalMonthsLiteral(this.units, Integer.MIN_VALUE, this.mayBeNull);
    }

    @Override
    public DBSPExpression getMaxValue() {
        return new DBSPIntervalMonthsLiteral(this.units, Integer.MAX_VALUE, this.mayBeNull);
    }

    @Override
    public DBSPType withMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return new DBSPTypeMonthsInterval(this.getNode(), this.units, mayBeNull);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.mayBeNull, this.units, 9);
    }

    @Override
    public DBSPExpression defaultValue() {
        if (this.mayBeNull)
            return this.none();
        return new DBSPIntervalMonthsLiteral(this.units, 0, false);
    }

    @Override
    public DBSPLiteral getZero() {
        return new DBSPIntervalMonthsLiteral(this.units, 0, this.mayBeNull);
    }

    @Override
    public boolean sameType(DBSPType other) {
        if (!super.sameNullability(other))
            return false;
        DBSPTypeMonthsInterval otherType = other.as(DBSPTypeMonthsInterval.class);
        if (otherType == null)
            return false;
        return this.units == otherType.units;
    }

    @Override
    public String baseTypeWithSuffix() {
        return this.shortName() + "_" + this.units.name() + this.nullableSuffix();
    }

    @Override
    public String toString() {
        return "LongInterval(" +
                switch (this.units) {
                    case YEARS_TO_MONTHS, YEARS -> "Y";
                    default -> "";
                } +
                switch (this.units) {
                    case YEARS_TO_MONTHS, MONTHS -> "M";
                    default -> "";
                } +
                ")";
    }

    @SuppressWarnings("unused")
    public static DBSPTypeMonthsInterval fromJson(JsonNode node, JsonDecoder decoder) {
        boolean mayBeNull = DBSPType.fromJsonMayBeNull(node);
        Units units = Units.valueOf(Utilities.getStringProperty(node, "units"));
        return new DBSPTypeMonthsInterval(CalciteObject.EMPTY, units, mayBeNull);
    }
}
