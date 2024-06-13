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

import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeVec;
import org.dbsp.sqlCompiler.ir.type.primitive.*;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public abstract class DBSPLiteral extends DBSPExpression {
    public final boolean isNull;

    protected DBSPLiteral(CalciteObject node, DBSPType type, boolean isNull) {
        super(node, type);
        this.isNull = isNull;
        if (this.isNull && !type.mayBeNull && !type.is(DBSPTypeAny.class))
            throw new UnsupportedException("Type " + type + " cannot represent null", node);
    }

    /** Represents a "null" value of the specified type. */
    public static DBSPExpression none(DBSPType type) {
        if (!type.mayBeNull)
            throw new RuntimeException(type.toString() + " cannot represent NULL");
        if (type.is(DBSPTypeInteger.class)) {
            DBSPTypeInteger it = type.to(DBSPTypeInteger.class);
            if (!it.signed)
                throw new UnimplementedException("Null of type ", type);
            switch (it.getWidth()) {
                case 8:
                    return new DBSPI8Literal();
                case 16:
                    return new DBSPI16Literal();
                case 32:
                    return new DBSPI32Literal();
                case 64:
                    return new DBSPI64Literal();
                case 128:
                    return new DBSPI128Literal();
            }
        } else if (type.is(DBSPTypeBool.class)) {
            return new DBSPBoolLiteral();
        } else if (type.is(DBSPTypeDate.class)) {
            return new DBSPDateLiteral();
        } else if (type.is(DBSPTypeDecimal.class)) {
            return new DBSPDecimalLiteral(type.getNode(), type, null);
        } else if (type.is(DBSPTypeDouble.class)) {
            return new DBSPDoubleLiteral();
        } else if (type.is(DBSPTypeReal.class)) {
            return new DBSPRealLiteral();
        } else if (type.is(DBSPTypeGeoPoint.class)) {
            return new DBSPGeoPointLiteral();
        } else if (type.is(DBSPTypeMillisInterval.class)) {
            return new DBSPIntervalMillisLiteral();
        } else if (type.is(DBSPTypeMonthsInterval.class)) {
            return new DBSPIntervalMonthsLiteral();
        } else if (type.is(DBSPTypeString.class)) {
            return new DBSPStringLiteral(CalciteObject.EMPTY, type, null, StandardCharsets.UTF_8);
        } else if (type.is(DBSPTypeTime.class)) {
            return new DBSPTimeLiteral();
        } else if (type.is(DBSPTypeVec.class)) {
            return new DBSPVecLiteral(type, true);
        } else if (type.is(DBSPTypeTuple.class)) {
            return new DBSPTupleExpression(type.to(DBSPTypeTuple.class));
        } else if (type.is(DBSPTypeNull.class)) {
            return new DBSPNullLiteral();
        } else if (type.is(DBSPTypeTimestamp.class)) {
            return new DBSPTimestampLiteral();
        } else if (type.is(DBSPTypeBinary.class)) {
            return new DBSPBinaryLiteral(type.getNode(), type, null);
        }
        throw new UnimplementedException(type);
    }

    public String wrapSome(String value) {
        if (this.getType().mayBeNull)
            return "Some(" + value + ")";
        return value;
    }

    /** True if this and the other literal have the same type and value. */
    public abstract boolean sameValue(@Nullable DBSPLiteral other);

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPLiteral otherLiteral = other.as(DBSPLiteral.class);
        if (otherLiteral == null)
            return false;
        return this.sameValue(otherLiteral);
    }

    public boolean mayBeNull() {
        return this.getType().mayBeNull;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }

    boolean hasSameType(DBSPLiteral other) {
        return this.getType().sameType(other.getType());
    }

    /** The same literal but with the specified nullability. */
    public abstract DBSPLiteral getWithNullable(boolean mayBeNull);

    @Nullable
    public <T> T checkIfNull(@Nullable T value, boolean mayBeNull) {
        if (!mayBeNull)
            return Objects.requireNonNull(value);
        return value;
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPLiteral o = other.as(DBSPLiteral.class);
        if (o == null)
            return false;
        return this.sameValue(o) && this.hasSameType(o);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPLiteral that = (DBSPLiteral) o;
        return this.sameValue(that);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isNull);
    }
}
