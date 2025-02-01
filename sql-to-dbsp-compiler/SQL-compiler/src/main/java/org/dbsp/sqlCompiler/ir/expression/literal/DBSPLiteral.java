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

import org.dbsp.sqlCompiler.compiler.IConstructor;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.ISameValue;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPGeoPointConstructor;
import org.dbsp.sqlCompiler.ir.expression.DBSPMapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariantExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.*;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public abstract class DBSPLiteral extends DBSPExpression
        implements ISameValue, IConstructor {
    protected final boolean isNull;

    protected DBSPLiteral(CalciteObject node, DBSPType type, boolean isNull) {
        super(node, type);
        this.isNull = isNull;
        if (this.isNull && !type.mayBeNull && !type.is(DBSPTypeAny.class))
            throw new UnsupportedException("Type " + type + " cannot represent null", node);
    }

    public boolean isConstant() { return true; }

    public boolean isNull() {
        return this.isNull;
    }

    /** Represents a "null" value of the specified type. */
    public static DBSPExpression none(DBSPType type) {
        if (!type.mayBeNull)
            throw new RuntimeException(type + " cannot represent NULL");
        return switch (type.code) {
            case INT8 -> new DBSPI8Literal();
            case INT16 -> new DBSPI16Literal();
            case INT32 -> new DBSPI32Literal();
            case INT64 -> new DBSPI64Literal();
            case INT128 -> new DBSPI128Literal();
            case BOOL -> new DBSPBoolLiteral();
            case DATE -> new DBSPDateLiteral();
            case DECIMAL -> new DBSPDecimalLiteral(type.getNode(), type, null);
            case DOUBLE -> new DBSPDoubleLiteral();
            case REAL -> new DBSPRealLiteral();
            case GEOPOINT -> new DBSPGeoPointConstructor();
            case INTERVAL_SHORT -> new DBSPIntervalMillisLiteral(type.getNode(), type, null);
            case INTERVAL_LONG -> new DBSPIntervalMonthsLiteral(type.getNode(), type, null);
            case STRING -> new DBSPStringLiteral(CalciteObject.EMPTY, type, null, StandardCharsets.UTF_8);
            case TIME -> new DBSPTimeLiteral();
            case ARRAY -> new DBSPArrayExpression(type, true);
            case MAP -> new DBSPMapExpression(type.to(DBSPTypeMap.class), null, null);
            case TUPLE -> DBSPTupleExpression.none(type.to(DBSPTypeTuple.class));
            case NULL -> new DBSPNullLiteral();
            case TIMESTAMP -> new DBSPTimestampLiteral();
            case BYTES -> new DBSPBinaryLiteral(type.getNode(), type, null);
            case VARIANT -> new DBSPVariantExpression(null, type);
            case STRUCT -> type.to(DBSPTypeStruct.class).toTuple().none();
            case UUID -> new DBSPUuidLiteral();
            default -> throw new InternalCompilerError("Unexpected type for NULL literal " + type, type);
        };
    }

    public String wrapSome(String value) {
        if (this.getType().mayBeNull)
            return "Some(" + value + ")";
        return value;
    }

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
    public boolean sameFields(IDBSPInnerNode other) {
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

    public abstract String toSqlString();

    @Override
    public int hashCode() {
        return Objects.hash(isNull);
    }
}
