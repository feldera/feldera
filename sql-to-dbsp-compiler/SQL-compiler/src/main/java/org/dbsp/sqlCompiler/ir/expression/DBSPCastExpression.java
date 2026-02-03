/*
 * Copyright 2023 VMware, Inc.
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

package org.dbsp.sqlCompiler.ir.expression;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeNull;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeSqlResult;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

/** This class represents a cast of an expression to a given type.
 * There are actually multiple kinds of casts, depending on the {@link #safe} value.
 *
 * <p>In the beginning all casts have safe set to one of {@link CastType#SqlUnsafe} and {@link CastType#SqlSafe}.
 * These represent the abstract SQL casts.  In later stages, while lowering to Rust,
 * each such cast is usually converted to a pair of casts: Unwrap(RustCast()).
 *
 * <p>All the cast_* functions in sqllib return values with type Result.
 * So each Sql cast is converted into a pair of operations: one which performs the cast and can return Err,
 * and another one which unwraps the Err to the expected type.  Unwrapping returns None for safe casts,
 * and causes panics for unsafe casts. */
public final class DBSPCastExpression extends DBSPExpression {
    public final DBSPExpression source;
    public enum CastType {
        /** An unsafe cast, which may crash at runtime.  Represents SQL CAST(x AS type) expression. */
        SqlUnsafe,
        /** Represents a high-level SAFE_CAST(x as type) expression.  Result type must always be nullable,
         * since on failure the cast returns NULL. */
        SqlSafe,

        /** A Rust cast which converts a result of type SqlResult[T] to a value of type T.
         * Used to implement unsafe SQL CASTs.  Later these casts are replaced with {@link DBSPHandleErrorExpression},
         * with a {@link DBSPHandleErrorExpression.RuntimeBehavior#Panic}.
         * For scalar types, a {@link CastType#SqlUnsafe} cast will be converted to a pair of casts
         * UnwrapResultUnsafe(RustCast(source, ...)), which will be later converted to a
         * HandleErrorExpression(RustCast(source, ...)). */
        UnwrapResultUnsafe,
        /** A Rust cast which converts a result of type SqlResult[Option[T]] to a value of type Option[T].
         * Later these are replaced with {@link DBSPHandleErrorExpression} with a value of
         * {@link DBSPHandleErrorExpression.RuntimeBehavior#ReturnNone}.
         * For scalar types, a {@link CastType#SqlSafe} cast will be converted to a pair of casts
         * UnwrapResultSafe(RustCast(source, ...)), which will be later converted to a
         * HandleErrorExpression(RustCast(source, ...)). */
        UnwrapResultSafe,

        /** A call to a Rust function in sqllib, which always returns a SqlResult[T] value. */
        RustCast;

        public boolean isSql() {
            return this == SqlUnsafe || this == SqlSafe;
        }

        public boolean isUnwrap() {
            return this == UnwrapResultSafe || this == UnwrapResultUnsafe;
        }

        public boolean isSafe() {
            return this == SqlSafe || this == UnwrapResultSafe;
        }

        public CastType getUnwrap() {
            return switch (this) {
                case SqlUnsafe -> UnwrapResultUnsafe;
                case SqlSafe -> UnwrapResultSafe;
                default -> this;
            };
        }
    }

    public final CastType safe;

    public DBSPCastExpression(CalciteObject node, DBSPExpression source, DBSPType to, CastType safe) {
        super(node, to);
        this.source = source;
        this.safe = safe;
        // SQL Casts never produce SqlResult results
        Utilities.enforce(!safe.isSql() || to.as(DBSPTypeSqlResult.class) == null);
        // RustCast must produce a sqllib SqlResult type
        Utilities.enforce(safe != CastType.RustCast || to.is(DBSPTypeSqlResult.class));
        // UnwrapResult must have an input which has type SqlResult<T>
        Utilities.enforce(!safe.isUnwrap() || source.getType().is(DBSPTypeSqlResult.class));
        // UnwrapResult must produce an output which has type T
        Utilities.enforce(!safe.isUnwrap() ||
                to.sameType(source.getType().to(DBSPTypeSqlResult.class).getWrappedType()),
                () -> "Target type " + to + " does not match unwrapped type " +
                        source.getType().to(DBSPTypeSqlResult.class).getWrappedType());
        // Safe casts must produce a nullable type
        Utilities.enforce(!safe.isSafe() || to.mayBeNull);
        Utilities.enforce(source.getType().is(DBSPTypeNull.class)
                        || source.getType().is(DBSPTypeTupleBase.class) == to.is(DBSPTypeTupleBase.class)
                        || source.getType().is(DBSPTypeVariant.class) || to.is(DBSPTypeVariant.class),
                () -> "Cast to/from tuple from/to non-tuple " + source.getType() + " to " + to);
        Utilities.enforce(to.code != DBSPTypeCode.INTERNED_STRING, () -> "Cast to INTERNED");
        Utilities.enforce(source.getType().code != DBSPTypeCode.INTERNED_STRING, () -> "Cast from INTERNED");
    }

    public DBSPCastExpression replaceSource(DBSPExpression source) {
        Utilities.enforce(source.getType().sameType(this.source.getType()));
        return new DBSPCastExpression(this.getNode(), source, this.type, this.safe);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("source");
        this.source.accept(visitor);
        visitor.property("type");
        this.type.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPCastExpression o = other.as(DBSPCastExpression.class);
        if (o == null)
            return false;
        return this.source == o.source &&
                this.safe == o.safe &&
                this.hasSameType(o);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("((")
                .append(this.type)
                .append(")")
                .append(this.source)
                .append(")");
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPCastExpression(this.getNode(), this.source.deepCopy(), this.getType(), this.safe);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPCastExpression otherExpression = other.as(DBSPCastExpression.class);
        if (otherExpression == null)
            return false;
        return context.equivalent(this.source, otherExpression.source) &&
                this.safe == otherExpression.safe &&
                this.hasSameType(other);
    }

    @SuppressWarnings("unused")
    public static DBSPCastExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPExpression source = fromJsonInner(node, "source", decoder, DBSPExpression.class);
        CastType safe = CastType.valueOf(Utilities.getStringProperty(node, "safe"));
        DBSPType type = getJsonType(node, decoder);
        return new DBSPCastExpression(CalciteObject.EMPTY, source, type, safe);
    }
}
