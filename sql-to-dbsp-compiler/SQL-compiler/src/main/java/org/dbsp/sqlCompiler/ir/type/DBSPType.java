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

package org.dbsp.sqlCompiler.ir.type;

import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.util.IndentStream;

import javax.annotation.Nullable;
import java.util.Objects;

public abstract class DBSPType extends DBSPNode implements IDBSPInnerNode {
    public final DBSPTypeCode code;
    /** True if this type may include null values. */
    public final boolean mayBeNull;

    protected DBSPType(CalciteObject node, DBSPTypeCode code, boolean mayBeNull) {
        super(node);
        this.mayBeNull = mayBeNull;
        this.code = code;
    }

    protected DBSPType(boolean mayBeNull, DBSPTypeCode code) {
        this(CalciteObject.EMPTY, code, mayBeNull);
    }

    public void wrapOption(IndentStream builder, String type) {
        if (this.mayBeNull) {
            builder.append("Option<").append(type).append(">");
            return;
        }
        builder.append(type);
    }

    /**
     * This is like 'equals', but it always takes a DBSPType.
     */
    public abstract boolean sameType(DBSPType other);

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean sameNullability(DBSPType other) {
        return this.mayBeNull == other.mayBeNull;
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        if (!other.is(DBSPType.class))
            return false;
        return this.sameType(other.to(DBSPType.class));
    }

    public static boolean sameTypes(DBSPType[] left, DBSPType[] right) {
        if (left.length != right.length)
            return false;
        for (int i = 0; i < left.length; i++) {
            if (!left[i].sameType(right[i]))
                return false;
        }
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DBSPType))
            return false;
        return this.sameType((DBSPType)obj);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.mayBeNull);
    }

    /**
     * Return a copy of this type with the mayBeNull bit set to the specified value.
     * @param mayBeNull  Value for the mayBeNull bit.
     */
    public abstract DBSPType setMayBeNull(boolean mayBeNull);

    public DBSPType ref() {
        return new DBSPTypeRef(this);
    }

    public DBSPType ref(boolean mutable) {
        return new DBSPTypeRef(this, mutable, false);
    }

    public DBSPVariablePath var(String variable) {
        return new DBSPVariablePath(variable, this);
    }

    public DBSPPathExpression path(DBSPPath path) {
        return new DBSPPathExpression(this, path);
    }

    /** Given a type that is expected to be a reference type,
     * compute the type that is the dereferenced type. */
    public DBSPType deref() {
        if (this.is(DBSPTypeAny.class))
            return this;
        return this.to(DBSPTypeRef.class).type;
    }

    /** The null value with this type. */
    public DBSPExpression nullValue() {
        return DBSPLiteral.none(this);
    }

    /** True if this type has a Rust 'copy' method. */
    public boolean hasCopy() {
        return true;
    }

    /** Returns "N" if the type may be nullable, "" otherwise.
     * Used in the code generation. */
    public String nullableSuffix() {
        return this.mayBeNull ? "N" : "";
    }

    @Nullable
    public String asSqlString() {
        return this.code.sqlName;
    }

    /** The name of the type as used in the runtime library.
     * Only defined for base types. */
    public String baseTypeWithSuffix() {
        return this.to(DBSPTypeBaseType.class).shortName() + this.nullableSuffix();
    }

    /** Returns a lambda which casts the current type to the specified type. */
    public DBSPExpression caster(DBSPType to) {
        throw new UnimplementedException("Casting from " + this + " to " + to, to);
    }

    /** Type must be nullable.
     * Return an expression that has a value of this type with value None. */
    public DBSPExpression none() {
        return DBSPLiteral.none(this);
    }
}
