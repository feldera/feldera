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

package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.circuit.DBSPNode;
import org.dbsp.sqlCompiler.circuit.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.IHasType;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;

public abstract class DBSPExpression
        extends DBSPNode
        implements IHasType, IDBSPInnerNode {
    // Null for an expression that evaluates to void.
    @Nullable
    public final DBSPType type;

    protected DBSPExpression(@Nullable Object node, @Nullable DBSPType type) {
        super(node);
        this.type = type;
    }

    /**
     * Generates an expression that calls clone() on this.
     */
    public DBSPExpression applyClone() {
        return new DBSPCloneExpression(this.getNode(), this);
    }

    @Override
    @Nullable
    public DBSPType getType() {
        return this.type;
    }

    public DBSPExpression deref() {
        return new DBSPDerefExpression(this);
    }

    public DBSPExpression borrow() {
        return new DBSPBorrowExpression(this);
    }

    public DBSPExpression unwrap() {
        return new DBSPApplyMethodExpression("unwrap", DBSPTypeAny.INSTANCE, this);
    }

    public DBSPExpression borrow(boolean mutable) {
        return new DBSPBorrowExpression(this, mutable);
    }

    public DBSPExpression field(int index) {
        return new DBSPFieldExpression(this, index);
    }

    /**
     * Convenient shortcut to wrap an expression into a Some() constructor.
     */
    public DBSPExpression some() {
        return new DBSPSomeExpression(null, this);
    }

    public DBSPClosureExpression closure(DBSPParameter... parameters) {
        return new DBSPClosureExpression(this, parameters);
    }

    public DBSPExpression is_null() {
        if (!this.getNonVoidType().mayBeNull)
            return DBSPBoolLiteral.FALSE;
        return new DBSPIsNullExpression(this.getNode(), this);
    }

    public static boolean same(@Nullable DBSPExpression left, @Nullable DBSPExpression right) {
        if (left == null)
            return right == null;
        return left.equals(right);
    }

    public DBSPExpression call(DBSPExpression... arguments) {
        return new DBSPApplyExpression(this, arguments);
    }

    public DBSPExpression cast(DBSPType to) {
        DBSPType fromType = this.getNonVoidType();
        if (fromType.sameType(to)) {
            return this;
        }
        return new DBSPCastExpression(this.getNode(), this, to);
    }

    public DBSPExpression applyCloneIfNeeded() {
        if (this.getNonVoidType().hasCopy())
            return this;
        return this.applyClone();
    }

    public boolean hasSameType(DBSPExpression other) {
        return this.type == other.type;
    }
}
