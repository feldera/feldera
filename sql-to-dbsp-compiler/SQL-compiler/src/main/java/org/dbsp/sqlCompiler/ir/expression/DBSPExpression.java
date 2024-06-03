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

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeResult;
import org.dbsp.sqlCompiler.ir.type.IHasType;

import javax.annotation.Nullable;

/** Base class for all expressions */
public abstract class DBSPExpression
        extends DBSPNode
        implements IHasType, IDBSPInnerNode {
    public final DBSPType type;

    protected DBSPExpression(CalciteObject node, DBSPType type) {
        super(node);
        this.type = type;
    }

    /** Generates an expression that calls clone() on this. */
    public DBSPExpression applyClone() {
        assert !this.type.is(DBSPTypeRef.class): "Cloning a reference " + this;
        if (this.is(DBSPCloneExpression.class))
            return this;
        return new DBSPCloneExpression(this.getNode(), this);
    }

    @Override
    public DBSPType getType() {
        return this.type;
    }

    public DBSPDerefExpression deref() {
        return new DBSPDerefExpression(this);
    }

    public DBSPBorrowExpression borrow() {
        return new DBSPBorrowExpression(this);
    }

    /** Unwrap a Rust 'Result' type */
    public DBSPExpression resultUnwrap() {
        DBSPType resultType;
        if (this.type.is(DBSPTypeAny.class)) {
            resultType = this.type;
        } else {
            DBSPTypeResult type = this.type.to(DBSPTypeResult.class);
            resultType = type.getTypeArg(0);
        }
        return new DBSPApplyMethodExpression(this.getNode(), "unwrap", resultType, this);
    }

    /** Unwrap an expression with a nullable type */
    public DBSPExpression unwrap() {
        assert this.type.mayBeNull;
        return new DBSPUnwrapExpression(this);
    }

    public DBSPExpressionStatement toStatement() {
        return new DBSPExpressionStatement(this);
    }

    public DBSPExpression borrow(boolean mutable) {
        return new DBSPBorrowExpression(this, mutable);
    }

    public DBSPFieldExpression field(int index) {
        return new DBSPFieldExpression(this, index);
    }

    public DBSPExpression question() { return new DBSPQuestionExpression(this); }

    /** Convenient shortcut to wrap an expression into a Some() constructor. */
    public DBSPExpression some() {
        return new DBSPSomeExpression(this.getNode(), this);
    }

    public DBSPClosureExpression closure(DBSPParameter... parameters) {
        return new DBSPClosureExpression(this, parameters);
    }

    public DBSPExpression is_null() {
        if (!this.getType().mayBeNull)
            return new DBSPBoolLiteral(false);
        return new DBSPIsNullExpression(this.getNode(), this);
    }

    /** The exact same expression, using reference equality */
    public static boolean same(@Nullable DBSPExpression left, @Nullable DBSPExpression right) {
        if (left == null)
            return right == null;
        return left.equals(right);
    }

    public DBSPExpression call(DBSPExpression... arguments) {
        return new DBSPApplyExpression(this, arguments);
    }

    public DBSPExpression cast(DBSPType to) {
        DBSPType fromType = this.getType();
        if (fromType.sameType(to)) {
            return this;
        }
        return new DBSPCastExpression(this.getNode(), this, to);
    }

    public DBSPExpression applyCloneIfNeeded() {
        if (this.getType().hasCopy())
            return this;
        return this.applyClone();
    }

    public boolean hasSameType(DBSPExpression other) {
        return this.type.sameType(other.type);
    }

    /** Make a deep copy of this expression. */
    public abstract DBSPExpression deepCopy();

    public static @Nullable DBSPExpression nullableDeepCopy(@Nullable DBSPExpression expression) {
        if (expression == null)
            return null;
        return expression.deepCopy();
    }

    /** Check expressions for equivalence in a specified context.
     * @param context Specifies variables that are renamed.
     * @param other Expression to compare against.
     * @return True if this expression is equivalent with 'other' in the specified context.
     */
    public abstract boolean equivalent(
            EquivalenceContext context,
            DBSPExpression other);

    /** Check expressions without free variables for equivalence.
     * @param other Expression to compare against.
     * @return True if this expression is equivalent with 'other'. */
    public boolean equivalent(DBSPExpression other) {
        return EquivalenceContext.equiv(this, other);
    }
}
