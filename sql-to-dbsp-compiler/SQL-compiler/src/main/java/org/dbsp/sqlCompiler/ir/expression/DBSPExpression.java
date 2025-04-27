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

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.AnalyzedSet;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.IConstructor;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.BetaReduction;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.RepeatedExpressions;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeResult;
import org.dbsp.sqlCompiler.ir.type.IHasType;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/** Base class for all expressions */
public abstract class DBSPExpression
        extends DBSPNode
        implements IHasType, IDBSPInnerNode {
    public final DBSPType type;

    protected DBSPExpression(CalciteObject node, DBSPType type) {
        super(node);
        this.type = type;
    }

    @Override
    public boolean isExpression() {
        return true;
    }

    public boolean isCompileTimeConstant() {
        return this.is(IConstructor.class) &&
                this.to(IConstructor.class).isConstant();
    }

    /** Generates an expression that calls clone() on this. */
    public DBSPExpression applyClone() {
        Utilities.enforce(!this.type.is(DBSPTypeRef.class), "Cloning a reference " + this);
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

    @CheckReturnValue
    public DBSPExpression wrapBoolIfNeeded() {
        DBSPType type = this.getType();
        if (type.mayBeNull) {
            return new DBSPUnaryExpression(
                    this.getNode(), type.withMayBeNull(false),
                    DBSPOpcode.WRAP_BOOL, this);
        }
        return this;
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
        Utilities.enforce(this.type.mayBeNull, "Unwrapping non-nullable type");
        return new DBSPUnwrapExpression(this);
    }

    /** Unwrap an expression if the type is nullable */
    public DBSPExpression unwrapIfNullable() {
        if (this.type.mayBeNull)
            return this.unwrap();
        return this;
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

    /** Apply some only if the expression is not nullable */
    public DBSPExpression someIfNeeded() {
        if (this.getType().mayBeNull)
            return this;
        return this.some();
    }

    public DBSPClosureExpression closure() {
        return new DBSPClosureExpression(this);
    }

    public DBSPClosureExpression closure(DBSPParameter... parameters) {
        return new DBSPClosureExpression(this, parameters);
    }

    public DBSPClosureExpression closure(DBSPVariablePath... parameters) {
        return new DBSPClosureExpression(this,
                Linq.map(parameters, DBSPVariablePath::asParameter, DBSPParameter.class));
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

    public DBSPExpression call(DBSPType resultType, DBSPExpression... arguments) {
        return new DBSPApplyExpression(this, resultType, arguments);
    }

    public DBSPExpression applyMethod(String method, DBSPType resultType, DBSPExpression... arguments) {
        return new DBSPApplyMethodExpression(method, resultType, this, arguments);
    }

    public DBSPExpression cast(DBSPType to, boolean force, boolean safe) {
        DBSPType fromType = this.getType();
        // Still, do not insert a cast if the source is a cast to the exact same type
        if (this.is(DBSPCastExpression.class)
                && this.to(DBSPCastExpression.class).type.sameType(to))
            force = false;
        if (fromType.sameType(to) && !force) {
            return this;
        }
        return new DBSPCastExpression(this.getNode(), this, to, safe);
    }

    /** Cast an expression to its own type, but nullable */
    public DBSPExpression castToNullable() {
        if (this.getType().mayBeNull)
            return this;
        return this.cast(this.getType().withMayBeNull(true), false);
    }

    public DBSPExpression cast(DBSPType to, boolean safe) {
        boolean force = type.is(DBSPTypeDecimal.class);
        // Computations on decimal values in Rust do not produce the correct result type,
        // so they must be always cast
        return this.cast(to, force, safe);
    }

    /** Insert a cast which may only change nullability */
    public DBSPExpression nullabilityCast(DBSPType to, boolean safe) {
        DBSPType sourceType = this.getType();
        Utilities.enforce(sourceType.sameTypeIgnoringNullability(to));
        return this.cast(to, safe);
    }

    public DBSPExpression applyCloneIfNeeded() {
        if (this.getType().hasCopy())
            return this;
        if (this.is(DBSPLiteral.class))
            return this;
        if (this.is(IConstructor.class))
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

    /** Attempt to simplify the current expression */
    @CheckReturnValue
    public DBSPExpression reduce(DBSPCompiler compiler) {
        BetaReduction beta = new BetaReduction(compiler);
        DBSPExpression reduced = beta.apply(this).to(DBSPExpression.class);
        Simplify simplify = new Simplify(compiler);
        return simplify.apply(reduced).to(DBSPExpression.class);
    }

    /** 'this' must be an expression with a tuple type.
     * @return a DBSPTupleExpression that contains all fields of this expression (cloned if necessary). */
    public List<DBSPExpression> allFields() {
        DBSPTypeTupleBase type = this.getType().to(DBSPTypeTupleBase.class);
        List<DBSPExpression> result = new ArrayList<>();
        for (int i = 0; i < type.tupFields.length; i++) {
            result.add(this.deepCopy().field(i).applyCloneIfNeeded());
        }
        return result;
    }

    /** Check expressions for equivalence in a specified context.
     * @param context Specifies variables that are renamed.
     * @param other Expression to compare against.
     * @return True if this expression is equivalent with 'other' in the specified context.
     */
    public abstract boolean equivalent(EquivalenceContext context, DBSPExpression other);

    /** Check expressions without free variables for equivalence.
     * @param other Expression to compare against.
     * @return True if this expression is equivalent with 'other'. */
    public boolean equivalent(DBSPExpression other) {
        return EquivalenceContext.equiv(this, other);
    }

    public DBSPExpression not() {
        Utilities.enforce(this.getType().is(DBSPTypeBool.class));
        return new DBSPUnaryExpression(this.getNode(), this.getType(), DBSPOpcode.NOT, this);
    }

    /** Cache here expressions which we know are trees, to avoid re-analyzing them */
    static AnalyzedSet<DBSPExpression> treeExpressions = new AnalyzedSet<>();

    /** If this expression is a DAG, convert it to a tree, otherwise it leave it unchanged */
    public DBSPExpression ensureTree(DBSPCompiler compiler) {
        if (treeExpressions.contains(this))
            return this;
        RepeatedExpressions repeated = new RepeatedExpressions(compiler, true, false);
        repeated.apply(this);
        DBSPExpression result = this;
        if (repeated.hasDuplicate())
            result = this.deepCopy();
        treeExpressions.done(result);
        return result;
    }

    public static DBSPType getJsonType(JsonNode node, JsonDecoder decoder) {
        return DBSPNode.fromJsonInner(node, "type", decoder, DBSPType.class);
    }
}
