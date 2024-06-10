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
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.IDBSPContainer;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeVec;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Represents a (constant) vector described by its elements. */
public final class DBSPVecLiteral extends DBSPLiteral implements IDBSPContainer {
    @Nullable
    public final List<DBSPExpression> data;
    public final DBSPTypeVec vecType;

    public DBSPVecLiteral(DBSPType elementType) {
        super(CalciteObject.EMPTY, new DBSPTypeVec(elementType, false), false);
        this.data = new ArrayList<>();
        this.vecType = this.getType().to(DBSPTypeVec.class);
    }

    public DBSPVecLiteral(DBSPType vectorType, boolean isNull) {
        super(CalciteObject.EMPTY, vectorType, isNull);
        this.data = isNull ? null : new ArrayList<>();
        this.vecType = this.getType().to(DBSPTypeVec.class);
    }

    public DBSPVecLiteral(CalciteObject node, DBSPType type, @Nullable List<DBSPExpression> data) {
        super(node, type, data == null);
        this.vecType = this.getType().to(DBSPTypeVec.class);
        this.data = new ArrayList<>();
        if (data != null) {
            for (DBSPExpression e : data) {
                if (!e.getType().sameType(data.get(0).getType()))
                    throw new InternalCompilerError("Not all values of set have the same type:" +
                            e.getType() + " vs " + data.get(0).getType(), this);
                this.add(e);
            }
        }
    }

    public DBSPVecLiteral(boolean mayBeNull, DBSPExpression... data) {
        super(CalciteObject.EMPTY, new DBSPTypeVec(data[0].getType(), mayBeNull), false);
        this.vecType = this.getType().to(DBSPTypeVec.class);
        this.data = new ArrayList<>();
        for (DBSPExpression e: data) {
            if (!e.getType().sameType(data[0].getType()))
                throw new InternalCompilerError("Not all values of set have the same type:" +
                        e.getType() + " vs " + data[0].getType(), this);
            this.add(e);
        }
    }

    public DBSPVecLiteral(DBSPExpression... data) {
       this(false, data);
    }

    public DBSPType getElementType() {
        return this.vecType.getTypeArg(0);
    }

    public IDBSPContainer add(DBSPExpression expression) {
        // We expect the expression to be a constant value (a literal)
        if (!expression.getType().sameType(this.getElementType()))
            throw new InternalCompilerError("Added element " + expression + " type " +
                    expression.getType() + " does not match vector type " + this.getElementType(), this);
        Objects.requireNonNull(this.data).add(expression);
        return this;
    }

    public void add(DBSPVecLiteral other) {
        if (!this.getType().sameType(other.getType()))
            throw new InternalCompilerError("Added vectors do not have the same type " +
                    this.getElementType() + " vs " + other.getElementType(), this);
        Objects.requireNonNull(other.data).forEach(this::add);
    }

    public int size() {
        return Objects.requireNonNull(this.data).size();
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        if (this.data != null) {
            for (DBSPExpression expr : this.data)
                expr.accept(visitor);
        }
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        return new DBSPVecLiteral(this.getNode(), this.type.setMayBeNull(mayBeNull), this.data);
    }

    @Override
    public boolean sameValue(@Nullable DBSPLiteral o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPVecLiteral that = (DBSPVecLiteral) o;
        if (!Objects.equals(data, that.data)) return false;
        return vecType.equals(that.vecType);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        if (this.data == null)
            return builder.append("(")
                    .append(this.type)
                    .append(")")
                    .append("null");
        return builder.append("vec!(")
                .increase()
                .intercalateI(System.lineSeparator(), this.data)
                .append(")")
                .decrease()
                .newline();
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPVecLiteral(this.getNode(), this.getType(),
                this.data != null ? Linq.map(this.data, DBSPExpression::deepCopy) : null);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPVecLiteral otherExpression = other.as(DBSPVecLiteral.class);
        if (otherExpression == null)
            return false;
        return context.equivalent(this.data, otherExpression.data);
    }

    // TODO: implement equals and hashcode
}
