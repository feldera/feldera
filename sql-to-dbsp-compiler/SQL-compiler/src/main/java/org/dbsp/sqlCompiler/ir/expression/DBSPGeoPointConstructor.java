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

import org.dbsp.sqlCompiler.compiler.IConstructor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.ISameValue;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeGeoPoint;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;
import java.util.Objects;

public final class DBSPGeoPointConstructor
        extends DBSPExpression
        implements IConstructor, ISameValue {
    // Null only when the literal itself is null.
    @Nullable
    public final DBSPExpression left;
    @Nullable
    public final DBSPExpression right;

    public DBSPGeoPointConstructor(CalciteObject node,
                                   @Nullable DBSPExpression left, @Nullable DBSPExpression right,
                                   DBSPType type) {
        super(node, type);
        assert type.is(DBSPTypeGeoPoint.class);
        this.left = left;
        this.right = right;
    }

    public DBSPGeoPointConstructor() {
        super(CalciteObject.EMPTY, new DBSPTypeGeoPoint(CalciteObject.EMPTY, true));
        this.left = null;
        this.right = null;
    }

    public boolean isNull() {
        return this.left == null || this.right == null;
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPGeoPointConstructor(this.getNode(),
                DBSPExpression.nullableDeepCopy(this.left), DBSPExpression.nullableDeepCopy(this.right),
                this.type);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        if (!other.is(DBSPGeoPointConstructor.class))
            return false;
        DBSPGeoPointConstructor o = other.to(DBSPGeoPointConstructor.class);
        return context.equivalent(this.left, o.left) &&
                context.equivalent(this.right, o.right);
    }

    @Override
    public boolean isConstant() {
        return (this.left == null || this.left.isCompileTimeConstant())
                && (this.right == null || this.right.isCompileTimeConstant());
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        if (this.left != null) {
            visitor.property("left");
            this.left.accept(visitor);
        }
        if (this.right != null) {
            visitor.property("right");
            this.right.accept(visitor);
        }
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPGeoPointConstructor o = other.as(DBSPGeoPointConstructor.class);
        if (o == null)
            return false;
        return this.left == o.left && this.right == o.right;
    }

    @Override
    public boolean sameValue(@Nullable ISameValue o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPGeoPointConstructor that = (DBSPGeoPointConstructor) o;
        if (!Objects.equals(left, that.left)) return false;
        return Objects.equals(right, that.right);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append(this.type)
                .append("(");
        if (this.left != null)
            builder.append(this.left);
        else
            builder.append("null");
        if (this.right != null)
            builder.append(this.right);
        else
            builder.append("null");
        return builder.append(")");
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.left, this.right);
    }
}
