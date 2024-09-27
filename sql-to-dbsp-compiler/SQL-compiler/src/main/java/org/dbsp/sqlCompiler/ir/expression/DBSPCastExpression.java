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

import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeVec;
import org.dbsp.util.IIndentStream;

/** This class does not correspond to any Rust primitive construct.
 * It is compiled into a function invocation, depending on the involved types.
 * It represents a cast of an expression to a given type. */
public final class DBSPCastExpression extends DBSPExpression {
    public final DBSPExpression source;

    public DBSPCastExpression(CalciteObject node, DBSPExpression source, DBSPType to) {
        super(node, to);
        this.source = source;
        this.validate();
    }

    void validate() {
        DBSPType sourceType = source.getType();
        if (type.is(DBSPTypeVariant.class))
            // Any cast to variant is ok
            return;
        if (type.is(DBSPTypeBaseType.class))
            assert sourceType.is(DBSPTypeBaseType.class);
        else if (type.is(DBSPTypeVec.class)) {
            if (!sourceType.is(DBSPTypeVec.class) &&
                    !sourceType.is(DBSPTypeVariant.class)) {
                this.unsupported();
            }
        } else if (type.is(DBSPTypeMap.class)) {
            if (!sourceType.is(DBSPTypeMap.class) &&
                    !sourceType.is(DBSPTypeVariant.class)) {
                this.unsupported();
            }
        } else if (type.is(DBSPTypeTuple.class)) {
            if (!sourceType.is(DBSPTypeTuple.class) &&
                    !sourceType.is(DBSPTypeVariant.class)) {
                this.unsupported();
            }
            if (sourceType.is(DBSPTypeVariant.class)) {
                // TODO
                this.unimplemented();
            }
        }
        if (type.is(DBSPTypeVariant.class)) {
            if (sourceType.is(DBSPTypeTuple.class)) {
                // TODO
                this.unimplemented();
            }
        }
    }

    void unsupported() {
        throw new UnsupportedException("Casting of value with type '" +
                this.source.getType().asSqlString() +
                "' to the target type '" + this.type.asSqlString() + "' not supported", this.getNode());
    }

    void unimplemented() {
        throw new UnimplementedException("Casting of value with type '" +
                this.source.getType().asSqlString() +
                "' to the target type '" + this.type.asSqlString() + "' not yet implemented", this.getNode());
    }

    public DBSPCastExpression replaceSource(DBSPExpression source) {
        assert source.getType().sameType(this.source.getType());
        return new DBSPCastExpression(this.getNode(), source, this.type);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        this.source.accept(visitor);
        this.getType().accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPCastExpression o = other.as(DBSPCastExpression.class);
        if (o == null)
            return false;
        return this.source == o.source &&
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
        return new DBSPCastExpression(this.getNode(), this.source.deepCopy(), this.getType());
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPCastExpression otherExpression = other.as(DBSPCastExpression.class);
        if (otherExpression == null)
            return false;
        return context.equivalent(this.source, otherExpression.source) &&
                this.hasSameType(other);
    }
}
