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
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;

public final class DBSPBinaryExpression extends DBSPExpression {
    public final DBSPExpression left;
    public final DBSPExpression right;
    public final DBSPOpcode operation;

    public DBSPBinaryExpression(CalciteObject node, DBSPType type, DBSPOpcode operation,
                                DBSPExpression left, DBSPExpression right) {
        super(node, type);
        this.operation = operation;
        this.left = left;
        this.right = right;
    }

    public DBSPBinaryExpression replaceSources(DBSPExpression left, DBSPExpression right) {
        return new DBSPBinaryExpression(this.getNode(), this.type, this.operation, left, right);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        this.type.accept(visitor);
        this.left.accept(visitor);
        this.right.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPBinaryExpression o = other.as(DBSPBinaryExpression.class);
        if (o == null)
            return false;
        return this.left == o.left &&
                this.right == o.right &&
                this.operation == o.operation &&
                this.hasSameType(o);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.left)
                .append(" ")
                .append(this.left.getType().mayBeNull ? "?" : "")
                .append(this.operation.toString())
                .append(this.right.getType().mayBeNull ? "?" : "")
                .append(" ")
                .append(this.right);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPBinaryExpression(this.getNode(), this.getType(), this.operation,
                this.left.deepCopy(), this.right.deepCopy());
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPBinaryExpression otherExpression = other.as(DBSPBinaryExpression.class);
        if (otherExpression == null)
            return false;
        return this.operation == otherExpression.operation &&
                context.equivalent(this.left, otherExpression.left) &&
                context.equivalent(this.right, otherExpression.right);
    }
}
