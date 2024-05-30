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

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;

/** Unary operation */
public final class DBSPUnaryExpression extends DBSPExpression {
    public final DBSPExpression source;
    public final DBSPOpcode operation;

    @SuppressWarnings("ConstantConditions")
    public DBSPUnaryExpression(CalciteObject node, DBSPType type, DBSPOpcode operation, DBSPExpression operand) {
        super(node, type);
        this.operation = operation;
        this.source = operand;
        if (this.source == null)
            throw new InternalCompilerError("Null operand", node);
    }

    public DBSPUnaryExpression replaceSource(DBSPExpression source) {
        return new DBSPUnaryExpression(this.getNode(), this.type, this.operation, source);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        this.type.accept(visitor);
        this.source.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPUnaryExpression o = other.as(DBSPUnaryExpression.class);
        if (o == null)
            return false;
        return this.source == o.source &&
                this.operation == o.operation &&
                this.hasSameType(o);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("(")
                .append(this.operation.toString())
                .append(this.source.getType().mayBeNull ? "? " : " ")
                .append(this.source)
                .append(")");
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPUnaryExpression(this.getNode(), this.getType(), this.operation, this.source.deepCopy());
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPUnaryExpression otherExpression = other.as(DBSPUnaryExpression.class);
        if (otherExpression == null)
            return false;
        return this.operation == otherExpression.operation &&
                context.equivalent(this.source, otherExpression.source);
    }
}
