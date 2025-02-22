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

import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.util.IIndentStream;

/** An expression of the form &expression.
 * Should not appear in the generated code from SQL, only in the
 * tools for test generation. */
@NonCoreIR
public final class DBSPBorrowExpression extends DBSPExpression {
    public final DBSPExpression expression;
    public final boolean mut;

    DBSPBorrowExpression(DBSPExpression expression, boolean mutable) {
        super(expression.getNode(), expression.getType().ref());
        this.expression = expression;
        this.mut = mutable;
    }

    DBSPBorrowExpression(DBSPExpression expression) {
        this(expression, false);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("expression");
        this.expression.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPBorrowExpression o = other.as(DBSPBorrowExpression.class);
        if (o == null)
            return false;
        return this.expression == o.expression &&
                this.mut == o.mut &&
                this.hasSameType(o);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("&")
                .append(this.mut ? "mut " : "")
                .append(this.expression);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPBorrowExpression(this.expression.deepCopy(), this.mut);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPBorrowExpression otherExpression = other.as(DBSPBorrowExpression.class);
        if (otherExpression == null)
            return false;
        return this.mut == otherExpression.mut &&
                context.equivalent(this.expression, otherExpression.expression);
    }
}
