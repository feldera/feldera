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
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;

/** Invocation of a Rust constructor with some arguments. */
public final class DBSPConstructorExpression extends DBSPExpression {
    public final DBSPExpression function;
    public final DBSPExpression[] arguments;

    public DBSPConstructorExpression(DBSPExpression function, DBSPType type, DBSPExpression... arguments) {
        super(function.getNode(), type);
        this.function = function;
        this.arguments = arguments;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        this.type.accept(visitor);
        this.function.accept(visitor);
        for (DBSPExpression arg : this.arguments)
            arg.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPConstructorExpression o = other.as(DBSPConstructorExpression.class);
        if (o == null)
            return false;
        return this.function == o.function &&
                Linq.same(this.arguments, o.arguments) &&
                this.hasSameType(o);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.function)
                .append("(")
                .join(", ", this.arguments)
                .append(")");
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPConstructorExpression(
                this.function.deepCopy(), this.type,
                Linq.map(this.arguments, DBSPExpression::deepCopy, DBSPExpression.class));
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPConstructorExpression otherExpression = other.as(DBSPConstructorExpression.class);
        if (otherExpression == null)
            return false;
        return context.equivalent(this.function, otherExpression.function)
                && context.equivalent(this.arguments, otherExpression.arguments);
    }
}
