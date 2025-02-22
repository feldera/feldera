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
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.util.IIndentStream;

@NonCoreIR
public final class DBSPForExpression extends DBSPExpression implements IDBSPDeclaration {
    public final String variable;
    public final DBSPExpression iterated;
    public final DBSPBlockExpression block;

    public DBSPForExpression(String variable, DBSPExpression iterated, DBSPBlockExpression block) {
        super(iterated.getNode(), new DBSPTypeRawTuple());
        this.variable = variable;
        this.iterated = iterated;
        this.block = block;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("iterated");
        this.iterated.accept(visitor);
        visitor.property("block");
        this.block.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPForExpression o = other.as(DBSPForExpression.class);
        if (o == null)
            return false;
        return this.variable.equals(o.variable) &&
                this.iterated == o.iterated &&
                this.block == o.block &&
                this.hasSameType(o);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("for ")
                .append(this.variable)
                .append(" in ")
                .append(this.iterated)
                .append(" ")
                .append(this.block);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPForExpression(this.variable, this.iterated.deepCopy(),
                this.block.deepCopy().to(DBSPBlockExpression.class));
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPForExpression otherExpression = other.as(DBSPForExpression.class);
        if (otherExpression == null)
            return false;
        if (!context.equivalent(this.iterated, otherExpression.iterated))
            return false;
        EquivalenceContext newContext = context.clone();
        newContext.leftDeclaration.substitute(this.variable, this);
        newContext.rightDeclaration.substitute(this.variable, otherExpression);
        newContext.leftToRight.put(this, otherExpression);
        return newContext.equivalent(this.block, otherExpression.block);
    }

    @Override
    public String getName() {
        return this.variable;
    }
}
