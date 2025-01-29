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
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.statement.DBSPComment;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.List;

public final class DBSPBlockExpression extends DBSPExpression {
    public final List<DBSPStatement> contents;
    @Nullable
    public final DBSPExpression lastExpression;

    public DBSPBlockExpression(List<DBSPStatement> contents, @Nullable DBSPExpression last) {
        super(CalciteObject.EMPTY, last != null ? last.getType() : DBSPTypeVoid.INSTANCE);
        this.contents = contents;
        this.lastExpression = last;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        for (DBSPStatement stat: this.contents)
            stat.accept(visitor);
        if (this.lastExpression != null)
            this.lastExpression.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPBlockExpression o = other.as(DBSPBlockExpression.class);
        if (o == null)
            return false;
        return Linq.same(this.contents, o.contents) &&
                this.lastExpression == o.lastExpression &&
                this.hasSameType(o);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append("{")
                .increase()
                .intercalateI(";" + System.lineSeparator(), this.contents);
        if (this.lastExpression != null) {
            builder.append(this.lastExpression);
        }
        return builder.newline()
                .decrease()
                .append("}");
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPBlockExpression(
                Linq.map(this.contents, DBSPStatement::deepCopy),
                DBSPApplyExpression.nullableDeepCopy(this.lastExpression));
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPBlockExpression otherExpression = other.as(DBSPBlockExpression.class);
        if (otherExpression == null)
            return false;
        // Drop comments
        List<DBSPStatement> contents = Linq.where(this.contents, s -> !s.is(DBSPComment.class));
        List<DBSPStatement> otherContents = Linq.where(otherExpression.contents, s -> !s.is(DBSPComment.class));
        if (contents.size() != otherContents.size())
            return false;
        EquivalenceContext newContext = context.clone();
        newContext.leftDeclaration.newContext();
        newContext.rightDeclaration.newContext();
        for (int i = 0; i < contents.size(); i++) {
            DBSPStatement stat = contents.get(i);
            DBSPStatement otherStat = otherContents.get(i);
            DBSPStatement.EquivalenceResult eq = stat.equivalent(newContext, otherStat);
            if (!eq.equivalent())
                return false;
            newContext = eq.context();
        }
        return newContext.equivalent(this.lastExpression, otherExpression.lastExpression);
    }
}
