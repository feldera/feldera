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
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeWithCustomOrd;
import org.dbsp.util.IIndentStream;

/** An expression of the form '(*expression).0.field' or
 * Some((*expression).0.field), where
 * - expression has a type of the form Option[&WithCustomOrd[T, S]]
 * The result type is always nullable */
public final class DBSPCustomOrdField extends DBSPExpression {
    public final DBSPExpression expression;
    public final int field;

    private static DBSPType getFieldType(DBSPType sourceType, int field) {
        return sourceType.deref()
                .to(DBSPTypeWithCustomOrd.class)
                .getDataType()
                .to(DBSPTypeTupleBase.class)
                .tupFields[field];
    }

    /** True if the original source field is not nullable, and thus the
     * final result needs to be wrapped in a Some(). */
    public boolean needsSome() {
        return !this.getFieldType().mayBeNull;
    }

    public DBSPCustomOrdField(DBSPExpression expression, int field) {
        super(expression.getNode(), getFieldType(expression.getType(), field).setMayBeNull(true));
        assert expression.getType().mayBeNull;
        this.expression = expression;
        this.field = field;
    }

    public DBSPType getFieldType() {
        return getFieldType(this.expression.getType(), this.field);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        this.type.accept(visitor);
        this.expression.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPCustomOrdField o = other.as(DBSPCustomOrdField.class);
        if (o == null)
            return false;
        return this.expression == o.expression &&
                this.field == o.field &&
                this.hasSameType(o);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("(*")
                .append(this.expression)
                .append(").get().")
                .append(this.field);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPCustomOrdField(this.expression.deepCopy(), this.field);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPCustomOrdField otherExpression = other.as(DBSPCustomOrdField.class);
        if (otherExpression == null)
            return false;
        return context.equivalent(this.expression, otherExpression.expression) &&
                this.field == otherExpression.field;
    }
}
