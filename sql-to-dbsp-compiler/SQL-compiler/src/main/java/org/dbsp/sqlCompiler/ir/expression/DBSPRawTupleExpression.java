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
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.List;

/** A Raw tuple expression generates a raw Rust tuple, e.g., (1, 's', a+b). */
public final class DBSPRawTupleExpression extends DBSPBaseTupleExpression {
    public DBSPRawTupleExpression(DBSPExpression... expressions) {
        super(CalciteObject.EMPTY,
                new DBSPTypeRawTuple(Linq.map(expressions, DBSPExpression::getType, DBSPType.class)), expressions);
    }

    public <T extends DBSPExpression> DBSPRawTupleExpression(List<T> fields) {
        this(fields.toArray(new DBSPExpression[0]));
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        this.type.accept(visitor);
        for (DBSPExpression expression: this.fields)
            expression.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPRawTupleExpression o = other.as(DBSPRawTupleExpression.class);
        if (o == null)
            return false;
        return Linq.same(this.fields, o.fields) &&
                this.hasSameType(o);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("(")
                .intercalateI(", ", this.fields)
                .append(")");
    }

    // In general, we don't want to compare expressions for equality.
    // This function is only used for testing, when constant tuples
    // are compared for equality to validate the test results.
    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPRawTupleExpression that = (DBSPRawTupleExpression) o;
        return this.sameFields(that);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPRawTupleExpression(
                Linq.map(this.fields, DBSPExpression::deepCopy, DBSPExpression.class));
    }

    @Override
    public DBSPBaseTupleExpression fromFields(List<DBSPExpression> fields) {
        return new DBSPRawTupleExpression(fields);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPRawTupleExpression otherExpression = other.as(DBSPRawTupleExpression.class);
        if (otherExpression == null)
            return false;
        return context.equivalent(this.fields, otherExpression.fields);
    }
}
