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

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;

/**
 * Represents a closure that sorts an IndexedZSet with empty keys and
 * a Vector of tuples as a value.
 * Sorting is done using a comparator.
 * The sort expression represents a closure that sorts the vector.
 * E.g., in Rust the closure has the following signature:
 * move |(k, v): (&(), &Vec<Tup<...>>)| -> Vec<Tup<...>>
 */
public final class DBSPSortExpression extends DBSPExpression {
    public final DBSPComparatorExpression comparator;
    public final DBSPType elementType;
    @Nullable
    public final DBSPExpression limit;

    public DBSPSortExpression(
            CalciteObject node, DBSPType elementType,
            DBSPComparatorExpression comparator, @Nullable DBSPExpression limit) {
        super(node, new DBSPTypeFunction(
                // Return type
                new DBSPTypeArray(elementType, false),
                // Argument type
                new DBSPTypeRawTuple(
                        new DBSPTypeRawTuple().ref(),
                        new DBSPTypeArray(elementType, false).ref())));
        this.comparator = comparator;
        this.elementType = elementType;
        this.limit = limit;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("comparator");
        this.comparator.accept(visitor);
        if (this.limit != null) {
            visitor.property("limit");
            this.limit.accept(visitor);
        }
        visitor.property("elementType");
        this.elementType.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }


    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPSortExpression o = other.as(DBSPSortExpression.class);
        if (o == null)
            return false;
        return this.comparator == o.comparator &&
                this.elementType == o.elementType &&
                this.limit == o.limit;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("(|k, v| k.")
                .append(this.comparator)
                .append(")");
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPSortExpression(this.getNode(), this.elementType,
                this.comparator.deepCopy().to(DBSPComparatorExpression.class), this.limit);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPSortExpression otherExpression = other.as(DBSPSortExpression.class);
        if (otherExpression == null)
            return false;
        return this.comparator.equivalent(context, otherExpression.comparator) &&
                EquivalenceContext.equiv(this.limit, otherExpression.limit);
    }
}
