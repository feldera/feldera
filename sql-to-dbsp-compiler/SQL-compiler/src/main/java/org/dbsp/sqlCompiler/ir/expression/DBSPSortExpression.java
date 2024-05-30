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
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeVec;
import org.dbsp.util.IIndentStream;

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

    public DBSPSortExpression(CalciteObject node, DBSPType elementType, DBSPComparatorExpression comparator) {
        super(node, new DBSPTypeFunction(
                // Return type
                new DBSPTypeVec(elementType, false),
                // Argument type
                new DBSPTypeRawTuple(
                        new DBSPTypeRawTuple().ref(),
                        new DBSPTypeVec(elementType, false).ref())));
        this.comparator = comparator;
        this.elementType = elementType;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        this.comparator.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }


    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPSortExpression o = other.as(DBSPSortExpression.class);
        if (o == null)
            return false;
        return this.comparator == o.comparator &&
                this.elementType == o.elementType;
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
                this.comparator.deepCopy().to(DBSPComparatorExpression.class));
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPSortExpression otherExpression = other.as(DBSPSortExpression.class);
        if (otherExpression == null)
            return false;
        return this.comparator.equivalent(context, otherExpression.comparator);
    }
}
