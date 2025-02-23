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

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

/** A comparator that looks at the field of a tuple.
 * A comparator takes a field of a tuple and compares tuples on the specified field.
 * It also takes a direction, indicating whether the sort is ascending or descending. */
public final class DBSPFieldComparatorExpression extends DBSPComparatorExpression {
    public final DBSPComparatorExpression source;
    public final boolean ascending;
    public final int fieldNo;

    public DBSPFieldComparatorExpression(CalciteObject node, DBSPComparatorExpression source, int fieldNo, boolean ascending) {
        super(node);
        this.source = source;
        this.fieldNo = fieldNo;
        this.ascending = ascending;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("source");
        this.source.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    public DBSPType comparedValueType() {
        return this.source.comparedValueType();
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPFieldComparatorExpression o = other.as(DBSPFieldComparatorExpression.class);
        if (o == null)
            return false;
        return this.source == o.source &&
                this.ascending == o.ascending &&
                this.fieldNo == o.fieldNo &&
                this.hasSameType(o);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.source)
                .append(".then_")
                .append(this.ascending ? "asc" : "desc")
                .append("(|t| t.")
                .append(this.fieldNo)
                .append(")");
    }

    @Override
    public DBSPExpression deepCopy() {
        return this.source.deepCopy().to(DBSPComparatorExpression.class)
                .field(this.fieldNo, this.ascending);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPFieldComparatorExpression otherExpression = other.as(DBSPFieldComparatorExpression.class);
        if (otherExpression == null)
            return false;
        return this.ascending == otherExpression.ascending &&
                this.fieldNo == otherExpression.fieldNo &&
                this.source.equivalent(context, otherExpression.source);
    }

    @SuppressWarnings("unused")
    public static DBSPFieldComparatorExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPComparatorExpression source = fromJsonInner(node, "source", decoder, DBSPComparatorExpression.class);
        int fieldNo = Utilities.getIntProperty(node, "fieldNo");
        boolean ascending = Utilities.getBooleanProperty(node, "ascending");
        return new DBSPFieldComparatorExpression(CalciteObject.EMPTY, source, fieldNo, ascending);
    }
}
