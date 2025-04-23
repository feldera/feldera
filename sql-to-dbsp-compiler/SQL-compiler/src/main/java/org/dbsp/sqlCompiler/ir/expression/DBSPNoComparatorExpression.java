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
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPComparatorType;
import org.dbsp.util.IIndentStream;

/** A comparator that does not compare any fields. */
public final class DBSPNoComparatorExpression extends DBSPComparatorExpression {
    public final DBSPType tupleType;

    public DBSPNoComparatorExpression(CalciteObject node, DBSPType tupleType) {
        super(node, DBSPComparatorType.generateType(tupleType));
        this.tupleType = tupleType;
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPNoComparatorExpression(this.getNode(), this.tupleType);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        if (!other.is(DBSPNoComparatorExpression.class))
            return false;
        DBSPNoComparatorExpression otherComparator = other.to(DBSPNoComparatorExpression.class);
        return this.tupleType.sameType(otherComparator.tupleType);
    }

    @Override
    public DBSPType comparedValueType() {
        return this.tupleType;
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPNoComparatorExpression o = other.as(DBSPNoComparatorExpression.class);
        if (o == null)
            return false;
        return this.tupleType == o.tupleType;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("tupleType");
        this.tupleType.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("compare(")
                .append(this.tupleType)
                .append(")");
    }

    @SuppressWarnings("unused")
    public static DBSPNoComparatorExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPType tupleType = fromJsonInner(node, "tupleType", decoder, DBSPType.class);
        return new DBSPNoComparatorExpression(CalciteObject.EMPTY, tupleType);
    }
}
