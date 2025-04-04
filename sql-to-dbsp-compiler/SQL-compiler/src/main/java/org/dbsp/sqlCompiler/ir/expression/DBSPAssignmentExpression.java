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

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.util.IIndentStream;

public final class DBSPAssignmentExpression extends DBSPExpression {
    public final DBSPExpression left;
    public final DBSPExpression right;

    public DBSPAssignmentExpression(DBSPExpression left, DBSPExpression right) {
        super(left.getNode(), DBSPTypeVoid.INSTANCE);
        this.left = left;
        this.right = right;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("left");
        this.left.accept(visitor);
        visitor.property("right");
        this.right.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPAssignmentExpression o = other.as(DBSPAssignmentExpression.class);
        if (o == null)
            return false;
        return this.left == o.left &&
                this.right == o.right;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.left)
                .append(" = ")
                .append(this.right);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPAssignmentExpression(this.left.deepCopy(), this.right.deepCopy());
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPAssignmentExpression otherExpression = other.as(DBSPAssignmentExpression.class);
        if (otherExpression == null)
            return false;
        return context.equivalent(this.left, otherExpression.left) &&
                context.equivalent(this.right, otherExpression.right);
    }

    @SuppressWarnings("unused")
    public static DBSPAssignmentExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPExpression left = fromJsonInner(node, "left", decoder, DBSPExpression.class);
        DBSPExpression right = fromJsonInner(node, "right", decoder, DBSPExpression.class);
        return new DBSPAssignmentExpression(left, right);
    }
}