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
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;

public final class DBSPBinaryExpression extends DBSPExpression {
    public final DBSPExpression left;
    public final DBSPExpression right;
    public final DBSPOpcode opcode;

    public DBSPBinaryExpression(CalciteObject node, DBSPType type, DBSPOpcode opcode,
                                DBSPExpression left, DBSPExpression right) {
        super(node, type);
        this.opcode = opcode;
        this.left = left;
        this.right = right;
    }

    public DBSPBinaryExpression replaceSources(DBSPExpression left, DBSPExpression right) {
        assert this.left.getType().sameType(left.getType());
        assert this.right.getType().sameType(right.getType());
        return new DBSPBinaryExpression(this.getNode(), this.type, this.opcode, left, right);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("type");
        this.type.accept(visitor);
        visitor.property("left");
        this.left.accept(visitor);
        visitor.property("right");
        this.right.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPBinaryExpression o = other.as(DBSPBinaryExpression.class);
        if (o == null)
            return false;
        return this.left == o.left &&
                this.right == o.right &&
                this.opcode == o.opcode;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder
                .append("(")
                .append(this.left)
                .append(" ")
                .append(this.left.getType().mayBeNull ? "?" : "")
                .append(this.opcode.toString())
                .append(this.right.getType().mayBeNull ? "?" : "")
                .append(" ")
                .append(this.right)
                .append(")");
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPBinaryExpression(this.getNode(), this.getType(), this.opcode,
                this.left.deepCopy(), this.right.deepCopy());
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPBinaryExpression otherExpression = other.as(DBSPBinaryExpression.class);
        if (otherExpression == null)
            return false;
        return this.opcode == otherExpression.opcode &&
                context.equivalent(this.left, otherExpression.left) &&
                context.equivalent(this.right, otherExpression.right);
    }

    @SuppressWarnings("unused")
    public static DBSPBinaryExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPType type = getJsonType(node, decoder);
        DBSPExpression left = fromJsonInner(node, "left", decoder, DBSPExpression.class);
        DBSPExpression right = fromJsonInner(node, "right", decoder, DBSPExpression.class);
        DBSPOpcode code = DBSPOpcode.fromJson(node);
        return new DBSPBinaryExpression(CalciteObject.EMPTY, type, code, left, right);
    }
}
