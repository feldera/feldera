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
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.util.IIndentStream;

/** Given an expression e, this is equivalent with the Rust code
 * e.is_none().  Obviously, e must have a nullable type. */
public final class DBSPIsNullExpression extends DBSPExpression {
    public final DBSPExpression expression;

    public DBSPIsNullExpression(CalciteObject node, DBSPExpression expression) {
        super(node, new DBSPTypeBool(CalciteObject.EMPTY, false));
        this.expression = expression;
        if (!expression.getType().mayBeNull)
            throw new InternalCompilerError("isNull applied to non-nullable expression?", expression);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("expression");
        this.expression.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPIsNullExpression o = other.as(DBSPIsNullExpression.class);
        if (o == null)
            return false;
        return this.expression == o.expression;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.expression)
                .append(".is_none()");
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPIsNullExpression(this.getNode(), this.expression.deepCopy());
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPIsNullExpression otherExpression = other.as(DBSPIsNullExpression.class);
        if (otherExpression == null)
            return false;
        return context.equivalent(this.expression, otherExpression.expression);
    }

    @SuppressWarnings("unused")
    public static DBSPIsNullExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPExpression expression = fromJsonInner(node, "expression", decoder, DBSPExpression.class);
        return new DBSPIsNullExpression(CalciteObject.EMPTY, expression);
    }
}
