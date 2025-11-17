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
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

/** Tuple field reference expression. */
public final class DBSPFieldExpression extends DBSPExpression {
    public final DBSPExpression expression;
    public final int fieldNo;

    DBSPFieldExpression(CalciteObject node, DBSPExpression expression, int fieldNo, DBSPType type) {
        super(node, type);
        this.expression = expression;
        this.fieldNo = fieldNo;
        Utilities.enforce(fieldNo >= 0, () -> "Negative field index " + fieldNo);
        Utilities.enforce(!expression.getType().mayBeNull || type.mayBeNull,
                () -> "Non-nullable field from nullable struct");
    }

    DBSPFieldExpression(DBSPExpression expression, int fieldNo, DBSPType type) {
        this(CalciteObject.EMPTY, expression, fieldNo, type);
    }

    static DBSPType getFieldType(DBSPType type, int fieldNo) {
        if (type.is(DBSPTypeAny.class))
            return type;
        DBSPTypeTupleBase tuple = type.to(DBSPTypeTupleBase.class);
        DBSPType fieldType = tuple.getFieldType(fieldNo);
        // A Field access in a nullable struct is nullable
        if (type.mayBeNull)
            fieldType = fieldType.withMayBeNull(true);
        return fieldType;
    }

    public DBSPFieldExpression(CalciteObject node, DBSPExpression expression, int fieldNo) {
        this(node, expression, fieldNo, getFieldType(expression.getType(), fieldNo));
    }

    DBSPFieldExpression(DBSPExpression expression, int fieldNo) {
        this(CalciteObject.EMPTY, expression, fieldNo);
    }

    public DBSPExpression simplify() {
        if (this.expression.is(DBSPBaseTupleExpression.class)) {
            return this.expression.to(DBSPBaseTupleExpression.class).get(this.fieldNo);
        }
        return this;
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
        DBSPFieldExpression o = other.as(DBSPFieldExpression.class);
        if (o == null)
            return false;
        return this.expression == o.expression &&
                this.fieldNo == o.fieldNo;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("(")
                .append(this.expression)
                .append(".")
                .append(this.fieldNo)
                .append(")");
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPFieldExpression(this.getNode(), this.expression.deepCopy(), this.fieldNo);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPFieldExpression otherExpression = other.as(DBSPFieldExpression.class);
        if (otherExpression == null)
            return false;
        return this.fieldNo == otherExpression.fieldNo &&
                context.equivalent(this.expression, otherExpression.expression);
    }

    @SuppressWarnings("unused")
    public static DBSPFieldExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPExpression expression = fromJsonInner(node, "expression", decoder, DBSPExpression.class);
        int fieldNo = Utilities.getIntProperty(node, "fieldNo");
        return new DBSPFieldExpression(CalciteObject.EMPTY, expression, fieldNo);
    }
}
