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
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;

public final class DBSPIfExpression extends DBSPExpression {
    public final DBSPExpression condition;
    public final DBSPExpression positive;
    @Nullable
    public final DBSPExpression negative;

    public DBSPIfExpression(CalciteObject node, DBSPExpression condition,
                            DBSPExpression positive, @Nullable DBSPExpression negative) {
        super(node, positive.getType());
        this.condition = condition;
        this.positive = positive;
        this.negative = negative;
        if (!this.condition.getType().is(DBSPTypeBool.class))
            throw new InternalCompilerError("Expected a boolean condition type " + condition + " got " +
                    this.condition.getType(), this);
        if (this.condition.getType().mayBeNull)
            throw new InternalCompilerError("Nullable condition in if expression", condition);
        if (this.negative != null && !this.positive.getType().sameType(this.negative.getType()))
            throw new InternalCompilerError("Mismatched types in conditional expression " + this.positive +
                    "/" + this.positive.getType() + " vs " + this.negative + "/" + this.negative.getType(), this);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("type");
        this.type.accept(visitor);
        visitor.property("condition");
        this.condition.accept(visitor);
        visitor.property("positive");
        this.positive.accept(visitor);
        if (this.negative != null) {
            visitor.property("negative");
            this.negative.accept(visitor);
        }
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPIfExpression o = other.as(DBSPIfExpression.class);
        if (o == null)
            return false;
        return this.condition == o.condition &&
                this.positive == o.positive &&
                this.negative == o.negative;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append("if ")
                .append(this.condition)
                .append(" ");
        if (this.positive.is(DBSPBlockExpression.class))
            builder.append(this.positive);
        else {
            builder.append("{")
                    .increase()
                    .append(this.positive)
                    .newline()
                    .decrease()
                    .append("}");
        }
        if (this.negative != null) {
            builder.append(" else ");
            if (this.negative.is(DBSPBlockExpression.class))
                builder.append(this.negative);
            else {
                builder.append("{")
                        .increase()
                        .append(this.negative)
                        .newline()
                        .decrease()
                        .append("}");
            }
        }
        return builder;
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPIfExpression(this.getNode(), this.condition.deepCopy(),
                this.positive.deepCopy(),
                this.negative != null ? this.negative.deepCopy() : null);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPIfExpression otherExpression = other.as(DBSPIfExpression.class);
        if (otherExpression == null)
            return false;
        return context.equivalent(this.condition, otherExpression.condition) &&
                context.equivalent(this.positive, otherExpression.positive) &&
                context.equivalent(this.negative, otherExpression.negative);
    }

    @SuppressWarnings("unused")
    public static DBSPIfExpression fromJson(JsonNode node, JsonDecoder decoder) {
        getJsonType(node, decoder);
        DBSPExpression condition = fromJsonInner(node, "condition", decoder, DBSPExpression.class);
        DBSPExpression positive = fromJsonInner(node, "positive", decoder, DBSPExpression.class);
        DBSPExpression negative = null;
        if (node.has("negative"))
            negative = fromJsonInner(node, "negative", decoder, DBSPExpression.class);
        return new DBSPIfExpression(CalciteObject.EMPTY, condition, positive, negative);
    }
}
