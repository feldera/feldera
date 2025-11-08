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
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

/** This class does not correspond to any Rust primitive construct.
 * It is compiled into a function invocation, depending on the involved types.
 * It represents a cast of an expression to a given type. */
public final class DBSPCastExpression extends DBSPExpression {
    public final DBSPExpression source;
    /** If true this is a safe cast */
    public final boolean safe;

    public DBSPCastExpression(CalciteObject node, DBSPExpression source, DBSPType to, boolean safe) {
        super(node, to);
        this.source = source;
        this.safe = safe;
        Utilities.enforce(source.getType().is(DBSPTypeTupleBase.class) == to.is(DBSPTypeTupleBase.class)
                || source.getType().is(DBSPTypeVariant.class) || to.is(DBSPTypeVariant.class),
                () -> "Cast to/from tuple from/to non-tuple " + source.getType() + " to " + to);
        Utilities.enforce(to.code != DBSPTypeCode.INTERNED_STRING, () -> "Cast to INTERNED");
        Utilities.enforce(source.getType().code != DBSPTypeCode.INTERNED_STRING, () -> "Cast from INTERNED");
        Utilities.enforce(!safe || to.mayBeNull);
    }

    public DBSPCastExpression replaceSource(DBSPExpression source) {
        Utilities.enforce(source.getType().sameType(this.source.getType()));
        return new DBSPCastExpression(this.getNode(), source, this.type, this.safe);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("source");
        this.source.accept(visitor);
        visitor.property("type");
        this.type.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPCastExpression o = other.as(DBSPCastExpression.class);
        if (o == null)
            return false;
        return this.source == o.source &&
                this.safe == o.safe &&
                this.hasSameType(o);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("((")
                .append(this.type)
                .append(")")
                .append(this.source)
                .append(")");
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPCastExpression(this.getNode(), this.source.deepCopy(), this.getType(), this.safe);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPCastExpression otherExpression = other.as(DBSPCastExpression.class);
        if (otherExpression == null)
            return false;
        return context.equivalent(this.source, otherExpression.source) &&
                this.safe == otherExpression.safe &&
                this.hasSameType(other);
    }

    @SuppressWarnings("unused")
    public static DBSPCastExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPExpression source = fromJsonInner(node, "source", decoder, DBSPExpression.class);
        boolean safe = Utilities.getBooleanProperty(node, "safe");
        DBSPType type = getJsonType(node, decoder);
        return new DBSPCastExpression(CalciteObject.EMPTY, source, type, safe);
    }
}
