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
import org.dbsp.sqlCompiler.compiler.IConstructor;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.ISameValue;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPNullLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Represents an ARRAY described by its elements. */
public final class DBSPArrayExpression extends DBSPExpression
        implements IDBSPContainer, ISameValue, IConstructor {
    @Nullable
    public final List<DBSPExpression> data;
    public final DBSPTypeArray arrayType;

    public static DBSPArrayExpression emptyWithElementType(DBSPType elementType, boolean mayBeNull) {
        return new DBSPArrayExpression(CalciteObject.EMPTY, new DBSPTypeArray(elementType, mayBeNull), Linq.list());
    }

    public DBSPArrayExpression(DBSPTypeArray arrayType, boolean isNull) {
        super(CalciteObject.EMPTY, arrayType);
        this.data = isNull ? null : new ArrayList<>();
        this.arrayType = arrayType;
    }

    public DBSPArrayExpression(CalciteObject node, DBSPType type, @Nullable List<DBSPExpression> data) {
        super(node, type);
        this.arrayType = this.getType().to(DBSPTypeArray.class);
        if (data != null) {
            this.data = new ArrayList<>();
            for (DBSPExpression e : data) {
                if (!e.getType().sameType(data.get(0).getType()))
                    throw new InternalCompilerError("Not all values of vector have the same type:" +
                            e.getType() + " vs " + data.get(0).getType(), this);
                this.append(e);
            }
        } else {
            this.data = null;
        }
    }

    public DBSPArrayExpression(boolean mayBeNull, DBSPExpression... data) {
        super(CalciteObject.EMPTY, new DBSPTypeArray(data[0].getType(), mayBeNull));
        this.arrayType = this.getType().to(DBSPTypeArray.class);
        this.data = new ArrayList<>();
        for (DBSPExpression e: data) {
            if (!e.getType().sameType(data[0].getType()))
                throw new InternalCompilerError("Not all values of vector have the same type:" +
                        e.getType() + " vs " + data[0].getType(), this);
            this.append(e);
        }
    }

    public DBSPArrayExpression(DBSPExpression... data) {
       this(false, data);
    }

    public DBSPType getElementType() {
        return this.arrayType.getTypeArg(0);
    }

    public void append(DBSPExpression expression) {
        // We expect the expression to be a constant value (a literal)
        if (!expression.getType().sameType(this.getElementType()))
            throw new InternalCompilerError("Added element " + expression + " type " +
                    expression.getType() + " does not match vector type " + this.getElementType(), this);
        Objects.requireNonNull(this.data).add(expression);
    }

    public void append(DBSPArrayExpression other) {
        if (!this.getType().sameType(other.getType()))
            throw new InternalCompilerError("Added vectors do not have the same type " +
                    this.getElementType() + " vs " + other.getElementType(), this);
        Objects.requireNonNull(other.data).forEach(this::append);
    }

    public int size() {
        return Objects.requireNonNull(this.data).size();
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("type");
        this.type.accept(visitor);
        if (this.data != null) {
            visitor.startArrayProperty("data");
            int index = 0;
            for (DBSPExpression expr : this.data) {
                visitor.propertyIndex(index);
                index++;
                expr.accept(visitor);
            }
            visitor.endArrayProperty("data");
        }
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPArrayExpression otherVec = other.as(DBSPArrayExpression.class);
        if (otherVec == null)
            return false;
        if (this.data == null)
            return otherVec.data == null;
        if (otherVec.data == null)
            return false;
        if (this.data.size() != otherVec.data.size())
            return false;
        for (int i = 0; i < this.size(); i++)
            if (this.data.get(i) != otherVec.data.get(i))
                return false;
        return true;
    }

    @Override
    public boolean sameValue(@Nullable ISameValue o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPArrayExpression that = (DBSPArrayExpression) o;
        if (!Objects.equals(data, that.data)) return false;
        return arrayType.equals(that.arrayType);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        if (this.data == null)
            return builder.append("(")
                    .append(this.type)
                    .append(")")
                    .append("null");
        builder.append("Arc::new(vec!(");
        if (this.data.size() > 1)
            builder
                .increase();
        builder.intercalateI(System.lineSeparator(), this.data)
                .append("))");
        if (this.data.size() > 1)
                builder.decrease();
        return builder;
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPArrayExpression(this.getNode(), this.getType(),
                this.data != null ? Linq.map(this.data, DBSPExpression::deepCopy) : null);
    }

    public boolean isConstant() {
        return this.data == null || Linq.all(this.data, DBSPExpression::isCompileTimeConstant);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPArrayExpression otherExpression = other.as(DBSPArrayExpression.class);
        if (otherExpression == null)
            return false;
        return context.equivalent(this.data, otherExpression.data);
    }

    public String toSqlString() {
        if (this.data == null)
            return DBSPNullLiteral.NULL;
        StringBuilder builder = new StringBuilder();
        builder.append("ARRAY[");
        boolean first = true;
        for (DBSPExpression d: this.data) {
            if (!first)
                builder.append(", ");
            first = false;
            if (d.is(DBSPLiteral.class)) {
                builder.append(d.to(DBSPLiteral.class).toSqlString());
            } else {
                builder.append(d);
            }
        }
        builder.append("]");
        return builder.toString();
    }

    @SuppressWarnings("unused")
    public static DBSPArrayExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPType type = DBSPNode.fromJsonInner(node, "type", decoder, DBSPType.class);
        List<DBSPExpression> data = null;
        if (node.has("data"))
            data = DBSPNode.fromJsonInnerList(node, "data", decoder, DBSPExpression.class);
        return new DBSPArrayExpression(CalciteObject.EMPTY, type, data);
    }
}
