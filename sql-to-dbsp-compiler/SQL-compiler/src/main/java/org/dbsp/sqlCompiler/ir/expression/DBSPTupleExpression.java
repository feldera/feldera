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
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class DBSPTupleExpression extends DBSPBaseTupleExpression {
    public DBSPTupleExpression(CalciteObject object, DBSPTypeTuple type, DBSPExpression... expressions) {
        super(object, type, expressions);
        Utilities.enforce(type.size() == expressions.length, "Tuple expression has size " + expressions.length +
                " but the declared type is " + type);
        for (int i = 0; i < type.size(); i++) {
            Utilities.enforce(type.tupFields[i].sameType(expressions[i].getType()),
                    "Tuple field " + expressions[i] + " has type " + expressions[i].getType() +
                    " that does not match the declared field type " + type.tupFields[i]);
        }
    }

    public DBSPTupleExpression(CalciteObject object, DBSPTypeTuple type, List<DBSPExpression> expressions) {
        this(object, type, expressions.toArray(new DBSPExpression[0]));
    }

    static DBSPTypeTuple createType(boolean mayBeNull, DBSPExpression... expressions) {
        return new DBSPTypeTuple(CalciteObject.EMPTY, mayBeNull, null,
                Linq.map(expressions, DBSPExpression::getType, DBSPType.class));
    }

    static DBSPTypeTuple createType(boolean mayBeNull, List<DBSPExpression> expressions) {
        return new DBSPTypeTuple(CalciteObject.EMPTY, mayBeNull, null,
                Linq.map(expressions, DBSPExpression::getType).toArray(new DBSPType[0]));
    }

    /** A DBSPTupleExpression with value null */
    private DBSPTupleExpression(DBSPTypeTuple type) {
        super(type.getNode(), type);
    }

    /** A DBSPTupleExpression with value null */
    public static DBSPTupleExpression none(DBSPTypeTuple type) {
        Utilities.enforce(type.mayBeNull);
        return new DBSPTupleExpression(type);
    }

    public DBSPTupleExpression(DBSPExpression... expressions) {
        this(CalciteObject.EMPTY, createType(false, expressions), expressions);
    }

    public DBSPTupleExpression(boolean mayBeNull, DBSPExpression... expressions) {
        this(CalciteObject.EMPTY, createType(mayBeNull, expressions), expressions);
    }

    public DBSPTupleExpression(List<DBSPExpression> fields, boolean mayBeNull) {
        this(CalciteObject.EMPTY, createType(mayBeNull, fields), fields.toArray(new DBSPExpression[0]));
    }

    public DBSPTupleExpression(CalciteObject node, List<DBSPExpression> fields) {
        this(node, createType(false, fields), fields.toArray(new DBSPExpression[0]));
    }

    public DBSPTypeTuple getTypeAsTuple() {
        return this.getType().to(DBSPTypeTuple.class);
    }

    /** @param expressions A list of expressions with tuple types.
     * @return  A tuple expressions that concatenates all fields of these tuple expressions. */
    public static DBSPTupleExpression flatten(List<DBSPExpression> expressions) {
        List<DBSPExpression> fields = new ArrayList<>(expressions.size());
        for (DBSPExpression expression: expressions) {
            DBSPTypeTupleBase type = expression.getType().to(DBSPTypeTupleBase.class);
            for (int i = 0; i < type.size(); i++) {
                DBSPType fieldType = type.tupFields[i];
                DBSPExpression field = new DBSPFieldExpression(expression.deepCopy(), i, fieldType)
                        .simplify()
                        .applyCloneIfNeeded();
                fields.add(field);
            }
        }
        return new DBSPTupleExpression(fields, false);
    }

    public static DBSPTupleExpression flatten(DBSPExpression... expressions) {
        return flatten(Linq.list(expressions));
    }

    public DBSPTupleExpression append(DBSPExpression expression) {
        List<DBSPExpression> fields = Linq.list(Objects.requireNonNull(this.fields));
        fields.add(expression);
        return new DBSPTupleExpression(this.getNode(), fields);
    }

    /** Cast each element of the tuple to the corresponding type in the destination tuple. */
    public DBSPTupleExpression pointwiseCast(DBSPTypeTuple destType) {
        if (this.size() != destType.size())
            throw new InternalCompilerError("Cannot cast " + this + " with " + this.size() + " fields "
                    + " to " + destType + " with " + destType.size() + " fields", this);
        return new DBSPTupleExpression(
                Linq.zip(Objects.requireNonNull(this.fields), destType.tupFields,
                        (e, t) -> e.cast(e.getNode(), t, false), DBSPExpression.class));
    }

    public DBSPTupleExpression slice(int start, int endExclusive) {
        if (endExclusive <= start)
            throw new InternalCompilerError("Incorrect slice parameters " + start + ":" + endExclusive, this);
        return new DBSPTupleExpression(Utilities.arraySlice(Objects.requireNonNull(this.fields), start, endExclusive));
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("type");
        this.type.accept(visitor);
        if (this.fields != null) {
            visitor.startArrayProperty("fields");
            int index = 0;
            for (DBSPExpression expression : this.fields) {
                visitor.propertyIndex(index);
                index++;
                expression.accept(visitor);
            }
            visitor.endArrayProperty("fields");
        }
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPTupleExpression o = other.as(DBSPTupleExpression.class);
        if (o == null)
            return false;
        if (this.fields == null)
            return o.fields == null;
        if (o.fields == null)
            return false;
        for (int index = 0; index < this.size(); index++) {
            DBSPExpression field = this.get(index);
            DBSPExpression oField = o.get(index);
            if (field != oField)
                return false;
        }
        return true;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        if (this.fields == null)
            return builder.append("None");
        if (this.getType().mayBeNull)
            builder.append("Some(");
        builder.append(DBSPTypeCode.TUPLE.rustName)
                .append(this.fields.length)
                .append("::new(")
                .intercalateI(", ", this.fields)
                .append(")");
        if (this.getType().mayBeNull)
            builder.append(")");
        return builder;
    }

    // In general, we don't want to compare expressions for equality.
    // This function is only used for testing, when constant tuples
    // are compared for equality to validate the test results.
    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPTupleExpression that = (DBSPTupleExpression) o;
        return this.sameFields(that);
    }

    @Override
    public DBSPExpression deepCopy() {
        if (this.fields == null)
            return this.getType().none();
        return new DBSPTupleExpression(this.getNode(), this.getTypeAsTuple(),
                Linq.map(this.fields, DBSPExpression::deepCopy, DBSPExpression.class));
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPTupleExpression otherExpression = other.as(DBSPTupleExpression.class);
        if (otherExpression == null)
            return false;
        if (!this.getType().sameType(other.getType()))
            return false;
        if (this.fields == null)
            return otherExpression.fields == null;
        if (otherExpression.fields == null)
            return false;
        return context.equivalent(this.fields, otherExpression.fields);
    }

    @Override
    public DBSPBaseTupleExpression fromFields(List<DBSPExpression> fields) {
        return new DBSPTupleExpression(this.getNode(), fields);
    }

    @SuppressWarnings("unused")
    public static DBSPTupleExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPType type = getJsonType(node, decoder);
        List<DBSPExpression> fields = null;
        if (node.has("fields"))
            fields = fromJsonInnerList(node, "fields", decoder, DBSPExpression.class);
        if (fields != null)
            return new DBSPTupleExpression(CalciteObject.EMPTY, type.to(DBSPTypeTuple.class), fields);
        else
            return new DBSPTupleExpression(type.to(DBSPTypeTuple.class));
    }
}
