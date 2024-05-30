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

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public final class DBSPTupleExpression extends DBSPBaseTupleExpression {
    public final boolean isNull;

    public DBSPTupleExpression(CalciteObject object, boolean mayBeNull, DBSPExpression... expressions) {
        super(object,
                new DBSPTypeTuple(object, mayBeNull, Linq.map(expressions, DBSPExpression::getType, DBSPType.class)),
                expressions);
        this.isNull = false;
    }

    /** A tuple with value 'null'. */
    public DBSPTupleExpression(DBSPTypeTuple type) {
        super(type.getNode(), type);
        this.isNull = true;
    }

    public DBSPTupleExpression(DBSPExpression... expressions) {
        this(CalciteObject.EMPTY, false, expressions);
    }

    public DBSPTupleExpression(List<DBSPExpression> fields, boolean mayBeNull) {
        this(CalciteObject.EMPTY, mayBeNull, fields.toArray(new DBSPExpression[0]));
    }

    public DBSPTupleExpression(CalciteObject node, List<DBSPExpression> fields) {
        this(node, false, fields.toArray(new DBSPExpression[0]));
    }

    /**
     * @param expressions A list of expressions with tuple types.
     * @return  A tuple expressions that concatenates all fields of these tuple expressions.
     */
    public static DBSPTupleExpression flatten(DBSPExpression... expressions) {
        List<DBSPExpression> fields = new ArrayList<>();
        for (DBSPExpression expression: expressions) {
            DBSPTypeTuple type = expression.getType().to(DBSPTypeTuple.class);
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

    public DBSPTupleExpression append(DBSPExpression expression) {
        List<DBSPExpression> fields = Linq.list(this.fields);
        fields.add(expression);
        return new DBSPTupleExpression(this.getNode(), fields);
    }

    /** Cast each element of the tuple to the corresponding type in the destination tuple. */
    public DBSPTupleExpression pointwiseCast(DBSPTypeTuple destType) {
        if (this.size() != destType.size())
            throw new InternalCompilerError("Cannot cast " + this + " with " + this.size() + " fields "
                    + " to " + destType + " with " + destType.size() + " fields", this);
        return new DBSPTupleExpression(
                Linq.zip(this.fields, destType.tupFields,
                        DBSPExpression::cast, DBSPExpression.class));
    }

    public DBSPTupleExpression slice(int start, int endExclusive) {
        if (endExclusive <= start)
            throw new InternalCompilerError("Incorrect slice parameters " + start + ":" + endExclusive, this);
        return new DBSPTupleExpression(Utilities.arraySlice(this.fields, start, endExclusive));
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        this.type.accept(visitor);
        for (DBSPExpression expression: this.fields)
            expression.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPTupleExpression o = other.as(DBSPTupleExpression.class);
        if (o == null)
            return false;
        if (!this.hasSameType(o))
            return false;
        for (int index = 0; index < this.size(); index++) {
            DBSPExpression field = this.get(index);
            DBSPExpression oField = o.get(index);
            if (!field.equals(oField))
                return false;
        }
        return true;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(DBSPTypeCode.TUPLE.rustName)
                .append(this.fields.length)
                .append("::new(")
                .intercalateI(", ", this.fields)
                .append(")");
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
        if (this.isNull)
            return new DBSPTupleExpression(this.getType().to(DBSPTypeTuple.class));
        return new DBSPTupleExpression(this.getNode(), this.getType().mayBeNull,
                Linq.map(this.fields, DBSPExpression::deepCopy, DBSPExpression.class));
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPTupleExpression otherExpression = other.as(DBSPTupleExpression.class);
        if (otherExpression == null)
            return false;
        return this.isNull == otherExpression.isNull &&
                context.equivalent(this.fields, otherExpression.fields);
    }

    @Override
    public DBSPBaseTupleExpression fromFields(List<DBSPExpression> fields) {
        return new DBSPTupleExpression(this.getNode(), fields);
    }
}
