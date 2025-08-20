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

package org.dbsp.sqlCompiler.ir.type.derived;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class DBSPTypeTupleBase extends DBSPType {
    public final DBSPType[] tupFields;

    protected DBSPTypeTupleBase(CalciteObject node, DBSPTypeCode code, boolean mayBeNull, DBSPType... tupFields) {
        super(node, code, mayBeNull);
        this.tupFields = tupFields;
        for (DBSPType type: this.tupFields)
            if (type == null)
                throw new NullPointerException("null field for tuple type");
    }

    @Nullable @Override
    public String asSqlString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ROW(");
        boolean first = true;
        for (DBSPType field: this.tupFields) {
            if (!first)
                builder.append(", ");
            first = false;
            builder.append(field.asSqlString());
        }
        builder.append(")");
        return builder.toString();
    }

    /** True if this is a Raw tuple (Rust ()),
     * false if it is a TupN tuple. */
    public abstract boolean isRaw();

    /** If the expression has a tuple type, return the list of fields.
     * Else return the expression itself. */
    public static List<DBSPExpression> flatten(DBSPExpression expression) {
        DBSPTypeTupleBase tuple = expression.getType().as(DBSPTypeTupleBase.class);
        if (tuple == null)
            return Linq.list(expression);
        List<DBSPExpression> fields = new ArrayList<>();
        for (int i = 0; i < tuple.size(); i++)
            fields.add(expression.field(i).applyCloneIfNeeded());
        return fields;
    }

    /** The tuple obtained by projecting this tuple on the specified fields.
     * Note that the elements are produced in the order indicated. */
    public abstract DBSPTypeTupleBase project(List<Integer> fields);

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        if (!this.sameNullability(other)) return false;
        DBSPTypeTupleBase type = other.as(DBSPTypeTupleBase.class);
        if (type == null) return false;
        return Linq.same(this.tupFields, type.tupFields);
    }

    public DBSPType getFieldType(int index) {
        if (index >= this.tupFields.length)
            throw new InternalCompilerError("Index " + index + " out of tuple bounds " + this.tupFields.length);
        return this.tupFields[index];
    }

    /** The type of an expression of the form t.i, where t has type this.
     * This is not the same as getFieldType. */
    public DBSPType getFieldExpressionType(int index) {
        DBSPType fieldType = this.getFieldType(index);
        if (this.mayBeNull)
            return fieldType.withMayBeNull(true);
        return fieldType;
    }

    public boolean hasCopy() {
        return false;
    }

    public abstract DBSPExpression makeTuple(DBSPExpression... expressions);

    public int size() {
        return this.tupFields.length;
    }

    @Override
    public DBSPExpression defaultValue() {
        if (this.mayBeNull)
            return this.none();
        DBSPExpression[] fields = Linq.map(this.tupFields, DBSPType::defaultValue, DBSPExpression.class);
        return this.makeTuple(fields);
    }

    /** Make a tuple of the same kind and nullability as this tuple, but with the specified fields */
    public abstract DBSPTypeTupleBase makeRelatedTupleType(List<DBSPType> fields);

    public DBSPTypeTupleBase concat(DBSPTypeTupleBase other) {
        List<DBSPType> fields = new ArrayList<>(this.size() + other.size());
        fields.addAll(Arrays.asList(this.tupFields));
        fields.addAll(Arrays.asList(other.tupFields));
        return this.makeRelatedTupleType(fields);
    }

    /** Generates a closure that computes a binary operation pairwise for two values of this type.
     * The binary operation is expected to preserve the type.
     * e.g., |a, b| (a.0 OP b.0, a.1 OP b.1) */
    public DBSPClosureExpression pairwiseOperation(CalciteObject node, DBSPOpcode code) {
        DBSPVariablePath left = this.ref().var();
        DBSPVariablePath right = this.ref().var();
        List<DBSPExpression> maxes = new ArrayList<>();
        for (int i = 0; i < this.size(); i++) {
            DBSPType ftype = this.tupFields[i];
            maxes.add(ExpressionCompiler.makeBinaryExpression(node, ftype, code,
                    left.deref().field(i), right.deref().field(i)));
        }
        DBSPExpression result = new DBSPTupleExpression(maxes, false);
        return result.closure(left, right);
    }

    public DBSPTypeTupleBase slice(int start, int endExclusive) {
        return this.makeRelatedTupleType(Linq.list(this.tupFields).subList(start, endExclusive));
    }

    /** Creates a ZSet or IndexedZSet, depending on whether this is a Tuple or a RawTuple */
    public DBSPType intoCollectionType() {
        if (this.code == DBSPTypeCode.TUPLE) {
            return new DBSPTypeZSet(this);
        } else {
            return new DBSPTypeIndexedZSet(this.to(DBSPTypeRawTuple.class));
        }
    }
}
