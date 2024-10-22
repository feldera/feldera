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
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.util.Linq;

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
                throw new NullPointerException();
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

    public DBSPType getFieldType(int index) {
        if (index >= this.tupFields.length)
            throw new InternalCompilerError("Index " + index + " out of tuple bounds " + this.tupFields.length);
        return this.tupFields[index];
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

    public abstract DBSPTypeTupleBase makeType(List<DBSPType> fields);

    public DBSPTypeTupleBase concat(DBSPTypeTupleBase other) {
        List<DBSPType> fields = new ArrayList<>(this.size() + other.size());
        fields.addAll(Arrays.asList(this.tupFields));
        fields.addAll(Arrays.asList(other.tupFields));
        return this.makeType(fields);
    }

    /** Generates a closure that computes a binary operation pairwise of two tuple timestamps fieldwise */
    public DBSPClosureExpression pairwiseOperation(CalciteObject node, DBSPOpcode code) {
        DBSPVariablePath left = this.ref().var();
        DBSPVariablePath right = this.ref().var();
        List<DBSPExpression> maxes = new ArrayList<>();
        for (int i = 0; i < this.size(); i++) {
            DBSPType ftype = this.tupFields[i];
            maxes.add(ExpressionCompiler.makeBinaryExpression(node, ftype, code,
                    left.deref().field(i), right.deref().field(i)));
        }
        DBSPExpression max = new DBSPTupleExpression(maxes, false);
        return max.closure(left.asParameter(), right.asParameter());
    }
}
