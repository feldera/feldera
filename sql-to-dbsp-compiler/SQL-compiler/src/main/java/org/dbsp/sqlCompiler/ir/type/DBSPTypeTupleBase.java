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

package org.dbsp.sqlCompiler.ir.type;

import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.util.Linq;

import java.util.ArrayList;
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

    /**
     * If the expression has a tuple type, return the list of fields.
     * Else return the expression itself.
     */
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
        return this.tupFields[index];
    }

    public boolean hasCopy() {
        return false;
    }

    public abstract DBSPExpression makeTuple(DBSPExpression... expressions);

    public int size() {
        return this.tupFields.length;
    }

    public abstract DBSPType makeType(List<DBSPType> fields);
}
