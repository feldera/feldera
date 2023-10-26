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

public abstract class DBSPTypeTupleBase extends DBSPType {
    public final DBSPType[] tupFields;

    protected DBSPTypeTupleBase(CalciteObject node, DBSPTypeCode code, boolean mayBeNull, DBSPType... tupFields) {
        super(node, code, mayBeNull);
        this.tupFields = tupFields;
        for (DBSPType type: this.tupFields)
            if (type == null)
                throw new NullPointerException();
    }

    public DBSPType getFieldType(int index) {
        return this.tupFields[index];
    }

    public boolean hasCopy() {
        return true;
    }

    public abstract DBSPExpression makeTuple(DBSPExpression... expressions);

    public int size() {
        return this.tupFields.length;
    }
}
