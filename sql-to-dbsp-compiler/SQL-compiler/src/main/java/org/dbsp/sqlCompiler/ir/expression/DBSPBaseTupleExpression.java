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
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.util.Linq;
import org.dbsp.util.Shuffle;

import java.util.Arrays;
import java.util.List;

public abstract class DBSPBaseTupleExpression extends DBSPExpression {
    public final DBSPExpression[] fields;

    public int size() { return this.fields.length; }

    public DBSPBaseTupleExpression(CalciteObject node, DBSPType type, DBSPExpression... expressions) {
        super(node, type);
        this.fields = expressions;
    }

    public DBSPExpression get(int index) {
        if (index >= this.fields.length)
            throw new InternalCompilerError("Index " + index + " out of bounds " + this.fields.length);
        return this.fields[index];
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(fields);
    }

    public boolean isRaw() {
        return this.is(DBSPRawTupleExpression.class);
    }

    /** Create a tuple of the same type from the specified fields */
    public abstract DBSPBaseTupleExpression fromFields(List<DBSPExpression> fields);

    public DBSPBaseTupleExpression shuffle(Shuffle data) {
        List<DBSPExpression> fields = data.shuffle(Linq.list(this.fields));
        return this.fromFields(fields);
    }

    public DBSPTypeTupleBase getTupleType() {
        return this.getType().to(DBSPTypeTupleBase.class);
    }
}
