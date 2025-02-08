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

import org.dbsp.sqlCompiler.compiler.IConstructor;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.ISameValue;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.util.Linq;
import org.dbsp.util.Shuffle;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public abstract class DBSPBaseTupleExpression
        extends DBSPExpression
        implements ISameValue, IConstructor {
    // Nullable only for constant null tuple expressions
    @Nullable
    public final DBSPExpression[] fields;

    public int size() { return Objects.requireNonNull(this.fields).length; }

    public DBSPBaseTupleExpression(CalciteObject node, DBSPType type) {
        super(node, type);
        assert type.mayBeNull;
        this.fields = null;
    }

    @Override
    public boolean isConstant() {
        if (this.fields == null)
            return true;
        for (DBSPExpression field: this.fields)
            if (!field.isCompileTimeConstant()) return false;
        return true;
    }

    public boolean isNull() {
        return this.fields == null;
    }

    public DBSPBaseTupleExpression(CalciteObject node, DBSPType type, DBSPExpression... expressions) {
        super(node, type);
        this.fields = expressions;
    }

    public DBSPExpression get(int index) {
        if (this.fields == null)
            return this.type.to(DBSPTypeTupleBase.class).getFieldType(index).none();
        if (index >= Objects.requireNonNull(this.fields).length)
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
        List<DBSPExpression> fields = data.shuffle(Linq.list(Objects.requireNonNull(this.fields)));
        return this.fromFields(fields);
    }

    public DBSPTypeTupleBase getTypeAsTupleBase() {
        return this.getType().to(DBSPTypeTupleBase.class);
    }

    @Override
    public boolean sameValue(@Nullable ISameValue o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPBaseTupleExpression other = o.to(DBSPBaseTupleExpression.class);
        if (this.fields == null)
            return other.fields == null;
        if (other.fields == null)
            return false;
        if (this.fields.length != other.fields.length)
            return false;
        for (int i = 0; i < this.fields.length; i++) {
            DBSPExpression field = this.fields[i];
            DBSPExpression ofield = other.fields[i];
            if (field == null) {
                if (ofield == null)
                    continue;
                return false;
            }
            if (ofield == null)
                return false;

            ISameValue sfield = field.as(ISameValue.class);
            if (sfield == null)
                return false;
            ISameValue sofield = ofield.as(ISameValue.class);
            if (sofield == null)
                return false;
            if (!sfield.sameValue(sofield))
                return false;
        }
        return true;
    }
}
