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
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.util.IIndentStream;

import java.util.Arrays;
import java.util.List;

/**
 * A Raw Rust tuple.
 */
public class DBSPTypeRawTuple extends DBSPTypeTupleBase {
    private DBSPTypeRawTuple(CalciteObject node, DBSPTypeCode code, boolean mayBeNull, DBSPType... tupArgs) {
        super(node, code, mayBeNull, tupArgs);
    }

    @Override
    public boolean isRaw() {
        return true;
    }

    public DBSPTypeRawTuple(DBSPType... tupArgs) {
        this(CalciteObject.EMPTY, DBSPTypeCode.RAW_TUPLE, false, tupArgs);
    }

    public DBSPTypeRawTuple(CalciteObject node, List<DBSPType> tupArgs) {
        this(node, DBSPTypeCode.RAW_TUPLE, false, tupArgs.toArray(new DBSPType[0]));
    }

    @Override
    public DBSPExpression makeTuple(DBSPExpression... expressions) {
        return new DBSPRawTupleExpression(expressions);
    }

    @Override
    public DBSPType makeType(List<DBSPType> fields) {
        return new DBSPTypeRawTuple(CalciteObject.EMPTY, fields);
    }

    @Override
    public DBSPTypeTupleBase project(List<Integer> fields) {
        DBSPType[] resultFields = new DBSPType[fields.size()];
        int index = 0;
        for (int i: fields)
            resultFields[index++] = this.tupFields[i];
        return new DBSPTypeRawTuple(resultFields);
    }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        if (mayBeNull == this.mayBeNull)
            return this;
        return new DBSPTypeRawTuple(this.getNode(), this.code, mayBeNull, this.tupFields);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(tupFields);
    }

    @Override
    public boolean sameType(DBSPType type) {
        if (!super.sameNullability(type))
            return false;
        if (!type.is(DBSPTypeRawTuple.class))
            return false;
        DBSPTypeRawTuple other = type.to(DBSPTypeRawTuple.class);
        return DBSPType.sameTypes(this.tupFields, other.tupFields);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        for (DBSPType type: this.tupFields)
            type.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    /**
     * @return A closure that casts every member of a tuple to
     * generate a raw tuple of this type. */
    @Override
    public DBSPExpression caster(DBSPType to) {
        if (!to.is(DBSPTypeRawTuple.class))
            return super.caster(to);  // throw
        DBSPTypeRawTuple tuple = to.to(DBSPTypeRawTuple.class);
        if (tuple.size() != this.size())
            return super.caster(to);  // throw
        DBSPVariablePath var = new DBSPVariablePath("x", this.ref());
        DBSPExpression[] casts = new DBSPExpression[this.tupFields.length];
        for (int i = 0; i < this.tupFields.length; i++) {
            casts[i] = this.tupFields[i].caster(tuple.tupFields[i]);
            casts[i] = casts[i].call(var.deref().field(i));
        }
        return new DBSPRawTupleExpression(casts).closure(var.asParameter());
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("(")
                .intercalateI(", ", this.tupFields)
                .append(")");
    }
}
