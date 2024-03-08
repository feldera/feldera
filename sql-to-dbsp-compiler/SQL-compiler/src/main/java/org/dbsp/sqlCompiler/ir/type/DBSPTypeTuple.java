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

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import java.util.Arrays;
import java.util.List;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.TUPLE;

/**
 * A Raw Rust tuple.
 */
public class DBSPTypeTuple extends DBSPTypeTupleBase {
    public DBSPTypeTuple(CalciteObject node, boolean mayBeNull, DBSPType... tupFields) {
        super(node, TUPLE, mayBeNull, tupFields);
    }

    public DBSPTypeTuple(CalciteObject node, DBSPType... tupFields) {
        this(node, false, tupFields);
    }

    public DBSPTypeTuple(DBSPType... tupFields) {
        this(CalciteObject.EMPTY, tupFields);
    }

    public DBSPTypeTuple(CalciteObject node, List<DBSPType> tupFields) {
        this(node, tupFields.toArray(new DBSPType[0]));
    }

    public DBSPTypeTuple(List<DBSPType> tupFields) {
        this(CalciteObject.EMPTY, tupFields);
    }

    @Override
    public boolean isRaw() {
        return false;
    }

    @Override
    public DBSPTypeTupleBase project(List<Integer> fields) {
        DBSPType[] resultFields = new DBSPType[fields.size()];
        int index = 0;
        for (int i: fields)
            resultFields[index++] = this.tupFields[i];
        return new DBSPTypeTuple(resultFields);
    }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        if (mayBeNull == this.mayBeNull)
            return this;
        return new DBSPTypeTuple(this.getNode(), mayBeNull, this.tupFields);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(tupFields);
    }

    @Override
    public boolean sameType(DBSPType type) {
        if (!super.sameNullability(type))
            return false;
        if (!type.is(DBSPTypeTuple.class))
            return false;
        DBSPTypeTuple other = type.to(DBSPTypeTuple.class);
        return DBSPType.sameTypes(this.tupFields, other.tupFields);
    }

    public DBSPTypeTuple slice(int start, int endExclusive) {
        if (endExclusive < start)
            throw new InternalCompilerError("Incorrect slice parameters " + start + ":" + endExclusive, this);
        return new DBSPTypeTuple(Utilities.arraySlice(this.tupFields, start, endExclusive));
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
     * Returns a lambda which casts every field of a tuple
     * to the corresponding field of this type.
     */
    @Override
    public DBSPExpression caster(DBSPType to) {
        if (!to.is(DBSPTypeTuple.class))
            return super.caster(to);  // throw
        DBSPTypeTuple tuple = to.to(DBSPTypeTuple.class);
        if (tuple.size() != this.size())
            return super.caster(to);  // throw
        DBSPVariablePath var = new DBSPVariablePath("x", this.ref());
        DBSPExpression[] casts = new DBSPExpression[this.tupFields.length];
        for (int i = 0; i < this.tupFields.length; i++) {
            casts[i] = this.tupFields[i].caster(tuple.tupFields[i]);
            casts[i] = casts[i].call(var.deref().field(i));
        }
        return new DBSPTupleExpression(casts).closure(var.asParameter());
    }

    @Override
    public DBSPExpression makeTuple(DBSPExpression... expressions) {
        return new DBSPTupleExpression(expressions);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.getName())
                .append("<")
                .join(", ", this.tupFields)
                .append(">");
    }

    @Override
    public DBSPType makeType(List<DBSPType> fields) {
        return new DBSPTypeTuple(CalciteObject.EMPTY, fields);
    }

    public String getName() {
        return this.code.rustName + this.tupFields.length;
    }
}
