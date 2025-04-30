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

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.TUPLE;

/** Our own version of a tuple */
public class DBSPTypeTuple extends DBSPTypeTupleBase {
    /** If the type is produced from a struct type, keep here original type.
     * WARNING: this field is ignored by sameType! */
    @Nullable
    public final DBSPTypeStruct originalStruct;

    public DBSPTypeTuple(CalciteObject node, boolean mayBeNull, @Nullable DBSPTypeStruct originalStruct, DBSPType... tupFields) {
        super(node, TUPLE, mayBeNull, tupFields);
        this.originalStruct = originalStruct;
    }

    public DBSPTypeTuple(CalciteObject node, boolean mayBeNull, @Nullable DBSPTypeStruct originalStruct, List<DBSPType> tupFields) {
        this(node, mayBeNull, originalStruct, tupFields.toArray(new DBSPType[0]));
    }

    public DBSPTypeTuple(CalciteObject node, DBSPType... tupFields) {
        this(node, false, null, tupFields);
    }

    public DBSPTypeTuple(DBSPType... tupFields) {
        this(CalciteObject.EMPTY, tupFields);
    }

    public DBSPTypeTuple(CalciteObject node, List<DBSPType> tupFields) {
        this(node, tupFields.toArray(new DBSPType[0]));
    }

    public DBSPTypeTuple(CalciteObject node, boolean mayBeNull, List<DBSPType> tupFields) {
        this(node, mayBeNull, null, tupFields.toArray(new DBSPType[0]));
    }

    public DBSPTypeTuple(List<DBSPType> tupFields) {
        this(CalciteObject.EMPTY, tupFields);
    }

    @Override
    public boolean isRaw() {
        return false;
    }

    @Override
    public int getToplevelFieldCount() {
        // Note: do *not* recurse into children
        return this.size();
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
    public DBSPType withMayBeNull(boolean mayBeNull) {
        if (mayBeNull == this.mayBeNull)
            return this;
        return new DBSPTypeTuple(this.getNode(), mayBeNull, this.originalStruct, this.tupFields);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(tupFields);
    }

    // Field names are ingored when comparing types!
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
        visitor.startArrayProperty("tupFields");
        int index = 0;
        for (DBSPType type: this.tupFields) {
            visitor.propertyIndex(index);
            index++;
            type.accept(visitor);
        }
        visitor.endArrayProperty("tupFields");
        visitor.pop(this);
        visitor.postorder(this);
    }

    /** Returns a lambda which casts every field of a tuple
     * to the corresponding field of this type. */
    @Override
    public DBSPClosureExpression caster(DBSPType to, boolean safe) {
        if (!to.is(DBSPTypeTuple.class))
            return super.caster(to, safe);  // throw
        DBSPTypeTuple tuple = to.to(DBSPTypeTuple.class);
        if (tuple.size() != this.size())
            return super.caster(to, safe);  // throw
        DBSPVariablePath var = this.ref().var();
        DBSPExpression[] casts = new DBSPExpression[this.tupFields.length];
        for (int i = 0; i < this.tupFields.length; i++) {
            casts[i] = this.tupFields[i].caster(tuple.tupFields[i], safe);
            casts[i] = casts[i].call(var.deepCopy().deref().field(i).borrow());
        }
        return new DBSPTupleExpression(to.mayBeNull, casts).closure(var);
    }

    @Override
    public DBSPExpression makeTuple(DBSPExpression... expressions) {
        return new DBSPTupleExpression(this.getNode(), this, expressions);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.getName())
                .append(this.mayBeNull ? "?" : "")
                .append("<")
                .join(", ", this.tupFields)
                .append(">");
    }

    @Override
    public DBSPTypeTuple makeRelatedTupleType(List<DBSPType> fields) {
        return new DBSPTypeTuple(CalciteObject.EMPTY, this.mayBeNull, fields);
    }

    public String getName() {
        return this.code.rustName + this.tupFields.length;
    }

    @SuppressWarnings("unused")
    public static DBSPTypeTuple fromJson(JsonNode node, JsonDecoder decoder) {
        boolean mayBeNull = DBSPType.fromJsonMayBeNull(node);
        List<DBSPType> fields = fromJsonInnerList(node, "tupFields", decoder, DBSPType.class);
        return new DBSPTypeTuple(CalciteObject.EMPTY, mayBeNull, fields);
    }
}
