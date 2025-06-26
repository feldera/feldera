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
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IHasType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.STRUCT;

public class DBSPTypeStruct extends DBSPType {
    public static class Field extends DBSPNode implements IHasType, IDBSPInnerNode {
        /** Position within struct */
        public final int index;
        public final ProgramIdentifier name;
        public final DBSPType type;

        public Field(CalciteObject node, ProgramIdentifier name, int index, DBSPType type) {
            super(node);
            this.name = name;
            this.index = index;
            this.type = type;
        }

        public ProgramIdentifier getName() {
            return this.name;
        }

        public String getSanitizedName() {
            return "field" + this.index;
        }

        public DBSPType getType() {
            return this.type;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.name, this.type.hashCode());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Field that = (Field) o;
            return this.name.equals(that.name) &&
                    Objects.equals(this.index, that.index) &&
                    this.type.sameType(that.type);
        }

        @Override
        public void accept(InnerVisitor visitor) {
            VisitDecision decision = visitor.preorder(this);
            if (decision.stop()) return;
            visitor.push(this);
            visitor.property("type");
            this.type.accept(visitor);
            visitor.pop(this);
            visitor.postorder(this);
        }

        @Override
        public boolean sameFields(IDBSPInnerNode other) {
            Field o = other.as(Field.class);
            if (o == null)
                return false;
            return this.name.equals(o.name) &&
                    Objects.equals(this.index, o.index) &&
                    this.type.sameType(o.type);
        }

        @Override
        public IIndentStream toString(IIndentStream builder) {
            return builder
                    .append(this.name.name())
                    .append(": ")
                    .append(this.type);
        }

        @SuppressWarnings("unused")
        public static Field fromJson(JsonNode node, JsonDecoder decoder) {
            int index = Utilities.getIntProperty(node, "index");
            ProgramIdentifier name = ProgramIdentifier.fromJson(Utilities.getProperty(node, "name"));
            DBSPType type = DBSPTypeStruct.fromJsonInner(node, "type", decoder, DBSPType.class);
            return new Field(CalciteObject.EMPTY, name, index, type);
        }
    }

    public final ProgramIdentifier name;
    public final String sanitizedName;
    public final LinkedHashMap<ProgramIdentifier, Field> fields;

    public DBSPTypeStruct(CalciteObject node, ProgramIdentifier name, String sanitizedName,
                          Collection<Field> args, boolean mayBeNull) {
        super(node, STRUCT, mayBeNull);
        this.sanitizedName = sanitizedName;
        this.name = name;
        this.fields = new LinkedHashMap<>();
        for (Field f: args) {
            if (this.hasField(f.getName()))
                this.error("Field name " + f + " is duplicated");
            this.fields.put(f.name, f);
        }
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        if (!this.sameNullability(other)) return false;
        DBSPTypeStruct type = other.as(DBSPTypeStruct.class);
        if (type == null) return false;
        if (this.fields.size() != type.fields.size())
            return false;
        for (ProgramIdentifier name: this.fields.keySet()) {
            Field otherField = type.getField(name);
            if (otherField == null)
                return false;
            Field field = Objects.requireNonNull(this.getField(name));
            if (!field.equals(otherField))
                return false;
        }
        return name == type.name &&
                this.sanitizedName.equals(type.sanitizedName);
    }

    @Nullable @Override
    public String asSqlString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ROW(");
        boolean first = true;
        for (var field: this.fields.values()) {
            if (!first)
                builder.append(", ");
            first = false;
            builder.append(field.getName())
                    .append(" ")
                    .append(field.getType().asSqlString());
        }
        builder.append(")");
        return builder.toString();
    }

    public DBSPTypeStruct rename(ProgramIdentifier newName) {
        return new DBSPTypeStruct(this.getNode(), newName, this.sanitizedName, this.fields.values(), this.mayBeNull);
    }

    @Override
    public DBSPType withMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return new DBSPTypeStruct(this.getNode(), this.name, this.sanitizedName, this.fields.values(), mayBeNull);
    }

    public boolean hasField(ProgramIdentifier fieldName) {
        return this.fields.containsKey(fieldName);
    }

    @Override
    public boolean sameType(DBSPType type) {
        if (!super.sameNullability(type))
            return false;
        if (!type.is(DBSPTypeStruct.class))
            return false;
        DBSPTypeStruct other = type.to(DBSPTypeStruct.class);
        if (!this.name.equals(other.name))
            return false;
        if (!Objects.equals(this.sanitizedName, other.sanitizedName))
            return false;
        if (this.fields.size() != other.fields.size())
            return false;
        for (ProgramIdentifier name: this.fields.keySet()) {
            Field otherField = other.getField(name);
            if (otherField == null)
                return false;
            Field field = Objects.requireNonNull(this.getField(name));
            if (!field.equals(otherField))
                return false;
        }
        return true;
    }

    @Nullable
    public Field getField(ProgramIdentifier name) {
        return this.fields.get(name);
    }

    public Field getExistingField(ProgramIdentifier name) {
        return Objects.requireNonNull(this.fields.get(name));
    }

    public Iterator<ProgramIdentifier> getFieldNames() {
        return this.fields.keySet().iterator();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.name, this.sanitizedName, this.fields.hashCode());
    }

    public DBSPType getFieldType(ProgramIdentifier fieldName) {
        Field field = this.getExistingField(fieldName);
        return field.type;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.startArrayProperty("fields");
        int index = 0;
        for (Field f: this.fields.values()) {
            visitor.propertyIndex(index);
            index++;
            f.accept(visitor);
        }
        visitor.endArrayProperty("fields");
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("struct ")
                .append(this.mayBeNull ? "?" : "")
                .append(this.name.name());
    }

    /** Generate a tuple type by ignoring the struct and field names. */
    public DBSPTypeTuple toTuple() {
        List<DBSPType> types = Linq.list(Linq.map(this.fields.values(), f -> f.type));
        return new DBSPTypeTuple(this.getNode(), this.mayBeNull, this, types);
    }

    private static DBSPType toTupleDeep(DBSPType type) {
        if (type.is(DBSPTypeStruct.class)) {
            DBSPTypeStruct struct = type.to(DBSPTypeStruct.class);
            List<DBSPType> types = Linq.list(Linq.map(struct.fields.values(), f -> toTupleDeep(f.type)));
            return new DBSPTypeTuple(struct.getNode(), type.mayBeNull, types);
        } else if (type.is(DBSPTypeUser.class)) {
            DBSPTypeUser user = type.to(DBSPTypeUser.class);
            DBSPType[] args = Linq.map(user.typeArgs, DBSPTypeStruct::toTupleDeep, DBSPType.class);
            return new DBSPTypeUser(user.getNode(), user.code, user.name, user.mayBeNull, args);
        } else if (type.is(DBSPTypeTupleBase.class)) {
            DBSPTypeTupleBase tuple = type.to(DBSPTypeTupleBase.class);
            DBSPType[] fields = Linq.map(tuple.tupFields, DBSPTypeStruct::toTupleDeep, DBSPType.class);
            return tuple.makeRelatedTupleType(Linq.list(fields));
        }
        return type;
    }

    @Override
    public DBSPExpression defaultValue() {
        throw new UnimplementedException();
    }

    @Override
    public int getToplevelFieldCount() {
        return this.fields.size();
    }

    /** Generate a tuple type by ignoring the struct and field names, recursively. */
    public DBSPType toTupleDeep() {
        return toTupleDeep(this);
    }

    @SuppressWarnings("unused")
    public static DBSPTypeStruct fromJson(JsonNode node, JsonDecoder decoder) {
        boolean mayBeNull = DBSPType.fromJsonMayBeNull(node);
        ProgramIdentifier name = ProgramIdentifier.fromJson(Utilities.getProperty(node, "name"));
        String sanitizedName = Utilities.getStringProperty(node, "sanitizedName");
        List<DBSPTypeStruct.Field> fields = fromJsonInnerList(node, "fields", decoder, DBSPTypeStruct.Field.class);
        return new DBSPTypeStruct(CalciteObject.EMPTY, name, sanitizedName, fields, mayBeNull);
    }
}
