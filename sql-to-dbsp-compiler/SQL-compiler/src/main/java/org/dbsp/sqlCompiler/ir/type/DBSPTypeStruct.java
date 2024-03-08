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
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.STRUCT;

public class DBSPTypeStruct extends DBSPType {
    public static class Field extends DBSPNode implements IHasType, IDBSPInnerNode {
        /**
         * Names coming from SQL may not be usable in Rust.
         * The sanitized name is the one used in code generation.
         * Initially names may not be "sane", but prior to code generation they
         * have to be sanitized.
         */
        public final String sanitizedName;
        public final String name;
        public final boolean nameIsQuoted;
        public final DBSPType type;

        public Field(CalciteObject node, String name, String sanitizedName, DBSPType type, boolean nameIsQuoted) {
            super(node);
            this.sanitizedName = sanitizedName;
            this.name = name;
            this.type = type;
            this.nameIsQuoted = nameIsQuoted;
        }

        public String getName() {
            return this.name;
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
                    this.sanitizedName.equals(that.sanitizedName) &&
                    this.type.sameType(that.type);
        }

        @Override
        public void accept(InnerVisitor visitor) {
            VisitDecision decision = visitor.preorder(this);
            if (decision.stop()) return;
            visitor.push(this);
            this.type.accept(visitor);
            visitor.pop(this);
            visitor.postorder(this);
        }

        @Override
        public boolean sameFields(IDBSPNode other) {
            Field o = other.as(Field.class);
            if (o == null)
                return false;
            return this.name.equals(o.name) &&
                    this.sanitizedName.equals(o.sanitizedName) &&
                    this.type.sameType(o.type);
        }

        @Override
        public IIndentStream toString(IIndentStream builder) {
            return builder
                    .append(this.name)
                    .append(": ")
                    .append(this.type);
        }
    }

    public final String name;
    @Nullable
    public final String sanitizedName;
    public final LinkedHashMap<String, Field> fields;

    public DBSPTypeStruct(CalciteObject node, String name, @Nullable String sanitizedName, Collection<Field> args) {
        super(node, STRUCT, false);
        this.sanitizedName = sanitizedName;
        assert this.sanitizedName == null || Utilities.isLegalRustIdentifier(sanitizedName);
        this.name = name;
        this.fields = new LinkedHashMap<>();
        for (Field f: args) {
            if (this.hasField(f.getName()))
                this.error("Field name " + f + " is duplicated");
            this.fields.put(f.name, f);
        }
    }

    public DBSPTypeStruct rename(String newName) {
        return new DBSPTypeStruct(this.getNode(), newName, this.sanitizedName, this.fields.values());
    }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        if (mayBeNull)
            this.error("Nullable structs not supported");
        return this;
    }

    public boolean hasField(String fieldName) {
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
        for (String name: this.fields.keySet()) {
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
    public Field getField(String name) {
        return this.fields.get(name);
    }

    public Field getExistingField(String name) {
        return Objects.requireNonNull(this.fields.get(name));
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.name, this.sanitizedName, this.fields.hashCode());
    }

    public DBSPType getFieldType(String fieldName) {
        Field field = this.getExistingField(fieldName);
        return field.type;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        for (Field f: this.fields.values())
            f.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append("struct ")
                .append(this.sanitizedName != null ? this.sanitizedName : this.name)
                .append(" {")
                .increase();
        for (DBSPTypeStruct.Field field: this.fields.values()) {
            builder.append(field)
                    .newline();
        }
        return builder
                .decrease()
                .append("}");
    }

    /** Generate a tuple type by ignoring the struct and field names. */
    public DBSPTypeTuple toTuple() {
        List<DBSPType> types = Linq.list(Linq.map(this.fields.values(), f -> f.type));
        return new DBSPTypeTuple(this.getNode(), types);
    }
}
