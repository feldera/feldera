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
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.util.IIndentStream;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.STRUCT;

public class DBSPTypeStruct extends DBSPType {
    public static class Field extends DBSPNode implements IHasType, IDBSPInnerNode {
        public final String name;
        public final DBSPType type;

        public Field(CalciteObject node, String name, DBSPType type) {
            super(node);
            this.name = name;
            this.type = type;
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
                    this.type.sameType(that.type);
        }

        @Override
        public void accept(InnerVisitor visitor) {
            if (visitor.preorder(this).stop()) return;
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
                    this.type == o.type;
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
    public final List<Field> args;
    private final HashSet<String> fields = new HashSet<>();

    public DBSPTypeStruct(CalciteObject node, String name, List<Field> args) {
        super(node, STRUCT, false);
        this.name = name;
        this.args = args;
        for (Field f: args) {
            if (this.hasField(f.getName()))
                this.error("Field name " + f + " is duplicated");
            fields.add(f.getName());
        }
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
        return this.fields.contains(fieldName);
    }

    public List<Field> getFields() { return this.args; }

    @Override
    public boolean sameType(DBSPType type) {
        if (!super.sameNullability(type))
            return false;
        if (!type.is(DBSPTypeStruct.class))
            return false;
        DBSPTypeStruct other = type.to(DBSPTypeStruct.class);
        if (!this.name.equals(other.name))
            return false;
        if (this.args.size() != other.args.size())
            return false;
        for (int i = 0; i < this.args.size(); i++)
            if (!this.args.get(i).equals(other.args.get(i)))
                return false;
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.name, this.fields.hashCode());
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.push(this);
        for (Field f: this.getFields())
            f.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("struct ")
                .append(this.name)
                .append(" {")
                .increase()
                .intercalate(System.lineSeparator(), this.fields)
                .decrease()
                .append("}");
    }
}
