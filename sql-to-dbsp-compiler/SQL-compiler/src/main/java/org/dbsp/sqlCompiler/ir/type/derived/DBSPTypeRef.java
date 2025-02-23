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
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import java.util.Objects;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.REF;

/** A type of the form &type. */
public class DBSPTypeRef extends DBSPType {
    public final DBSPType type;
    public final boolean mutable;

    public DBSPTypeRef(DBSPType type, boolean mutable, boolean mayBeNull) {
        super(type.getNode(), REF, mayBeNull);
        assert !type.is(DBSPTypeRef.class);
        this.type = type;
        this.mutable = mutable;
    }

    public DBSPTypeRef(DBSPType type) {
        this(type, false, false);
    }

    @Override
    public DBSPType deref() {
        return this.type;
    }

    @Override
    public DBSPType withMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return new DBSPTypeRef(this.type, this.mutable, mayBeNull);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        if (!this.sameNullability(other)) return false;
        DBSPTypeRef ref = other.as(DBSPTypeRef.class);
        if (ref == null) return false;
        return this.type == ref.type && this.mutable == ref.mutable;
    }

    @Override
    public DBSPExpression defaultValue() {
        return this.type.defaultValue().borrow();
    }

    @Override
    public int getToplevelFieldCount() {
        return this.type.getToplevelFieldCount();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), type.hashCode(), mutable);
    }

    public boolean sameType(DBSPType other) {
        if (!super.sameNullability(other))
            return false;
        DBSPTypeRef oRef = other.as(DBSPTypeRef.class);
        if (oRef == null)
            return false;
        return this.type.sameType(oRef.type);
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
    public IIndentStream toString(IIndentStream builder) {
        return builder
                .append(this.mayBeNull ? "Option<" : "")
                .append("&")
                .append(this.mutable ? "mut " : "")
                .append(this.type)
                .append(this.mayBeNull ? ">" : "");
    }

    @SuppressWarnings("unused")
    public static DBSPTypeRef fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPType type = fromJsonInner(node, "type", decoder, DBSPType.class);
        boolean mayBeNull = DBSPType.fromJsonMayBeNull(node);
        boolean mutable = Utilities.getBooleanProperty(node, "mutable");
        return new DBSPTypeRef(type, mutable, mayBeNull);
    }
}