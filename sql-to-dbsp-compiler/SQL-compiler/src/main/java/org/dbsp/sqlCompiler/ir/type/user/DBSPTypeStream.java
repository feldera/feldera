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

package org.dbsp.sqlCompiler.ir.type.user;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import java.util.Objects;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.STREAM;

/** A type of the form 'Stream<Circuit, elementType>' */
public class DBSPTypeStream extends DBSPType {
    public final DBSPType elementType;
    /** Whether the stream carries deltas, collections, or scalars. */
    public final StreamKind kind;
    /** Depth of the circuit the stream belongs to: 0 for the root circuit,
     * 1 for a stream inside a nested (recursive) circuit. */
    public final int nesting;

    public DBSPTypeStream(DBSPType elementType, StreamKind kind, int nesting) {
        super(elementType.getNode(), STREAM, elementType.mayBeNull);
        Utilities.enforce(nesting >= 0);
        this.elementType = elementType;
        this.kind = kind;
        this.nesting = nesting;
    }

    @Override
    public int getToplevelFieldCount() {
        return this.elementType.getToplevelFieldCount();
    }

    @Override
    public DBSPType withMayBeNull(boolean mayBeNull) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DBSPExpression defaultValue() {
        throw new UnimplementedException();
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        if (!this.sameNullability(other)) return false;
        DBSPTypeStream s = other.as(DBSPTypeStream.class);
        if (s == null) return false;
        return this.elementType == s.elementType &&
                this.kind == s.kind &&
                this.nesting == s.nesting;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("elementType");
        this.elementType.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                this.elementType.hashCode(), this.kind, this.nesting);
    }

    @Override
    public boolean sameType(DBSPType other) {
        if (!super.sameNullability(other))
            return false;
        DBSPTypeStream oRef = other.as(DBSPTypeStream.class);
        if (oRef == null)
            return false;
        return this.elementType.sameType(oRef.elementType) &&
                this.kind == oRef.kind &&
                this.nesting == oRef.nesting;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("Stream<")
                .append(this.elementType)
                .append(">");
    }

    @SuppressWarnings("unused")
    public static DBSPTypeStream fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPType elementType = fromJsonInner(node, "elementType", decoder, DBSPType.class);
        StreamKind kind = StreamKind.valueOf(Utilities.getStringProperty(node, "kind"));
        int nesting = Utilities.getIntProperty(node, "nesting");
        return new DBSPTypeStream(elementType, kind, nesting);
    }
}
