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

package org.dbsp.sqlCompiler.ir.type.primitive;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.util.IIndentStream;

import java.util.Objects;

/** An unknown type, represented in code as _. */
public class DBSPTypeAny extends DBSPType {
    DBSPTypeAny() {
        super(CalciteObject.EMPTY, DBSPTypeCode.ANY, false);
    }

    public static final DBSPTypeAny INSTANCE = new DBSPTypeAny();

    public static DBSPTypeAny getDefault() {
        return DBSPTypeAny.INSTANCE;
    }

    @Override
    public DBSPType withMayBeNull(boolean mayBeNull) {
        return this;
    }

    @Override
    public DBSPExpression defaultValue() {
        throw new UnimplementedException();
    }

    @Override
    public int getToplevelFieldCount() {
        throw new UnimplementedException();
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        if (this.sameNullability(other)) return false;
        DBSPTypeAny type = other.as(DBSPTypeAny.class);
        if (type == null) return false;
        return this.code == type.code;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.mayBeNull, 1);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameType(DBSPType other) {
        if (!super.sameNullability(other))
            return false;
        return other.is(DBSPTypeAny.class);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("_");
    }

    @SuppressWarnings("unused")
    public static DBSPTypeAny fromJson(JsonNode node, JsonDecoder decoder) {
        return DBSPTypeAny.INSTANCE;
    }
}
