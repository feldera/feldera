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
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.FUNCTION;

public class DBSPTypeFunction extends DBSPType {
    public final DBSPType resultType;
    public final DBSPType[] parameterTypes;

    public DBSPTypeFunction(DBSPType resultType, DBSPType... parameterTypes) {
        super(false, FUNCTION);
        this.resultType = resultType;
        this.parameterTypes = parameterTypes;
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
    public int getToplevelFieldCount() {
        return this.resultType.getToplevelFieldCount();
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("resultType");
        this.resultType.accept(visitor);
        visitor.startArrayProperty("parameterTypes");
        int index = 0;
        for (DBSPType arg: this.parameterTypes) {
            visitor.propertyIndex(index);
            index++;
            arg.accept(visitor);
        }
        visitor.endArrayProperty("parameterTypes");
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        if (!this.sameNullability(other)) return false;
        DBSPTypeFunction type = other.as(DBSPTypeFunction.class);
        if (type == null) return false;
        return resultType == type.resultType &&
                Linq.same(this.parameterTypes, type.parameterTypes);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(super.hashCode(), resultType);
        result = 31 * result + Arrays.hashCode(parameterTypes);
        return result;
    }

    @Override
    public boolean sameType(DBSPType type) {
        if (!super.sameNullability(type))
            return false;
        if (!type.is(DBSPTypeFunction.class))
            return false;
        DBSPTypeFunction other = type.to(DBSPTypeFunction.class);
        if (!this.resultType.sameType(other.resultType))
            return false;
        return DBSPType.sameTypes(this.parameterTypes, other.parameterTypes);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("|")
                .join(", ", this.parameterTypes)
                .append("| -> ")
                .append(this.resultType);
    }

    @SuppressWarnings("unused")
    public static DBSPTypeFunction fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPType resultType = fromJsonInner(node, "resultType", decoder, DBSPType.class);
        List<DBSPType> params = fromJsonInnerList(node, "parameterTypes", decoder, DBSPType.class);
        return new DBSPTypeFunction(resultType, params.toArray(new DBSPType[0]));
    }
}
