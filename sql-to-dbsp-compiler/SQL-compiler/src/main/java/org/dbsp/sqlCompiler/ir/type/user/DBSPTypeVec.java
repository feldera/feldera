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
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.ICollectionType;
import org.dbsp.util.Utilities;

import java.util.List;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.USER;

/** Represents the type of a Rust Vec as a TypeUser.
 * (Note that Vec is not the same as a SQL Array). */
public class DBSPTypeVec extends DBSPTypeUser implements ICollectionType {
    public DBSPTypeVec(DBSPType vectorElementType, boolean mayBeNull) {
        super(vectorElementType.getNode(), USER, "Vec", mayBeNull, vectorElementType);
    }

    public DBSPType getElementType() {
        return this.getTypeArg(0);
    }

    /** vec!() or Some(vec!()), depending on nullability */
    public DBSPExpression emptyVector() {
        DBSPExpression vec = new DBSPApplyExpression("vec!", this.withMayBeNull(false));
        if (!this.mayBeNull)
            return vec;
        return vec.some();
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.startArrayProperty("typeArgs");
        int index = 0;
        for (DBSPType type: this.typeArgs) {
            visitor.propertyIndex(index);
            index++;
            type.accept(visitor);
        }
        visitor.endArrayProperty("typeArgs");
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public DBSPType withMayBeNull(boolean mayBeNull) {
        if (mayBeNull == this.mayBeNull)
            return this;
        return new DBSPTypeVec(this.getElementType(), mayBeNull);
    }

    // sameType and hashCode inherited from TypeUser.

    @SuppressWarnings("unused")
    public static DBSPTypeVec fromJson(JsonNode node, JsonDecoder decoder) {
        List<DBSPType> typeArgs = fromJsonInnerList(node, "typeArgs", decoder, DBSPType.class);
        Utilities.enforce(typeArgs.size() == 1);
        boolean mayBeNull = fromJsonMayBeNull(node);
        return new DBSPTypeVec(typeArgs.get(0), mayBeNull);
    }
}
