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
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.ICollectionType;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.ZSET;

public class DBSPTypeZSet extends DBSPTypeUser implements ICollectionType {
    public final DBSPType elementType;

    public DBSPTypeZSet(CalciteObject node, DBSPType elementType) {
        super(node, ZSET, "WSet", false, elementType);
        this.elementType = elementType;
        assert !elementType.is(DBSPTypeZSet.class);
        assert !elementType.is(DBSPTypeIndexedZSet.class);
    }

    public DBSPTypeZSet(DBSPType elementType) {
        this(elementType.getNode(), elementType);
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
    public int getToplevelFieldCount() {
        return this.elementType.getToplevelFieldCount();
    }

    @Override
    public DBSPType getElementType() {
        return this.elementType;
    }

    // sameType and hashCode inherited from TypeUser

    @SuppressWarnings("unused")
    public static DBSPTypeZSet fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPType elementType = DBSPNode.fromJsonInner(node, "elementType", decoder, DBSPType.class);
        return new DBSPTypeZSet(elementType);
    }
}
