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
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.util.Utilities;

public class DBSPTypeIndexedZSet extends DBSPTypeUser {
    public final DBSPType keyType;
    public final DBSPType elementType;

    public DBSPTypeIndexedZSet(CalciteObject node, DBSPType keyType,
                               DBSPType elementType) {
        super(node, DBSPTypeCode.INDEXED_ZSET, "IndexedWSet", false, keyType, elementType);
        this.keyType = keyType;
        this.elementType = elementType;
        Utilities.enforce(!elementType.is(DBSPTypeZSet.class));
        Utilities.enforce(!elementType.is(DBSPTypeIndexedZSet.class));
    }

    @Override
    public int getToplevelFieldCount() {
        return this.keyType.getToplevelFieldCount() + this.elementType.getToplevelFieldCount();
    }

    /** Build an IndexedZSet type from a tuple with 2 elements: key type, value type */
    public DBSPTypeIndexedZSet(DBSPTypeRawTuple tuple) {
        this(tuple.getNode(), tuple.getFieldType(0), tuple.getFieldType(1));
    }

    public DBSPTypeTuple getKeyTypeTuple() {
        return this.keyType.to(DBSPTypeTuple.class);
    }

    public DBSPTypeTuple getElementTypeTuple() {
        return this.elementType.to(DBSPTypeTuple.class);
    }

    public DBSPTypeRawTuple getKVRefType() {
        return new DBSPTypeRawTuple(this.keyType.ref(), this.elementType.ref());
    }

    public DBSPTypeRawTuple getKVType() {
        return new DBSPTypeRawTuple(this.keyType, this.elementType);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("keyType");
        this.keyType.accept(visitor);
        visitor.property("elementType");
        this.elementType.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @SuppressWarnings("unused")
    public static DBSPTypeIndexedZSet fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPType keyType = fromJsonInner(node, "keyType", decoder, DBSPType.class);
        DBSPType elementType = fromJsonInner(node, "elementType", decoder, DBSPType.class);
        return new DBSPTypeIndexedZSet(CalciteObject.EMPTY, keyType, elementType);
    }
}
