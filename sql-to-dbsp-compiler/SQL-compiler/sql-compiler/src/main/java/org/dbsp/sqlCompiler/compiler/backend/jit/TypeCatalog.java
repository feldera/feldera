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
 *
 *
 */

package org.dbsp.sqlCompiler.compiler.backend.jit;

import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITRowType;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;

import java.util.HashMap;
import java.util.Map;

/**
 * Maps each tuple type to an integer id.
 */
public class TypeCatalog {
    public final Map<DBSPType, JITRowType> typeId;

    public TypeCatalog() {
        this.typeId = new HashMap<>();
    }

    public JITRowType convertTupleType(DBSPType type, ToJitVisitor jitVisitor) {
        if (type.is(DBSPTypeRef.class))
            type = type.to(DBSPTypeRef.class).type;
        DBSPTypeTupleBase tuple = type.to(DBSPTypeTupleBase.class);
        if (tuple.tupFields.length == 0)
            // Raw tuples and normal tuples represented in the same way in the JIT
            tuple = new DBSPTypeTuple();
        if (this.typeId.containsKey(tuple))
            return this.typeId.get(tuple);
        long id = this.typeId.size() + 1;  // 0 is not a valid id
        JITRowType result = new JITRowType(id, tuple, jitVisitor);
        this.typeId.put(tuple, result);
        return result;
    }

    public BaseJsonNode asJson() {
        ObjectNode result = JITNode.jsonFactory().createObjectNode();
        for (JITRowType row: this.typeId.values()) {
            result.set(Long.toString(row.id), row.asJson());
        }
        return result;
    }
}
