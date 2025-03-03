/*
 * Copyright 2023 VMware, Inc.
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

package org.dbsp.sqlCompiler.ir;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IHasType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

/**
 * Rust supports parameters with patterns, but we don't.
 * We only use simple parameters, with a single name. */
public final class DBSPParameter extends DBSPNode implements
        IHasType, IDBSPInnerNode, IDBSPDeclaration {
    public final String name;
    public final DBSPType type;

    public DBSPParameter(String name, DBSPType type) {
        super(type.getNode());
        this.name = name;
        this.type = type;
    }

    /** Return a variable that refers to the parameter. */
    public DBSPVariablePath asVariable() {
        return new DBSPVariablePath(this.name, this.type);
    }

    @Override
    public DBSPType getType() {
        return this.type;
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
    public String getName() {
        return this.name;
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPParameter o = other.as(DBSPParameter.class);
        if (o == null)
            return false;
        return this.name.equals(o.name) &&
                this.type.sameType(o.type);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.name)
                .append(": ")
                .append(this.type);
    }

    @SuppressWarnings("unused")
    public static DBSPParameter fromJson(JsonNode node, JsonDecoder decoder) {
        String name = Utilities.getStringProperty(node, "name");
        DBSPType type = DBSPNode.fromJsonInner(node, "type", decoder, DBSPType.class);
        return new DBSPParameter(name, type);
    }
}
