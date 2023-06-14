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

import org.dbsp.sqlCompiler.circuit.DBSPNode;
import org.dbsp.sqlCompiler.circuit.IDBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.IDBSPInnerNode;
import org.dbsp.sqlCompiler.circuit.IDBSPNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.pattern.DBSPIdentifierPattern;
import org.dbsp.sqlCompiler.ir.pattern.DBSPPattern;
import org.dbsp.sqlCompiler.ir.pattern.DBSPTuplePattern;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.IHasType;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;

/**
 * Rust supports parameters with patterns, but we don't.
 * We only use simple parameters, with a single name.
 */
public class DBSPParameter extends DBSPNode implements
        IHasType, IDBSPInnerNode, IDBSPDeclaration {
    public final String name;
    public final DBSPType type;
    public final boolean mutable;

    public DBSPParameter(String name, DBSPType type, boolean mutable) {
        super(null);
        this.name = name;
        this.type = type;
        this.mutable = mutable;
    }

    public DBSPParameter(String name, DBSPType type) {
        this(name, type, false);
    }

    /**
     * Return a variable that refers to the parameter.
     */
    public DBSPVariablePath asVariableReference() {
        return new DBSPVariablePath(this.name, this.type);
    }

    @Nullable
    @Override
    public DBSPType getType() {
        return this.type;
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
    public String getName() {
        return this.name;
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPParameter o = other.as(DBSPParameter.class);
        if (o == null)
            return false;
        return this.name.equals(o.name) &&
                this.type == o.type &&
                this.mutable == o.mutable;
    }
}
