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
import org.dbsp.sqlCompiler.circuit.IDBSPInnerNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
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

public class DBSPParameter extends DBSPNode implements IHasType, IDBSPInnerNode {
    public final DBSPPattern pattern;
    public final DBSPType type;

    public DBSPParameter(DBSPPattern pattern, DBSPType type) {
        super(null);
        this.pattern = pattern;
        this.type = type;
    }

    public DBSPParameter(String name, DBSPType type) {
        super(null);
        this.pattern = new DBSPIdentifierPattern(name);
        this.type = type;
    }

    public DBSPParameter(DBSPVariablePath... variables) {
        super(null);
        this.pattern = new DBSPTuplePattern(Linq.map(variables, DBSPVariablePath::asPattern, DBSPPattern.class));
        this.type = new DBSPTypeRawTuple(Linq.map(variables, DBSPExpression::getType, DBSPType.class));
    }

    /**
     * This is only defined for parameters that have an IdentifierPattern.
     * Return a variable that refers to the parameter.
     */
    public DBSPVariablePath asVariableReference() {
        DBSPIdentifierPattern id = this.pattern.to(DBSPIdentifierPattern.class);
        return new DBSPVariablePath(id.identifier, this.type);
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
        this.pattern.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }
}
