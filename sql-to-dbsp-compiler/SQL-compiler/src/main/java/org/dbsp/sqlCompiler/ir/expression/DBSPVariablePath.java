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

package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;

/**
 * A special case of a PathExpression in Rust which refers to a variable by name.
 * More convenient that using always Paths.
 */
public class DBSPVariablePath extends DBSPExpression {
    public final String variable;

    public DBSPVariablePath(String variable, DBSPType type) {
        super(type.getNode(), type);
        this.variable = variable;
    }

    public DBSPParameter asParameter(boolean mutable) {
        return new DBSPParameter(this.variable, this.getType(), mutable);
    }

    public DBSPParameter asParameter() {
        return this.asParameter(false);
    }

    public DBSPParameter asRefParameter(boolean mutable) {
        return new DBSPParameter(
                this.variable,
                this.getType().ref(mutable));
    }

    public DBSPParameter asRefParameter() {
        return this.asRefParameter(false);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        this.type.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }


    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPVariablePath o = other.as(DBSPVariablePath.class);
        if (o == null)
            return false;
        return this.variable.equals(o.variable) &&
                this.hasSameType(o);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.variable);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPVariablePath(this.variable, this.type);
    }
}
