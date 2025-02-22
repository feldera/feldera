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
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

/** A special case of a PathExpression in Rust which refers to a variable by name.
 * More convenient that using always Paths. */
public final class DBSPVariablePath extends DBSPExpression {
    public final String variable;
    static long crtId = 0;
    static final String uniquePrefix = "t_";

    public DBSPVariablePath(String variable, DBSPType type) {
        super(type.getNode(), type);
        this.variable = variable;
        assert Utilities.isLegalRustIdentifier(variable);
    }

    /** Allocate a likely new variable name */
    public DBSPVariablePath(DBSPType type) {
        this(uniquePrefix + crtId++, type);
    }

    public DBSPParameter asParameter() {
        return new DBSPParameter(this.variable, this.getType());
    }

    // Do not call this method, it is only used for testing
    public static void reset() {
        crtId = 0;
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
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPVariablePath o = other.as(DBSPVariablePath.class);
        if (o == null)
            return false;
        return this.variable.equals(o.variable) &&
                this.type == o.type;
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPVariablePath otherExpression = other.as(DBSPVariablePath.class);
        if (otherExpression == null)
            return false;
        IDBSPDeclaration leftDeclaration = context.leftDeclaration.get(this.variable);
        assert leftDeclaration != null
                : "Declaration for variable " + Utilities.singleQuote(this.variable) + " not found";
        IDBSPDeclaration rightDeclaration = context.rightDeclaration.get(otherExpression.variable);
        assert rightDeclaration != null
                : "Declaration for variable " + Utilities.singleQuote(otherExpression.variable) + " not found";
        IDBSPDeclaration subst = context.leftToRight.get(leftDeclaration);
        return subst.equals(rightDeclaration);
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
