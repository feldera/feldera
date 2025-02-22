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

package org.dbsp.sqlCompiler.ir.statement;

import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;

public final class DBSPLetStatement extends DBSPStatement implements IDBSPDeclaration {
    public final String variable;
    public final DBSPType type;
    @Nullable
    public final DBSPExpression initializer;
    public final boolean mutable;

    public DBSPLetStatement(String variable, DBSPExpression initializer, boolean mutable) {
        super(initializer.getNode());
        this.variable = variable;
        this.initializer = initializer;
        this.type = initializer.getType();
        this.mutable = mutable;
    }

    @Override
    public DBSPStatement deepCopy() {
        if (this.initializer != null)
            return new DBSPLetStatement(this.variable, this.initializer.deepCopy(), this.mutable);
        else
            return new DBSPLetStatement(this.variable, this.type, this.mutable);
    }

    @Override
    public EquivalenceResult equivalent(EquivalenceContext context, DBSPStatement other) {
        DBSPLetStatement otherStatement = other.as(DBSPLetStatement.class);
        if (otherStatement == null)
            return new EquivalenceResult(false, context);
        if (!context.equivalent(this.initializer, otherStatement.initializer))
            return new EquivalenceResult(false, context);
        if (this.mutable != otherStatement.mutable)
            return new EquivalenceResult(false, context);
        EquivalenceContext newContext = context.clone();
        newContext.leftDeclaration.substitute(this.variable, this);
        newContext.rightDeclaration.substitute(otherStatement.variable, otherStatement);
        newContext.leftToRight.put(this, otherStatement);
        return new EquivalenceResult(true, newContext);
    }

    public DBSPLetStatement(String variable, DBSPType type, boolean mutable) {
        super(type.getNode());
        this.variable = variable;
        this.initializer = null;
        this.type = type;
        this.mutable = mutable;
    }

    public DBSPLetStatement(String variable, DBSPExpression initializer) {
        this(variable, initializer, false);
    }

    @Override
    public String getName() {
        return this.variable;
    }

    public DBSPVariablePath getVarReference() {
        return new DBSPVariablePath(this.variable, this.type);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("type");
        this.type.accept(visitor);
        if (this.initializer != null) {
            visitor.property("initializer");
            this.initializer.accept(visitor);
        }
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPLetStatement o = other.as(DBSPLetStatement.class);
        if (o == null)
            return false;
        return this.variable.equals(o.variable) &&
                this.initializer == o.initializer &&
                this.type.sameType(o.type) &&
                this.mutable == o.mutable;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append("let ")
                .append(this.mutable ? "mut " : "")
                .append(this.variable)
                .append(": ")
                .append(this.type);
        if (this.initializer != null)
            builder.append(" = ")
                    .append(this.initializer);
        return builder;
    }

    @Override
    public DBSPType getType() {
        return this.type;
    }
}
