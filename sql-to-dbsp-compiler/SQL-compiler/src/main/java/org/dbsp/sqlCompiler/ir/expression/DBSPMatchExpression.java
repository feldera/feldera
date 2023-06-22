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

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.pattern.DBSPPattern;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;

import java.util.List;

/**
 * A (Rust) match expression.
 */
@NonCoreIR
public class DBSPMatchExpression extends DBSPExpression {
    @NonCoreIR
    public static class Case extends DBSPNode implements IDBSPInnerNode {
        public final DBSPPattern against;
        public final DBSPExpression result;

        public Case(DBSPPattern against, DBSPExpression result) {
            super(against.getNode());
            this.against = against;
            this.result = result;
        }

        @Override
        public void accept(InnerVisitor visitor) {
            if (visitor.preorder(this).stop()) return;
            visitor.push(this);
            this.against.accept(visitor);
            this.result.accept(visitor);
            visitor.pop(this);
            visitor.postorder(this);
        }

        @Override
        public boolean sameFields(IDBSPNode other) {
            Case o = other.as(Case.class);
            if (o == null)
                return false;
            return this.against == o.against &&
                    this.result == o.result;
        }

        @Override
        public IIndentStream toString(IIndentStream builder) {
            return builder.append(this.against)
                    .append(" => ")
                    .append(this.result);
        }
    }

    public final DBSPExpression matched;
    public final List<Case> cases;

    public DBSPMatchExpression(DBSPExpression matched, List<Case> cases, DBSPType type) {
        super(matched.getNode(), type);
        this.matched = matched;
        this.cases = cases;
        if (cases.isEmpty())
            throw new InternalCompilerError("Empty list of cases for match", this);
        for (Case c: cases) {
            if (!c.result.getType().sameType(type))
                throw new InternalCompilerError("Type mismatch in case " + c +
                        " expected " + type + " got " + c.result.getType(), this);
        }
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.push(this);
        this.matched.accept(visitor);
        for (Case c: this.cases)
            c.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPMatchExpression o = other.as(DBSPMatchExpression.class);
        if (o == null)
            return false;
        return this.matched == o.matched &&
                Linq.same(this.cases, o.cases) &&
                this.hasSameType(o);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("match ")
                .append(this.matched)
                .append(" {")
                .increase()
                .intercalateI("," + System.lineSeparator(), this.cases)
                .decrease()
                .append("}");
    }
}
