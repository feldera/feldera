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
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.pattern.DBSPPattern;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.util.IIndentStream;

@NonCoreIR
public class DBSPForExpression extends DBSPExpression {
    public final DBSPPattern pattern;
    public final DBSPExpression iterated;
    public final DBSPBlockExpression block;

    public DBSPForExpression(DBSPPattern pattern, DBSPExpression iterated, DBSPBlockExpression block) {
        super(pattern.getNode(), new DBSPTypeRawTuple());
        this.pattern = pattern;
        this.iterated = iterated;
        this.block = block;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        this.pattern.accept(visitor);
        this.iterated.accept(visitor);
        this.block.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPForExpression o = other.as(DBSPForExpression.class);
        if (o == null)
            return false;
        return this.pattern == o.pattern &&
                this.iterated == o.iterated &&
                this.block == o.block &&
                this.hasSameType(o);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("for ")
                .append(this.pattern)
                .append(" in ")
                .append(this.iterated)
                .append(" ")
                .append(this.block);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPForExpression(this.pattern, this.iterated.deepCopy(),
                this.block.deepCopy().to(DBSPBlockExpression.class));
    }
}
