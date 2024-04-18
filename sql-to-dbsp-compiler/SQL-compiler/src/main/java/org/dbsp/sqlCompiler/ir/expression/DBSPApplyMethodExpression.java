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

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;

/**
 * Function application expression.
 */
public class DBSPApplyMethodExpression extends DBSPApplyBaseExpression {
    public final DBSPExpression self;

    public DBSPApplyMethodExpression(CalciteObject node,
            String function, DBSPType returnType,
            DBSPExpression self, DBSPExpression... arguments) {
        super(node, new DBSPPath(function).toExpression(), returnType, arguments);
        this.self = self;
    }

    public DBSPApplyMethodExpression(String function, DBSPType returnType,
                                     DBSPExpression self, DBSPExpression... arguments) {
        this(CalciteObject.EMPTY, function, returnType, self, arguments);
    }

    public DBSPApplyMethodExpression(
            DBSPExpression function, DBSPType returnType,
            DBSPExpression self, DBSPExpression... arguments) {
        super(CalciteObject.EMPTY, function, returnType, arguments);
        this.self = self;
        this.checkArgs(true);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        this.type.accept(visitor);
        this.self.accept(visitor);
        this.function.accept(visitor);
        for (DBSPExpression arg: this.arguments)
            arg.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPApplyMethodExpression o = other.as(DBSPApplyMethodExpression.class);
        if (o == null)
            return false;
        return this.function == o.function &&
                this.self == o.self &&
                Linq.same(this.arguments, o.arguments) &&
                this.hasSameType(o);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.self)
                .append(".")
                .append(this.function)
                .append("(")
                .join(", ", this.arguments)
                .append(")");
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPApplyMethodExpression(this.function.deepCopy(), this.getType(),
                this.self.deepCopy(), Linq.map(this.arguments, DBSPExpression::deepCopy, DBSPExpression.class));
    }
}
