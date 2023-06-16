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

import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeFunction;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;

/**
 * Function application expression.
 * Note: the type of the expression is the type of the result returned by the function.
 */
public class DBSPApplyExpression extends DBSPExpression {
    public final DBSPExpression function;
    public final DBSPExpression[] arguments;

    void checkArgs() {
        for (DBSPExpression arg: this.arguments)
            if (arg == null)
                throw new RuntimeException("Null arg");
    }

    public DBSPApplyExpression(CalciteObject node, String function, DBSPType returnType, DBSPExpression... arguments) {
        super(node, returnType);
        this.function = new DBSPPathExpression(DBSPTypeAny.INSTANCE, new DBSPPath(function));
        this.arguments = arguments;
        this.checkArgs();
    }

    public DBSPApplyExpression(String function, DBSPType returnType, DBSPExpression... arguments) {
        this(new CalciteObject(), function, returnType, arguments);
    }

    public static DBSPType getReturnType(DBSPType type) {
        if (type.is(DBSPTypeAny.class))
            return type;
        DBSPTypeFunction func = type.to(DBSPTypeFunction.class);
        return func.resultType;
    }

    public DBSPApplyExpression(DBSPExpression function, DBSPExpression... arguments) {
        this(function, DBSPApplyExpression.getReturnType(function.getType()), arguments);
    }

    public DBSPApplyExpression(DBSPExpression function, DBSPType returnType, DBSPExpression... arguments) {
        super(new CalciteObject(), returnType);
        this.function = function;
        this.arguments = arguments;
        this.checkArgs();
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.push(this);
        this.type.accept(visitor);
        this.function.accept(visitor);
        for (DBSPExpression arg: this.arguments)
            arg.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPApplyExpression o = other.as(DBSPApplyExpression.class);
        if (o == null)
            return false;
        return this.function == o.function &&
                Linq.same(this.arguments, o.arguments) &&
                this.hasSameType(o);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.function)
                .append("(")
                .join(", ", this.arguments)
                .append(")");
    }
}
