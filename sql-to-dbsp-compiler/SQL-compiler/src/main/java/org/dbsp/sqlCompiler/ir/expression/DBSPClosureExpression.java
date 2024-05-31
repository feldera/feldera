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

import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.BetaReduction;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeFunction;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;

/** An expression of the form |param0, param1, ...| body. */
public final class DBSPClosureExpression extends DBSPExpression {
    public final DBSPExpression body;
    public final DBSPParameter[] parameters;

    public DBSPTypeFunction getFunctionType() {
        return this.getType().to(DBSPTypeFunction.class);
    }

    public DBSPType getResultType() {
        return this.getFunctionType().resultType;
    }

    public DBSPClosureExpression(CalciteObject node, DBSPExpression body, DBSPParameter... parameters) {
        super(node, new DBSPTypeFunction(body.getType(), Linq.map(parameters, DBSPParameter::getType, DBSPType.class)));
        this.body = body;
        this.parameters = parameters;
    }

    public DBSPClosureExpression(DBSPExpression body, DBSPParameter... variables) {
        this(CalciteObject.EMPTY, body, variables);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPClosureExpression(this.getNode(), this.body.deepCopy(), this.parameters);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPClosureExpression otherExpression = other.as(DBSPClosureExpression.class);
        if (otherExpression == null)
            return false;
        if (this.parameters.length != otherExpression.parameters.length)
            return false;
        EquivalenceContext newContext = context.clone();
        newContext.leftDeclaration.newContext();
        newContext.rightDeclaration.newContext();
        for (int i = 0; i < parameters.length; i++) {
            newContext.leftDeclaration.substitute(this.parameters[i].name, this.parameters[i]);
            newContext.rightDeclaration.substitute(otherExpression.parameters[i].name, otherExpression.parameters[i]);
            newContext.leftToRight.put(this.parameters[i], otherExpression.parameters[i]);
        }
        return newContext.equivalent(this.body, otherExpression.body);
    }

    public DBSPApplyExpression call(DBSPExpression... arguments) {
        if (arguments.length != this.parameters.length)
            throw new InternalCompilerError("Received " + arguments.length +
                    " arguments, but need " + this.parameters.length, this);
        return new DBSPApplyExpression(this, arguments);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        this.type.accept(visitor);
        for (DBSPParameter param: this.parameters)
            param.accept(visitor);
        this.body.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPClosureExpression o = other.as(DBSPClosureExpression.class);
        if (o == null)
            return false;
        return this.body == o.body &&
                this.hasSameType(o) &&
                Linq.same(this.parameters, o.parameters);
    }

    /** Check if the two closures have the same parameter names,
     * with the same corresponding types */
    public boolean sameParameters(DBSPClosureExpression other) {
        if (this.parameters.length != other.parameters.length)
            return false;
        for (int i = 0; i < this.parameters.length; i++) {
            DBSPParameter mine = this.parameters[i];
            DBSPParameter their = other.parameters[i];
            if (!mine.sameFields(their))
                return false;
        }
        return true;
    }

    /** A closure representing the identity function with the specified parameter. */
    public static DBSPClosureExpression id(DBSPParameter param) {
        return param.asVariable().closure(param);
    }

    /** A closure representing the identity function with the specified type. */
    public static DBSPClosureExpression id(DBSPType type) {
        DBSPParameter param = new DBSPParameter("t", type);
        return DBSPClosureExpression.id(param);
    }

    /** A closure representing the identity function with the ANY type. */
    public static DBSPClosureExpression id() {
        return DBSPClosureExpression.id(DBSPTypeAny.getDefault());
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("(|")
                .join(",", this.parameters)
                .append("| ")
                .append(this.body)
                .append(")");
    }

    /** Compose this closure by applying it after the 'before'
     * closure expression.  This closure must have exactly 1
     * parameter, while the before one can have multiple ones.
     * @param before Closure to compose. */
    public DBSPClosureExpression applyAfter(
            IErrorReporter errorReporter, DBSPClosureExpression before) {
        if (this.parameters.length != 1)
            throw new InternalCompilerError("Expected closure with 1 parameter", this);
        DBSPExpression apply = this.call(before.body.borrow());
        DBSPClosureExpression result = new DBSPClosureExpression(apply, before.parameters);
        BetaReduction reduction = new BetaReduction(errorReporter);
        Simplify simplify = new Simplify(errorReporter);
        IDBSPInnerNode reduced = reduction.apply(result);
        IDBSPInnerNode simplified = simplify.apply(reduced);
        return simplified.to(DBSPClosureExpression.class);
    }
}