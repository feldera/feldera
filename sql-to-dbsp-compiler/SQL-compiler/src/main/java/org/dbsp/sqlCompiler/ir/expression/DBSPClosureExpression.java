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

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Expensive;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Maybe;

import java.util.List;

import static org.dbsp.util.Maybe.*;

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
        visitor.property("type");
        this.type.accept(visitor);
        visitor.startArrayProperty("parameters");
        int index = 0;
        for (DBSPParameter param: this.parameters) {
            visitor.propertyIndex(index);
            index++;
            param.accept(visitor);
        }
        visitor.endArrayProperty("parameters");
        visitor.property("body");
        this.body.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPClosureExpression o = other.as(DBSPClosureExpression.class);
        if (o == null)
            return false;
        return this.body == o.body &&
                this.hasSameType(o) &&
                Linq.same(this.parameters, o.parameters);
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
                .append("|").newline()
                .append(this.body)
                .append(")");
    }

    /** Compose this closure by applying it after the 'before'
     * closure expression.  This closure must have exactly 1
     * parameter, while the before one can have multiple ones.
     * @param before Closure to compose.
     * @param maybe If {@link Maybe#YES}, inline the call,
     *              If {@link Maybe#NO},  use a temporary variable,
     *              If {@link Maybe#MAYBE} use a heuristic. */
    public DBSPClosureExpression applyAfter(
            DBSPCompiler compiler, DBSPClosureExpression before, Maybe maybe) {
        if (this.parameters.length != 1)
            throw new InternalCompilerError("Expected closure with 1 parameter", this);

        if (maybe == MAYBE) {
            Expensive expensive = new Expensive(compiler);
            expensive.apply(before);
            if (expensive.isExpensive())
                maybe = NO;
            else
                maybe = YES;
        }

        if (maybe == YES) {
            DBSPExpression apply = this.call(before.body.borrow());
            return apply.closure(before.parameters).reduce(compiler).to(DBSPClosureExpression.class);
        } else {
            DBSPLetExpression let = new DBSPLetExpression(this.parameters[0].asVariable(),
                    before.body.borrow(), this.body);
            return let.closure(before.parameters);
        }
    }

    @SuppressWarnings("unused")
    public static DBSPClosureExpression fromJson(JsonNode node, JsonDecoder decoder) {
        // Need to read the type even if we don't use it, to populate the cache
        fromJsonInner(node, "type", decoder, DBSPType.class);
        DBSPExpression body = fromJsonInner(node, "body", decoder, DBSPExpression.class);
        List<DBSPParameter> parameters = fromJsonInnerList(node, "parameters", decoder, DBSPParameter.class);
        return new DBSPClosureExpression(CalciteObject.EMPTY, body, parameters.toArray(new DBSPParameter[0]));
    }
}