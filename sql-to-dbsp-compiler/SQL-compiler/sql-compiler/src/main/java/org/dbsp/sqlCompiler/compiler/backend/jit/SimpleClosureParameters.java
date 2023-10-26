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

package org.dbsp.sqlCompiler.compiler.backend.jit;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.SubstitutionContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerRewriteVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.util.Logger;
import org.dbsp.util.NameGen;

import java.util.ArrayList;
import java.util.List;

/**
 * If a closure has parameter which is a tuple p = (a, b), replace it
 * with multiple parameters a, b.
 * This requires that all expressions involving the parameter are only field
 * accesses, e.g., p.1.
 */
public class SimpleClosureParameters
        extends InnerRewriteVisitor {
    final SubstitutionContext<List<DBSPVariablePath>> context;
    final NameGen generator;

    public SimpleClosureParameters(IErrorReporter reporter) {
        super(reporter);
        this.context = new SubstitutionContext<>();
        this.generator = new NameGen("p_");
    }

    @Override
    public VisitDecision preorder(DBSPType node) {
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPClosureExpression expression) {
        return super.preorder(expression);
    }

    @Override
    public VisitDecision preorder(DBSPFieldExpression field) {
        DBSPVariablePath var = field.expression.as(DBSPVariablePath.class);
        if (var != null) {
            List<DBSPVariablePath> fields = this.context.get(var.variable);
            if (fields != null) {
                DBSPExpression replacement = fields.get(field.fieldNo);
                this.map(field, replacement);
                return VisitDecision.STOP;
            }
        }
        return super.preorder(field);
    }

    @Override
    public VisitDecision preorder(DBSPVariablePath variable) {
        if (this.context.containsSubstitution(variable.variable))
            // We cannot allow accesses to the original parameter.
            throw new InternalCompilerError("Could not substitute all uses of " + variable, variable);
        this.map(variable, variable);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBlockExpression block) {
        this.context.newContext();
        super.preorder(block);
        this.context.popContext();
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPLetStatement statement) {
        super.preorder(statement);
        this.context.substitute(statement.variable, null);
        return VisitDecision.STOP;
    }

    @Override
    public void startVisit() {
        this.context.newContext();
        super.startVisit();
    }

    /**
     * Rewrite the parameters of the specified closure such that tuple-typed parameters
     * are decomposed into a list of simpler parameters each.  (This is not done recursively).
     */
    public DBSPClosureExpression rewriteClosure(DBSPClosureExpression closure) {
        this.startVisit();
        List<DBSPParameter> parameters = new ArrayList<>();
        for (DBSPParameter parameter: closure.parameters) {
            DBSPType type = parameter.getType();
            DBSPTypeTupleBase tuple = type.as(DBSPTypeTupleBase.class);
            if (tuple == null) {
                parameters.add(parameter);
                continue;
            }

            List<DBSPVariablePath> newParams = new ArrayList<>();
            for (DBSPType field: tuple.tupFields) {
                String name = this.generator.nextName();
                if (field.is(DBSPTypeTupleBase.class))
                    throw new InternalCompilerError("Tuple types nested too deeply", parameter);
                DBSPVariablePath newParam = new DBSPVariablePath(name, field);
                newParams.add(newParam);
                parameters.add(newParam.asParameter());
            }
            this.context.substitute(parameter.asVariableReference().variable, newParams);
        }
        DBSPClosureExpression result = closure;
        if (parameters.size() != closure.parameters.length) {
            DBSPExpression newBody = this.transform(closure.body);
            result = new DBSPClosureExpression(newBody, parameters.toArray(new DBSPParameter[0]));
        }
        this.endVisit();
        if (result != closure)
            Logger.INSTANCE.belowLevel(this, 2)
                    .append("SimpleClosureParameters replaces")
                    .newline()
                    .append(closure.toString())
                    .newline()
                    .append("with")
                    .newline()
                    .append(result.toString());
        return result;
    }

    @Override
    public IDBSPInnerNode apply(IDBSPInnerNode node) {
        return this.rewriteClosure(node.to(DBSPClosureExpression.class));
    }

    @Override
    public void endVisit() {
        this.context.popContext();
        this.context.mustBeEmpty();
        super.endVisit();
    }
}
