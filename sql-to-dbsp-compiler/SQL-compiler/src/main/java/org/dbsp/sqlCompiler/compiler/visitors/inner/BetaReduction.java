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

package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import java.util.HashSet;
import java.util.Set;

/**
 * Performs beta reduction on an Expression.
 * I.e., replace an application of a closure with the closure
 * body with arguments substituted.
 * If the arguments contain declarations, they are deep-copied for each
 * substitution.  This ensures that declarations have different IR nodes.
 * TODO: perform this in a subsequent alpha-renaming step instead of doing it
 * while expanding the code. */
public class BetaReduction extends InnerRewriteVisitor {
    final DeclarationValue<DBSPExpression> variableValue;
    /** Set of expressions to deepCopy before substitution */
    final Set<DBSPExpression> needsDeepCopy;
    final ResolveReferences resolver;

    /** Find out whether an expression contains any declarations */
    static class HasDeclarations extends InnerVisitor {
        boolean found = false;

        public HasDeclarations(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public VisitDecision preorder(DBSPType type) {
            return VisitDecision.STOP;
        }

        @Override
        public VisitDecision preorder(IDBSPInnerNode node) {
            if (node.is(IDBSPDeclaration.class)) {
                this.found = true;
                return VisitDecision.STOP;
            }
            return VisitDecision.CONTINUE;
        }

        @Override
        public void startVisit(IDBSPInnerNode node) {
            super.startVisit(node);
            this.found = false;
        }
    }

    public BetaReduction(DBSPCompiler compiler) {
        super(compiler, false);
        this.variableValue = new DeclarationValue<>();
        this.resolver = new ResolveReferences(compiler, true);
        this.needsDeepCopy = new HashSet<>();
    }

    @Override
    public VisitDecision preorder(DBSPApplyExpression expression) {
        HasDeclarations decls = new HasDeclarations(this.compiler);
        if (expression.function.is(DBSPClosureExpression.class)) {
            DBSPClosureExpression closure = expression.function.to(DBSPClosureExpression.class);
            if (closure.parameters.length != expression.arguments.length)
                throw new InternalCompilerError("Closure with " + closure.parameters.length +
                        " parameters called with " + expression.arguments.length + " arguments",
                        expression);
            this.push(expression);
            for (int i = 0; i < closure.parameters.length; i++) {
                DBSPParameter param = closure.parameters[i];
                DBSPExpression arg = this.transform(expression.arguments[i]);
                decls.apply(arg);
                this.variableValue.substitute(param, arg);
                if (decls.found)
                    this.needsDeepCopy.add(arg);
            }

            DBSPExpression newBody = this.transform(closure.body);
            this.pop(expression);

            this.map(expression, newBody);
            return VisitDecision.STOP;
        }
        super.preorder(expression);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPType type) {
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPVariablePath variable) {
        DBSPExpression replacement = null;
        IDBSPDeclaration declaration = this.resolver.reference.get(variable);
        if (declaration != null) {
            // Free variable mapped to itself
            replacement = this.variableValue.get(declaration);
        }
        if (replacement != null) {
            if (this.needsDeepCopy.contains(replacement))
                replacement = replacement.deepCopy();
            this.map(variable, replacement);
        } else {
            // Map the variable to itself - no replacement
            this.map(variable, variable);
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPLetStatement statement) {
        super.preorder(statement);
        return VisitDecision.STOP;
    }

    @Override
    public void startVisit(IDBSPInnerNode node) {
        this.resolver.apply(node);
        super.startVisit(node);
    }

    public DBSPExpression reduce(DBSPExpression expression) {
        return this.apply(expression).to(DBSPExpression.class);
    }
}
