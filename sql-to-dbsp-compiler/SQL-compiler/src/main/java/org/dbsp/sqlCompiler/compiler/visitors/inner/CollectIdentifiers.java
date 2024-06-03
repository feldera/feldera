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

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitRewriter;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.DBSPEnumValue;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.path.DBSPSimplePathSegment;
import org.dbsp.sqlCompiler.ir.pattern.DBSPIdentifierPattern;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;

import java.util.Set;

/**
 * Discovers all identifiers used in a program.
 */
public class CollectIdentifiers extends InnerVisitor {
    final Set<String> used;

    /**
     * Create a visitor which collects all identifiers that appear in an inner program.
     * @param used  Deposit the identifiers in this set.
     */
    public CollectIdentifiers(IErrorReporter reporter, Set<String> used) {
        super(reporter);
        this.used = used;
    }

    @Override
    public void postorder(DBSPVariablePath node) {
        this.used.add(node.variable);
    }

    @Override
    public void postorder(DBSPFunction node) {
        this.used.add(node.name);
    }

    @Override
    public void postorder(DBSPEnumValue node) {
        this.used.add(node.enumName);
        this.used.add(node.constructor);
    }

    @Override
    public void postorder(DBSPIdentifierPattern pattern) {
        this.used.add(pattern.identifier);
    }

    @Override
    public void postorder(DBSPSimplePathSegment path) {
        this.used.add(path.identifier);
    }

    @Override
    public void postorder(DBSPLetStatement statement) {
        this.used.add(statement.variable);
    }

    @Override
    public void postorder(DBSPTypeUser type) {
        this.used.add(type.name);
    }

    static class OuterCollectIdentifiers extends CircuitRewriter {
        final Set<String> identifiers;

        OuterCollectIdentifiers(IErrorReporter reporter, Set<String> used, InnerVisitor visitor) {
            super(reporter, visitor);
            this.identifiers = used;
        }

        @Override
        public VisitDecision preorder(DBSPOperator operator) {
            this.identifiers.add(operator.getOutputName());
            return super.preorder(operator);
        }
    }

    /**
     * Returns a visitor that collects all identifiers from a circuit
     * and its functions.
     */
    @Override
    public CircuitRewriter getCircuitVisitor() {
        return new OuterCollectIdentifiers(this.errorReporter, this.used, this);
    }
}
