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

package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperatorWithError;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.util.Logger;

import java.util.Set;

public class RemoveOperators extends CircuitCloneVisitor {
    /** Keep all operators that appear in this list. */
    public final Set<DBSPOperator> keep;

    public RemoveOperators(DBSPCompiler compiler, Set<DBSPOperator> keep) {
        super(compiler, false);
        this.keep = keep;
    }

    @Override
    public VisitDecision preorder(DBSPSimpleOperator node) {
        if (this.keep.contains(node)) {
            this.replace(node);
        } else {
            Logger.INSTANCE.belowLevel(this, 2)
                    .append("Removing ")
                    .appendSupplier(node::toString)
                    .newline();
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPOperatorWithError node) {
        if (this.keep.contains(node)) {
            this.replaceMultiOutput(node);
        } else {
            Logger.INSTANCE.belowLevel(this, 2)
                    .append("Removing ")
                    .appendSupplier(node::toString)
                    .newline();
        }
        return VisitDecision.STOP;
    }

    @Override
    public Token startVisit(IDBSPOuterNode node) {
        Logger.INSTANCE.belowLevel(this, 2)
                .append("Keeping ")
                .appendSupplier(this.keep::toString)
                .newline();
        return super.startVisit(node);
    }
}
