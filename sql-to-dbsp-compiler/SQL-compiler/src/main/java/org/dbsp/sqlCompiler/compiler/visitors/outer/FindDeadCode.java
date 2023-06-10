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

package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.IDBSPOuterNode;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.util.IModule;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import java.util.*;

/**
 * At the end of the visit the set 'keep' contains all
 * operators that are 'used' by other operators (inputs, outputs,
 * and sources).
 */
public class FindDeadCode extends CircuitVisitor implements IModule {
    public final Set<DBSPOperator> reachable = new HashSet<>();
    // Includes reachable plus all inputs
    public final Set<DBSPOperator> toKeep = new HashSet<>();
    public final boolean warn;

    /**
     * Run the dead code visitor.
     * @param reporter  Report errors here.
     * @param warn      If set warn about unused tables.
     */
    public FindDeadCode(IErrorReporter reporter, boolean warn) {
        super(reporter, true);
        this.warn = warn;
    }

    public void keep(DBSPOperator operator) {
        Logger.INSTANCE.from(this, 1)
                .append(operator.toString())
                .append(" reachable")
                .newline();
        this.toKeep.add(operator);
    }

    @Override
    public void startVisit(IDBSPOuterNode node) {
        this.toKeep.clear();
        this.reachable.clear();
        super.startVisit(node);
    }

    @Override
    public VisitDecision preorder(DBSPSourceOperator operator) {
        this.keep(operator);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSinkOperator operator) {
        List<DBSPOperator> r = new ArrayList<>();
        r.add(operator);
        while (!r.isEmpty()) {
            DBSPOperator op = r.remove(0);
            this.reachable.add(op);
            if (this.toKeep.contains(op))
                continue;
            this.keep(op);
            r.addAll(op.inputs);
        }
        return VisitDecision.STOP;
    }

    @Override
    public void endVisit() {
        for (DBSPOperator source: this.getCircuit().circuit.inputOperators) {
            if (!this.reachable.contains(source) && this.warn)
                this.errorReporter.reportError(source.getSourcePosition(), true,
                        "Unused", "Table " + Utilities.singleQuote(source.outputName) +
                                " is not used");
        }
        super.endVisit();
    }

    @Override
    public VisitDecision preorder(DBSPOperator operator) {
        return VisitDecision.STOP;
    }
}
