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

import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPInternOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperatorWithError;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.IInputOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** At the end of the visit the set 'keep' contains all
 * operators that are 'used' by other operators (inputs, outputs,
 * and sources). */
public class FindDeadCode extends CircuitVisitor implements IWritesLogs {
    public final Set<DBSPOperator> reachable = new HashSet<>();
    // Includes reachable plus all inputs
    public final Set<DBSPOperator> toKeep = new HashSet<>();
    /** If true all sources are kept, even if they are dead. */
    public final boolean keepAllSources;

    /**
     * Run the dead code visitor.
     * @param keepAllSources  If true all sources are kept, even if they are not used.
     */
    public FindDeadCode(DBSPCompiler compiler, boolean keepAllSources) {
        super(compiler);
        this.keepAllSources = keepAllSources;
    }

    public void keep(DBSPOperator operator) {
        Logger.INSTANCE.belowLevel(this, 1)
                .appendSupplier(operator::toString)
                .append(" reachable")
                .newline();
        this.toKeep.add(operator);
    }

    @Override
    public Token startVisit(IDBSPOuterNode node) {
        this.toKeep.clear();
        this.reachable.clear();
        return super.startVisit(node);
    }

    @Override
    public VisitDecision preorder(DBSPSourceMultisetOperator operator) {
        if (this.keepAllSources)
            this.keep(operator);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSourceMapOperator operator) {
        if (this.keepAllSources)
            this.keep(operator);
        return VisitDecision.STOP;
    }

    VisitDecision keepInverseReachable(OutputPort destination) {
        List<OutputPort> r = new ArrayList<>();
        r.add(destination);
        while (!r.isEmpty()) {
            OutputPort op = r.remove(0);
            this.reachable.add(op.node());
            if (op.node().is(DBSPNestedOperator.class)) {
                DBSPNestedOperator nested = op.node().to(DBSPNestedOperator.class);
                OutputPort internal = nested.internalOutputs.get(op.outputNumber);
                this.keepInverseReachable(internal);
            }
            if (this.toKeep.contains(op.node()))
                continue;
            this.keep(op.node());
            r.addAll(op.node().inputs);
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPIntegrateTraceRetainKeysOperator operator) {
        return this.keepInverseReachable(operator.outputPort());
    }

    @Override
    public VisitDecision preorder(DBSPIntegrateTraceRetainValuesOperator operator) {
        return this.keepInverseReachable(operator.outputPort());
    }

    @Override
    public VisitDecision preorder(DBSPSinkOperator operator) {
       return this.keepInverseReachable(operator.outputPort());
    }

    @Override
    public VisitDecision preorder(DBSPInternOperator operator) {
        return this.keepInverseReachable(operator.outputPort());
    }

    @Override
    public void endVisit() {
        for (IInputOperator source: this.getCircuit().sourceOperators.values()) {
            if (!this.reachable.contains(source.asOperator()) && !this.keepAllSources)
                this.compiler.reportWarning(source.getSourcePosition(),
                        "Unused", "Table " + source.getTableName().singleQuote() +
                                " is not used");
        }
        super.endVisit();
    }

    @Override
    public VisitDecision preorder(DBSPSimpleOperator operator) {
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPOperatorWithError operator) {
        return VisitDecision.STOP;
    }
}
