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
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainNValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperatorWithError;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.IInputOperator;
import org.dbsp.sqlCompiler.circuit.operator.ISplitInput;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
    /** Key: the end of an implicit back edge, a view declaration or the output half of a split
     * operator; value: the port that feeds the key on the back edge. */
    final Map<DBSPOperator, OutputPort> backEdges = new HashMap<>();

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
        this.backEdges.clear();
        return super.startVisit(node);
    }

    void addBackEdge(DBSPOperator end, @Nullable OutputPort source) {
        if (source != null)
            Utilities.putNew(this.backEdges, end, source);
    }

    /** Records the back edge of the split operator whose input half is {@code operator}, if any. */
    void addSplitBackEdge(DBSPOperator operator) {
        ISplitInput split = operator.as(ISplitInput.class);
        if (split != null)
            this.addBackEdge(split.splitOutput().outputPort().node(),
                    operator.to(DBSPSimpleOperator.class).outputPort());
    }

    /** Finds all back edges before any traversal starts. */
    @Override
    public VisitDecision preorder(DBSPCircuit circuit) {
        for (DBSPOperator operator : circuit.getAllOperators()) {
            this.addSplitBackEdge(operator);
            DBSPNestedOperator nested = operator.as(DBSPNestedOperator.class);
            if (nested == null)
                continue;
            for (DBSPOperator inner : nested.getAllOperators()) {
                this.addSplitBackEdge(inner);
                // In a nested circuit the internal output of a view feeds its declaration
                DBSPViewDeclarationOperator innerDeclaration = inner.as(DBSPViewDeclarationOperator.class);
                if (innerDeclaration != null)
                    this.addBackEdge(innerDeclaration, nested.internalOutputForDeclaration(innerDeclaration));
            }
        }
        return super.preorder(circuit);
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
        Deque<OutputPort> r = new ArrayDeque<>();
        r.add(destination);
        while (!r.isEmpty()) {
            OutputPort op = r.removeFirst();
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
            OutputPort backEdge = this.backEdges.get(op.node());
            // A declaration outside a recursive circuit has no readers
            DBSPViewDeclarationOperator declaration = op.node().as(DBSPViewDeclarationOperator.class);
            Utilities.enforce(declaration == null || backEdge != null,
                    () -> "No output of a recursive circuit feeds the declaration of view " +
                            declaration.originalViewName().singleQuote());
            if (backEdge != null)
                r.add(backEdge);
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
    public VisitDecision preorder(DBSPIntegrateTraceRetainNValuesOperator operator) {
        return this.keepInverseReachable(operator.outputPort());
    }

    @Override
    public VisitDecision preorder(DBSPSinkOperator operator) {
       return this.keepInverseReachable(operator.outputPort());
    }

    @Override
    public void endVisit() {
        for (IInputOperator source: this.getCircuit().sourceOperators.values()) {
            // The compiler reports unused view declarations elsewhere
            boolean declaration = source.asOperator().is(DBSPViewDeclarationOperator.class);
            if (!this.reachable.contains(source.asOperator()) && !this.keepAllSources && !declaration)
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
