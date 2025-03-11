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

import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.*;

/** This visitor rewrites a circuit by replacing each operator
 * recursively with an equivalent one.
 * The entire work is done in 'postorder' methods, except for ICircuit nodes.
 * Each operator is replaced in one of two cases:
 * - any of its inputs has changed
 * - the 'force' flag is 'true'.
 * This visitor is a base class for all visitors that modify circuits. */
public class CircuitCloneVisitor extends CircuitVisitor implements IWritesLogs, ICompilerComponent {
    /** For each {@link OutputPort} in the original circuit an {@link OutputPort} in the
     * result circuit which computes the same result. */
    protected final Map<OutputPort, OutputPort> remap;
    protected final Map<ICircuit, ICircuit> circuitRemap;
    protected final boolean force;
    protected final Set<IDBSPOuterNode> visited = new HashSet<>();
    /** Stack of circuits under construction.  Not the same as 'current': current
     * has nodes from the *old* circuit, whereas these are nodes in the *new* circuit. */
    protected final List<ICircuit> underConstruction;

    public CircuitCloneVisitor(DBSPCompiler compiler, boolean force) {
        super(compiler);
        this.remap = new HashMap<>();
        this.circuitRemap = new HashMap<>();
        this.force = force;
        this.underConstruction = new ArrayList<>();
    }

    public OutputPort mapped(OutputPort original) {
        return Utilities.getExists(this.remap, original);
    }

    /**
     * The output that used to be computed by 'old' is now
     * computed by 'newOp',
     * @param old    Operator in the previous circuit.
     * @param newOp  Operator replacing it in the new circuit.
     * @param add    If true add the operator to the new circuit.
     *               This may not be necessary if the operator has already been added. */
    protected void map(OutputPort old, OutputPort newOp, boolean add) {
        if (!old.equals(newOp)) {
            Logger.INSTANCE.belowLevel(this, 1)
                    .appendSupplier(this::toString)
                    .append(":")
                    .appendSupplier(old::toString)
                    .append(" -> ")
                    .appendSupplier(newOp::toString)
                    .newline();
        }
        Utilities.putNew(this.remap, old, newOp);
        if (add)
            this.addOperator(newOp.node());
    }

    protected void map(DBSPSimpleOperator old, DBSPSimpleOperator newOp, boolean add) {
        this.map(old.outputPort(), newOp.outputPort(), add);
    }

    protected void map(DBSPOperatorWithError old, DBSPOperatorWithError newOp, boolean add) {
        this.map(old.getOutput(0), newOp.getOutput(0), add);
        this.map(old.getOutput(1), newOp.getOutput(1), false);
    }

    protected void map(OutputPort old, OutputPort newOp) {
        this.map(old, newOp, true);
        assert old.node() == this.getCurrent();
        if (!old.equals(newOp)) {
            long derivedFrom = old.node().derivedFrom;
            newOp.node().setDerivedFrom(derivedFrom);
            assert old.outputType().sameType(newOp.outputType()) :
                    "Replacing operator with type\n" + old.outputType() +
                            " with new type\n" + newOp.outputType();
        }
    }

    protected void map(DBSPSimpleOperator old, DBSPSimpleOperator newOp) {
        this.map(old.outputPort(), newOp.outputPort());
    }

    protected void map(ICircuit old, ICircuit newOp) {
        if (newOp.is(DBSPOperator.class))
            this.addOperator(newOp.to(DBSPOperator.class));
        Utilities.putNew(this.circuitRemap, old, newOp);
    }

    /**
     * Add an operator to the produced circuit.
     * @param operator  Operator to add. */
    protected void addOperator(DBSPOperator operator) {
        Logger.INSTANCE.belowLevel(this, 2)
                .appendSupplier(this::toString)
                .append(" adding ")
                .appendSupplier(operator::toString)
                .newline();
        ICircuit parent = this.getUnderConstruction();
        parent.addOperator(operator);
        if (!this.current.isEmpty()) {
            // Current can be empty when operators are inserted in startVisit, for example.
            // Such operators are not derived from the "current" operator.
            IDBSPOuterNode node = this.getCurrent();
            if (node.is(ICircuit.class))
                return;
            if (operator != node)
                operator.setDerivedFrom(node.getDerivedFrom());
        }
    }

    /**
     * Replace the specified operator with an equivalent one
     * by replacing all the inputs with their replacements from the 'mapped' map.
     * @param operator  Operator to replace. */
    public void replace(DBSPSimpleOperator operator) {
        if (this.visited.contains(operator))
            // Graph can be a DAG
            return;
        this.visited.add(operator);
        List<OutputPort> sources = Linq.map(operator.inputs, this::mapped);
        if (!Linq.same(sources, operator.inputs)) {
            Logger.INSTANCE.belowLevel(this, 2)
                    .appendSupplier(this::toString)
                    .append(" replacing inputs of ")
                    .increase()
                    .appendSupplier(operator::toString)
                    .append(":")
                    .joinSupplier(", ", () -> Linq.map(operator.inputs, OutputPort::toString))
                    .newline()
                    .append("with:")
                    .joinSupplier(", ", () -> Linq.map(sources, OutputPort::toString))
                    .newline()
                    .decrease();
        }
        DBSPSimpleOperator result = operator.withInputs(sources, this.force);
        this.map(operator, result);
    }

    /**
     * Replace the specified operator with an equivalent one
     * by replacing all the inputs with their replacements from the 'mapped' map.
     * @param operator  Operator to replace. */
    public void replace(DBSPOperatorWithError operator) {
        if (this.visited.contains(operator))
            // Graph can be a DAG
            return;
        this.visited.add(operator);
        List<OutputPort> sources = Linq.map(operator.inputs, this::mapped);
        if (!Linq.same(sources, operator.inputs)) {
            Logger.INSTANCE.belowLevel(this, 2)
                    .append(this.toString())
                    .append(" replacing inputs of ")
                    .increase()
                    .append(operator.toString())
                    .append(":")
                    .joinSupplier(", ", () -> Linq.map(operator.inputs, OutputPort::toString))
                    .newline()
                    .append("with:")
                    .joinSupplier(", ", () -> Linq.map(sources, OutputPort::toString))
                    .newline()
                    .decrease();
        }
        DBSPOperatorWithError result = operator.withInputs(sources, this.force);
        result.setDerivedFrom(operator.derivedFrom);
        this.map(operator.getOutput(0), result.getOutput(0), true);
        this.map(operator.getOutput(1), result.getOutput(1), false);
    }

    @Override
    public void postorder(DBSPDeclaration declaration) {
        this.getUnderConstructionCircuit().addDeclaration(declaration);
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateWithWaterlineOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPNoopOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPWeighOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPApplyOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPDeltaOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPApply2Operator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPDelayOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPIndexedTopKOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPMapIndexOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPUnaryOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPStreamAggregateOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPConstantOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPAggregateZeroOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPNowOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPDifferentiateOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPStreamDistinctOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPFlatMapOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPFlatMapIndexOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPHopOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPIntegrateOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPUpsertFeedbackOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPDelayedIntegralOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPNegateOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPSinkOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPViewOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPSourceMultisetOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPViewDeclarationOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPSourceMapOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPSubtractOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPSumOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPStreamJoinOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPStreamAntiJoinOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPAntiJoinOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPJoinOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPStreamJoinIndexOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPJoinIndexOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPAsofJoinOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPJoinFilterMapOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPDistinctOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPDistinctIncrementalOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPAggregateOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPChainAggregateOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPChainOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPAggregateLinearPostprocessOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPAggregateLinearPostprocessRetainKeysOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPDeindexOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPPrimitiveAggregateOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPWindowOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPIntegrateTraceRetainValuesOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPWaterlineOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPControlledKeyFilterOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPOperatorWithError operator) { this.replace(operator); }

    public DBSPCircuit getUnderConstructionCircuit() {
        return this.underConstruction.get(0).to(DBSPCircuit.class);
    }

    public ICircuit getUnderConstruction() {
        return Utilities.last(this.underConstruction);
    }

    @Override
    public Token startVisit(IDBSPOuterNode circuit) {
        this.visited.clear();
        this.remap.clear();
        this.circuitRemap.clear();
        return super.startVisit(circuit);
    }

    @Override
    public VisitDecision preorder(DBSPNestedOperator operator) {
        if (this.visited.contains(operator))
            return VisitDecision.STOP;
        this.visited.add(operator);
        DBSPNestedOperator result = new DBSPNestedOperator(operator.getRelNode());
        this.underConstruction.add(result);
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(DBSPNestedOperator operator) {
        DBSPNestedOperator result = Utilities.removeLast(this.underConstruction).to(DBSPNestedOperator.class);
        result.setDerivedFrom(operator.derivedFrom);
        result.copyAnnotations(operator);
        if (result.sameCircuit(operator))
            result = operator;
        for (int i = 0; i < operator.outputCount(); i++) {
            OutputPort originalOutput = operator.internalOutputs.get(i);
            @Nullable OutputPort newPort = this.remap.get(originalOutput);
            // Output may have been deleted if it was unused, so newPort can be null
            if (result != operator)
                result.addOutput(operator.outputViews.get(i), newPort);
            this.map(new OutputPort(operator, i), new OutputPort(result, i), false);
        }
        assert operator.outputCount() == result.outputCount();
        this.map(operator, result);
    }

    @Override
    public VisitDecision preorder(DBSPCircuit circuit) {
        if (this.visited.contains(circuit))
            return VisitDecision.STOP;
        this.underConstruction.add(new DBSPCircuit(circuit.metadata));
        return VisitDecision.CONTINUE;
    }

    @Override
    public void postorder(DBSPCircuit circuit) {
        DBSPCircuit result = Utilities.removeLast(this.underConstruction).to(DBSPCircuit.class);
        if (result.sameCircuit(circuit)) {
            DBSPNode.discardOuterNode(result);
            result = circuit;
        } else {
            Logger.INSTANCE.belowLevel(this, 1)
                    .append("Circuit has changed").newline();
        }
        this.map(circuit, result);
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        this.startVisit(circuit);
        circuit.accept(this);
        this.endVisit();
        ICircuit result = Utilities.getExists(this.circuitRemap, circuit);
        return result.to(DBSPCircuit.class);
    }
}
