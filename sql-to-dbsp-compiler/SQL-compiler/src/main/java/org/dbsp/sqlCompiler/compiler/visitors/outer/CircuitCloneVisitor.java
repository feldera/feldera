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

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.DBSPPartialCircuit;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.*;

/** This visitor rewrites a circuit by replacing each operator
 * recursively with an equivalent one.
 * Each operator is replaced in one of two cases:
 * - any of its inputs has changed
 * - the 'force' flag is 'true'.
 * We expect that this is a base class for all visitors which modify a circuit.
 * This visitor is a base class for all visitors that modify circuits. */
public class CircuitCloneVisitor extends CircuitVisitor implements IWritesLogs {
    @Nullable
    protected DBSPPartialCircuit result;
    /** For each operator in the original circuit an operator in the
     * result circuit which computes the same result. */
    protected final Map<DBSPOperator, DBSPOperator> remap;
    protected final boolean force;
    protected final Set<DBSPOperator> visited = new HashSet<>();

    public CircuitCloneVisitor(IErrorReporter reporter, boolean force) {
        super(reporter);
        this.remap = new HashMap<>();
        this.force = force;
    }

    public DBSPOperator mapped(DBSPOperator original) {
        return Utilities.getExists(this.remap, original);
    }

    public IDBSPOuterNode getCurrent() {
        return Utilities.last(this.current);
    }

    /**
     * The output that used to be computed by 'old' is now
     * computed by 'newOp',
     * @param old    Operator in the previous circuit.
     * @param newOp  Operator replacing it in the new circuit.
     * @param add    If true add the operator to the new circuit.
     *               This may not be necessary if the operator has already been added. */
    protected void map(DBSPOperator old, DBSPOperator newOp, boolean add) {
        if (old != newOp) {
            Logger.INSTANCE.belowLevel(this, 1)
                    .append(this.toString())
                    .append(":")
                    .append(old.toString())
                    .append(" -> ")
                    .append(newOp.toString())
                    .newline();
        }
        Utilities.putNew(this.remap, old, newOp);
        if (add)
            this.addOperator(newOp);
    }

    protected void map(DBSPOperator old, DBSPOperator newOp) {
        this.map(old, newOp, true);
        assert old == this.getCurrent();
        long derivedFrom = old.derivedFrom;
        if (derivedFrom == -1)
            derivedFrom = old.id;
        newOp.setDerivedFrom(derivedFrom);
    }

    /**
     * Add an operator to the produced circuit.
     * @param operator  Operator to add. */
    protected void addOperator(DBSPOperator operator) {
        Logger.INSTANCE.belowLevel(this, 2)
                .append(this.toString())
                .append(" adding ")
                .append(operator.toString())
                .newline();
        this.getResult().addOperator(operator);
        operator.setDerivedFrom(this.getCurrent().getId());
    }

    @Override
    public VisitDecision preorder(DBSPPartialCircuit circuit) {
        super.preorder(circuit);
        for (DBSPDeclaration node : circuit.declarations)
            node.accept(this);
        for (DBSPOperator node : circuit.getAllOperators())
            node.accept(this);
        return VisitDecision.STOP;
    }

    /**
     * Replace the specified operator with an equivalent one
     * by replacing all the inputs with their replacements from the 'mapped' map.
     * @param operator  Operator to replace. */
    public void replace(DBSPOperator operator) {
        if (this.visited.contains(operator))
            // Graph can be a DAG
            return;
        this.visited.add(operator);
        List<DBSPOperator> sources = Linq.map(operator.inputs, this::mapped);
        if (!Linq.same(sources, operator.inputs)) {
            Logger.INSTANCE.belowLevel(this, 1)
                    .append(this.toString())
                    .append(" replacing inputs of ")
                    .increase()
                    .append(operator.toString())
                    .append(":")
                    .join(", ", Linq.map(operator.inputs, DBSPOperator::toString))
                    .newline()
                    .append("with:")
                    .join(", ", Linq.map(sources, DBSPOperator::toString))
                    .newline()
                    .decrease();
        }
        DBSPOperator result = operator.withInputs(sources, this.force);
        result.setDerivedFrom(operator.id);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPDeclaration declaration) {
        this.getResult().addDeclaration(declaration);
    }

    @Override
    public void postorder(DBSPWindowAggregateOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPNoopOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPPartitionedTreeAggregateOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPWeighOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPApplyOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPDelayOperator operator) {
        if (operator.output == null) {
            this.replace(operator);
            return;
        }
        // If the delay has an output we must replace it too.
        this.visited.add(operator);
        List<DBSPOperator> sources = Linq.map(operator.inputs, this::mapped);
        DBSPOperator output = this.mapped(operator.output);
        DBSPOperator result = operator;
        if (operator.inputsDiffer(sources) || output != operator.output) {
            result = new DBSPDelayOperator(
                    operator.getNode(), operator.function,
                    sources.get(0), output.to(DBSPDelayOutputOperator.class));
        }
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPIndexedTopKOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPMapIndexOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPUnaryOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPPartitionedRadixTreeAggregateOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPPartitionedRollingAggregateOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPStreamAggregateOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPConstantOperator operator) {
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
    public void postorder(DBSPIndexOperator operator) {
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
    public void postorder(DBSPDelayOutputOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPSourceMultisetOperator operator) {
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
    public void postorder(DBSPUpsertOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPJoinOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPDistinctOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPAggregateOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPDeindexOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPPrimitiveAggregateOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPControlledFilterOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPWindowOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) { this.replace(operator); }

    @Override
    public void postorder(DBSPWaterlineOperator operator) { this.replace(operator); }

    public DBSPPartialCircuit getResult() {
        return Objects.requireNonNull(this.result);
    }

    @Override
    public void startVisit(IDBSPOuterNode circuit) {
        this.visited.clear();
        this.remap.clear();
        super.startVisit(circuit);
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        this.startVisit(circuit);
        this.result = new DBSPPartialCircuit(circuit.circuit.errorReporter, circuit.circuit.metadata);
        circuit.accept(this);
        this.endVisit();
        DBSPPartialCircuit result = this.getResult();
        if (circuit.circuit.sameCircuit(result))
            return circuit;
        return result.seal(circuit.name);
    }
}
