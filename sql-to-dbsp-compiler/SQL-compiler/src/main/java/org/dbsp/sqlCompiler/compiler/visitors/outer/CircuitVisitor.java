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
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.circuit.DBSPPartialCircuit;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Depth-first traversal of an IDBSOuterPNode hierarchy. */
@SuppressWarnings({"SameReturnValue", "BooleanMethodIsAlwaysInverted"})
public abstract class CircuitVisitor
        implements CircuitTransform, IWritesLogs {
    @Nullable
    private DBSPCircuit circuit = null;
    public final IErrorReporter errorReporter;
    /** Circuit or operator currently being visited. */
    protected final List<IDBSPOuterNode> current;


    public CircuitVisitor(IErrorReporter errorReporter) {
        this.errorReporter = errorReporter;
        this.current = new ArrayList<>();
    }

    public DBSPCircuit getCircuit() {
        return Objects.requireNonNull(this.circuit);
    }

    /** Override to initialize before visiting any node. */
    public void startVisit(IDBSPOuterNode node) {
        if (node.is(DBSPCircuit.class))
            this.setCircuit(node.to(DBSPCircuit.class));
    }

    /** Override to finish after visiting all nodes. */
    public void endVisit() {
        this.circuit = null;
    }

    /** Returns by default the input circuit unmodified. */
    @Override
    public DBSPCircuit apply(DBSPCircuit node) {
        this.startVisit(node);
        node.accept(this);
        this.endVisit();
        return node;
    }

    public void push(IDBSPOuterNode node) {
        this.current.add(node);
    }

    public void pop(IDBSPOuterNode node) {
        IDBSPOuterNode previous = Utilities.removeLast(this.current);
        assert previous == node: "Unexpected node popped " + node + " expected " + previous;
    }

    /************************* PREORDER *****************************/

    // preorder methods return 'true' when normal traversal is desired,
    // and 'false' when the traversal should stop right away at the current node.
    // base classes
    public VisitDecision preorder(DBSPOperator node) { return VisitDecision.CONTINUE; }

    public void setCircuit(DBSPCircuit circuit) {
        if (this.circuit != null)
            throw new InternalCompilerError("Circuit is already set", circuit);
        this.circuit = circuit;
    }

    public VisitDecision preorder(DBSPDeclaration ignored) { return VisitDecision.CONTINUE; }

    public VisitDecision preorder(DBSPCircuit circuit) {
        return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPPartialCircuit circuit) {
        return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(IDBSPOuterNode ignoredNode) { return VisitDecision.CONTINUE; }

    public VisitDecision preorder(DBSPUnaryOperator node) {
        return this.preorder(node.to(DBSPOperator.class));
    }

    public VisitDecision preorder(DBSPIndexedTopKOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPLagOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPNoopOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPPartitionedTreeAggregateOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPWeighOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPDeindexOperator node)  {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPSubtractOperator node) {
        return this.preorder(node.to(DBSPOperator.class));
    }

    public VisitDecision preorder(DBSPSumOperator node) {
        return this.preorder(node.to(DBSPOperator.class));
    }

    public VisitDecision preorder(DBSPStreamJoinOperator node) {
        return this.preorder(node.to(DBSPOperator.class));
    }

    public VisitDecision preorder(DBSPUpsertOperator node) {
        return this.preorder(node.to(DBSPOperator.class));
    }

    public VisitDecision preorder(DBSPAggregateOperatorBase node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPStreamAggregateOperator node) {
        return this.preorder((DBSPAggregateOperatorBase) node);
    }

    public VisitDecision preorder(DBSPPartitionedRadixTreeAggregateOperator node) {
        return this.preorder(node.to(DBSPOperator.class));
    }

    public VisitDecision preorder(DBSPAggregateOperator node) {
        return this.preorder((DBSPAggregateOperatorBase) node);
    }

    public VisitDecision preorder(DBSPPartitionedRollingAggregateOperator node) {
        return this.preorder((DBSPAggregateOperatorBase) node);
    }

    public VisitDecision preorder(DBSPPartitionedRollingAggregateWithWaterlineOperator node) {
        return this.preorder((DBSPOperator) node);
    }

    public VisitDecision preorder(DBSPConstantOperator node) {
        return this.preorder(node.to(DBSPOperator.class));
    }

    public VisitDecision preorder(DBSPMapOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPMapIndexOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPDifferentiateOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPIntegrateOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPUpsertFeedbackOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPDelayedIntegralOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPNegateOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPDelayOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPFlatMapOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPFilterOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPStreamDistinctOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPDistinctOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPViewBaseOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPSinkOperator node) {
        return this.preorder(node.to(DBSPViewBaseOperator.class));
    }

    public VisitDecision preorder(DBSPViewOperator node) {
        return this.preorder(node.to(DBSPViewBaseOperator.class));
    }

    public VisitDecision preorder(DBSPSourceBaseOperator node) {
        return this.preorder(node.to(DBSPOperator.class));
    }

    public VisitDecision preorder(DBSPDelayOutputOperator node) {
        return this.preorder(node.to(DBSPSourceBaseOperator.class));
    }

    public VisitDecision preorder(DBSPSourceTableOperator node) {
        return this.preorder(node.to(DBSPSourceBaseOperator.class));
    }

    public VisitDecision preorder(DBSPSourceMultisetOperator node) {
        return this.preorder(node.to(DBSPSourceTableOperator.class));
    }

    public VisitDecision preorder(DBSPSourceMapOperator node) {
        return this.preorder(node.to(DBSPSourceTableOperator.class));
    }

    public VisitDecision preorder(DBSPJoinOperator node) {
        return this.preorder(node.to(DBSPOperator.class));
    }

    public VisitDecision preorder(DBSPPrimitiveAggregateOperator node) {
        return this.preorder(node.to(DBSPOperator.class));
    }

    public VisitDecision preorder(DBSPApplyOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPApply2Operator node) {
        return this.preorder(node.to(DBSPOperator.class));
    }

    public VisitDecision preorder(DBSPWindowOperator node) {
        return this.preorder(node.to(DBSPOperator.class));
    }

    public VisitDecision preorder(DBSPControlledFilterOperator node) {
        return this.preorder(node.to(DBSPOperator.class));
    }

    public VisitDecision preorder(DBSPIntegrateTraceRetainKeysOperator node) {
        return this.preorder(node.to(DBSPOperator.class));
    }

    public VisitDecision preorder(DBSPWaterlineOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    ////////////////////////////////////

    @SuppressWarnings("EmptyMethod")
    public void postorder(IDBSPOuterNode ignored) {}

    @SuppressWarnings("EmptyMethod")
    public void postorder(DBSPCircuit ignored) {}

    public void postorder(DBSPPartialCircuit ignoredCircuit) {}

    public void postorder(DBSPOperator ignored) {}

    public void postorder(DBSPDeclaration ignored) {}

    public void postorder(DBSPUnaryOperator node) {
        this.postorder(node.to(DBSPOperator.class));
    }

    public void postorder(DBSPIndexedTopKOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPLagOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPSubtractOperator node) {
        this.postorder(node.to(DBSPOperator.class));
    }

    public void postorder(DBSPSumOperator node) {
        this.postorder(node.to(DBSPOperator.class));
    }

    public void postorder(DBSPStreamJoinOperator node) {
        this.postorder(node.to(DBSPOperator.class));
    }

    public void postorder(DBSPUpsertOperator node) {
        this.postorder(node.to(DBSPOperator.class));
    }

    public void postorder(DBSPJoinOperator node) {
        this.postorder(node.to(DBSPOperator.class));
    }

    public void postorder(DBSPAggregateOperatorBase node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPStreamAggregateOperator node) {
        this.postorder((DBSPAggregateOperatorBase) node);
    }

    public void postorder(DBSPPartitionedRadixTreeAggregateOperator node) {
        this.postorder(node.to(DBSPOperator.class));
    }

    public void postorder(DBSPAggregateOperator node) {
        this.postorder((DBSPAggregateOperatorBase) node);
    }

    public void postorder(DBSPPartitionedRollingAggregateOperator node) {
        this.postorder((DBSPAggregateOperatorBase) node);
    }

    public void postorder(DBSPPartitionedRollingAggregateWithWaterlineOperator node) {
        this.postorder((DBSPOperator) node);
    }

    public void postorder(DBSPConstantOperator node) {
        this.postorder(node.to(DBSPOperator.class));
    }

    public void postorder(DBSPMapOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPMapIndexOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPDifferentiateOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPNoopOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPPartitionedTreeAggregateOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPWeighOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPDeindexOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPIntegrateOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPUpsertFeedbackOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPDelayedIntegralOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPNegateOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPDelayOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPFlatMapOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPFilterOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPStreamDistinctOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPDistinctOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPViewBaseOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPSinkOperator node) {
        this.postorder(node.to(DBSPViewBaseOperator.class));
    }

    public void postorder(DBSPViewOperator node) {
        this.postorder(node.to(DBSPViewBaseOperator.class));
    }

    public void postorder(DBSPSourceBaseOperator node) {
        this.postorder(node.to(DBSPOperator.class));
    }

    public void postorder(DBSPDelayOutputOperator node) { this.postorder(node.to(DBSPSourceBaseOperator.class));}

    public void postorder(DBSPSourceTableOperator node) {
        this.postorder(node.to(DBSPSourceBaseOperator.class));
    }

    public void postorder(DBSPSourceMultisetOperator node) {
        this.postorder(node.to(DBSPSourceTableOperator.class));
    }

    public void postorder(DBSPSourceMapOperator node) {
        this.postorder(node.to(DBSPSourceTableOperator.class));
    }

    public void postorder(DBSPPrimitiveAggregateOperator node) {
        this.postorder(node.to(DBSPOperator.class));
    }

    public void postorder(DBSPApplyOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPApply2Operator node) {
        this.postorder(node.to(DBSPOperator.class));
    }

    public void postorder(DBSPWindowOperator node) {
        this.postorder(node.to(DBSPOperator.class));
    }

    public void postorder(DBSPControlledFilterOperator node) {
        this.postorder(node.to(DBSPOperator.class));
    }

    public void postorder(DBSPIntegrateTraceRetainKeysOperator node) {
        this.postorder(node.to(DBSPOperator.class));
    }

    public void postorder(DBSPWaterlineOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
