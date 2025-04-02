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
import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.VisitorProfiles;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.util.ICastable;
import org.dbsp.util.IHasId;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Depth-first traversal of an IDBSOuterNode hierarchy. */
@SuppressWarnings({"SameReturnValue", "BooleanMethodIsAlwaysInverted"})
public abstract class CircuitVisitor
        implements CircuitTransform, IWritesLogs, IHasId, ICompilerComponent, ICastable {
    final long id;
    static long crtId = 0;

    /** Used to force startVisit to call the base class,
     * since only the base class can access this object. */
    public static class Token {
        private Token() {}
    }
    static final Token TOKEN_INSTANCE = new Token();

    @Nullable
    protected DBSPCircuit circuit = null;
    public final DBSPCompiler compiler;
    /** Circuit or operator currently being visited. */
    protected final List<IDBSPOuterNode> context;

    public CircuitVisitor(DBSPCompiler compiler) {
        this.id = crtId++;
        this.compiler = compiler;
        this.context = new ArrayList<>();
    }

    @Override
    public DBSPCompiler compiler() {
        return this.compiler;
    }

    public DBSPCircuit getCircuit() {
        return Objects.requireNonNull(this.circuit);
    }

    public ICircuit getParent() {
        assert this.context.size() > 1;
        return this.context.get(this.context.size() - 2).to(ICircuit.class);
    }

    /** Property of a node that is being visited */
    public void label(String property) {}

    /** Property that has an array of values */
    public void startArrayProperty(String property) {}

    /** End a property that has an array of values */
    public void endArrayProperty(String property) {}

    /** Index of a property that has an array or list value */
    @SuppressWarnings("unused")
    public void propertyIndex(int index) {}

    public static final VisitorProfiles profiles = new VisitorProfiles();

    /** Override to initialize before visiting any node. */
    public Token startVisit(IDBSPOuterNode node) {
        profiles.start(this);
        if (node.is(DBSPCircuit.class))
            this.setCircuit(node.to(DBSPCircuit.class));
        return TOKEN_INSTANCE;
    }

    /** Override to finish after visiting all nodes. */
    public void endVisit() {
        assert this.circuit != null;
        this.circuit = null;
        profiles.stop(this);
    }

    public IDBSPOuterNode getCurrent() {
        return Utilities.last(this.context);
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
        this.context.add(node);
    }

    public void pop(IDBSPOuterNode node) {
        Utilities.removeLast(this.context, node);
    }

    /************************* PREORDER *****************************/

    // preorder methods return 'true' when normal traversal is desired,
    // and 'false' when the traversal should stop right away at the current node.
    // base classes

    public VisitDecision preorder(DBSPOperator ignored) {
        return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPSimpleOperator node) {
        return this.preorder(node.to(DBSPOperator.class));
    }

    public VisitDecision preorder(DBSPOperatorWithError node) {
        return this.preorder(node.to(DBSPOperator.class));
    }

    public void setCircuit(DBSPCircuit circuit) {
        if (this.circuit != null)
            throw new InternalCompilerError("Circuit is already set", circuit);
        this.circuit = circuit;
    }

    public VisitDecision preorder(DBSPDeclaration ignored) { return VisitDecision.CONTINUE; }

    public VisitDecision preorder(DBSPCircuit circuit) {
        return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(IDBSPOuterNode ignoredNode) { return VisitDecision.CONTINUE; }

    public VisitDecision preorder(DBSPUnaryOperator node) {
        return this.preorder(node.to(DBSPSimpleOperator.class));
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

    public VisitDecision preorder(DBSPDeltaOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPWeighOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPDeindexOperator node)  {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPSubtractOperator node) {
        return this.preorder(node.to(DBSPBinaryOperator.class));
    }

    public VisitDecision preorder(DBSPNestedOperator node) {
        return this.preorder(node.to(DBSPOperator.class));
    }

    public VisitDecision preorder(DBSPSumOperator node) {
        return this.preorder(node.to(DBSPSimpleOperator.class));
    }

    public VisitDecision preorder(DBSPJoinBaseOperator node) {
        return this.preorder(node.to(DBSPBinaryOperator.class));
    }

    public VisitDecision preorder(DBSPStreamJoinOperator node) {
        return this.preorder(node.to(DBSPJoinBaseOperator.class));
    }

    public VisitDecision preorder(DBSPStreamAntiJoinOperator node) {
        return this.preorder(node.to(DBSPBinaryOperator.class));
    }

    public VisitDecision preorder(DBSPAntiJoinOperator node) {
        return this.preorder(node.to(DBSPBinaryOperator.class));
    }

    public VisitDecision preorder(DBSPAggregateOperatorBase node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPChainAggregateOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPAggregateZeroOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPStreamAggregateOperator node) {
        return this.preorder((DBSPAggregateOperatorBase) node);
    }

    public VisitDecision preorder(DBSPAggregateOperator node) {
        return this.preorder((DBSPAggregateOperatorBase) node);
    }

    public VisitDecision preorder(DBSPAggregateLinearPostprocessOperator node) {
        return this.preorder((DBSPUnaryOperator) node);
    }

    public VisitDecision preorder(DBSPAggregateLinearPostprocessRetainKeysOperator node) {
        return this.preorder((DBSPBinaryOperator) node);
    }

    public VisitDecision preorder(DBSPPartitionedRollingAggregateOperator node) {
        return this.preorder((DBSPAggregateOperatorBase) node);
    }

    public VisitDecision preorder(DBSPPartitionedRollingAggregateWithWaterlineOperator node) {
        return this.preorder((DBSPBinaryOperator) node);
    }

    public VisitDecision preorder(DBSPConstantOperator node) {
        return this.preorder(node.to(DBSPSimpleOperator.class));
    }

    public VisitDecision preorder(DBSPNowOperator node) {
        return this.preorder(node.to(DBSPSimpleOperator.class));
    }

    public VisitDecision preorder(DBSPMapOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPChainOperator node) {
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

    public VisitDecision preorder(DBSPFlatMapIndexOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPHopOperator node) {
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

    public VisitDecision preorder(DBSPDistinctIncrementalOperator node) {
        return this.preorder(node.to(DBSPBinaryOperator.class));
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
        return this.preorder(node.to(DBSPSimpleOperator.class));
    }

    public VisitDecision preorder(DBSPSourceTableOperator node) {
        return this.preorder(node.to(DBSPSourceBaseOperator.class));
    }

    public VisitDecision preorder(DBSPSourceMultisetOperator node) {
        return this.preorder(node.to(DBSPSourceTableOperator.class));
    }

    public VisitDecision preorder(DBSPViewDeclarationOperator node) {
        return this.preorder(node.to(DBSPSourceTableOperator.class));
    }

    public VisitDecision preorder(DBSPSourceMapOperator node) {
        return this.preorder(node.to(DBSPSourceTableOperator.class));
    }

    public VisitDecision preorder(DBSPJoinOperator node) {
        return this.preorder(node.to(DBSPJoinBaseOperator.class));
    }

    public VisitDecision preorder(DBSPStreamJoinIndexOperator node) {
        return this.preorder(node.to(DBSPJoinBaseOperator.class));
    }

    public VisitDecision preorder(DBSPJoinIndexOperator node) {
        return this.preorder(node.to(DBSPJoinBaseOperator.class));
    }

    public VisitDecision preorder(DBSPAsofJoinOperator node) {
        return this.preorder(node.to(DBSPJoinBaseOperator.class));
    }

    public VisitDecision preorder(DBSPJoinFilterMapOperator node) {
        return this.preorder(node.to(DBSPJoinBaseOperator.class));
    }

    public VisitDecision preorder(DBSPPrimitiveAggregateOperator node) {
        return this.preorder(node.to(DBSPBinaryOperator.class));
    }

    public VisitDecision preorder(DBSPApplyOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    public VisitDecision preorder(DBSPApply2Operator node) {
        return this.preorder(node.to(DBSPBinaryOperator.class));
    }

    public VisitDecision preorder(DBSPWindowOperator node) {
        return this.preorder(node.to(DBSPBinaryOperator.class));
    }

    public VisitDecision preorder(DBSPControlledKeyFilterOperator node) {
        return this.preorder(node.to(DBSPOperatorWithError.class));
    }

    public VisitDecision preorder(DBSPIntegrateTraceRetainKeysOperator node) {
        return this.preorder(node.to(DBSPBinaryOperator.class));
    }

    public VisitDecision preorder(DBSPIntegrateTraceRetainValuesOperator node) {
        return this.preorder(node.to(DBSPBinaryOperator.class));
    }

    public VisitDecision preorder(DBSPWaterlineOperator node) {
        return this.preorder(node.to(DBSPUnaryOperator.class));
    }

    ////////////////////////////////////

    @SuppressWarnings("EmptyMethod")
    public void postorder(IDBSPOuterNode ignored) {}

    public void postorder(DBSPCircuit ignoredCircuit) {}

    public void postorder(DBSPOperator ignored) {}

    public void postorder(DBSPNestedOperator node) {
        this.postorder(node.to(DBSPOperator.class));
    }

    public void postorder(DBSPSimpleOperator node) { this.postorder(node.to(DBSPOperator.class)); }

    public void postorder(DBSPOperatorWithError node) { this.postorder(node.to(DBSPOperator.class)); }

    public void postorder(DBSPDeclaration ignored) {}

    public void postorder(DBSPUnaryOperator node) {
        this.postorder(node.to(DBSPSimpleOperator.class));
    }

    public void postorder(DBSPIndexedTopKOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPLagOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPSubtractOperator node) {
        this.postorder(node.to(DBSPBinaryOperator.class));
    }

    public void postorder(DBSPSumOperator node) {
        this.postorder(node.to(DBSPSimpleOperator.class));
    }

    public void postorder(DBSPJoinBaseOperator node) {
        this.postorder(node.to(DBSPBinaryOperator.class));
    }

    public void postorder(DBSPStreamJoinOperator node) {
        this.postorder(node.to(DBSPJoinBaseOperator.class));
    }

    public void postorder(DBSPStreamAntiJoinOperator node) {
        this.postorder(node.to(DBSPBinaryOperator.class));
    }

    public void postorder(DBSPAntiJoinOperator node) {
        this.postorder(node.to(DBSPBinaryOperator.class));
    }

    public void postorder(DBSPJoinOperator node) {
        this.postorder(node.to(DBSPJoinBaseOperator.class));
    }

    public void postorder(DBSPStreamJoinIndexOperator node) {
        this.postorder(node.to(DBSPJoinBaseOperator.class));
    }

    public void postorder(DBSPJoinIndexOperator node) {
        this.postorder(node.to(DBSPJoinBaseOperator.class));
    }

    public void postorder(DBSPAsofJoinOperator node) {
        this.postorder(node.to(DBSPJoinBaseOperator.class));
    }

    public void postorder(DBSPJoinFilterMapOperator node) {
        this.postorder(node.to(DBSPJoinBaseOperator.class));
    }

    public void postorder(DBSPAggregateOperatorBase node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPChainAggregateOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPAggregateZeroOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPStreamAggregateOperator node) {
        this.postorder((DBSPAggregateOperatorBase) node);
    }

    public void postorder(DBSPAggregateOperator node) {
        this.postorder((DBSPAggregateOperatorBase) node);
    }

    public void postorder(DBSPAggregateLinearPostprocessOperator node) {
        this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPAggregateLinearPostprocessRetainKeysOperator node) {
        this.postorder((DBSPBinaryOperator) node);
    }

    public void postorder(DBSPPartitionedRollingAggregateOperator node) {
        this.postorder((DBSPAggregateOperatorBase) node);
    }

    public void postorder(DBSPPartitionedRollingAggregateWithWaterlineOperator node) {
        this.postorder((DBSPBinaryOperator) node);
    }

    public void postorder(DBSPConstantOperator node) {
        this.postorder(node.to(DBSPSimpleOperator.class));
    }

    public void postorder(DBSPNowOperator node) {
        this.postorder(node.to(DBSPSimpleOperator.class));
    }

    public void postorder(DBSPMapOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPChainOperator node) {
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

    public void postorder(DBSPDeltaOperator node) {
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

    public void postorder(DBSPFlatMapIndexOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPHopOperator node) {
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

    public void postorder(DBSPDistinctIncrementalOperator node) {
        this.postorder(node.to(DBSPBinaryOperator.class));
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
        this.postorder(node.to(DBSPSimpleOperator.class));
    }

    public void postorder(DBSPSourceTableOperator node) {
        this.postorder(node.to(DBSPSourceBaseOperator.class));
    }

    public void postorder(DBSPSourceMultisetOperator node) {
        this.postorder(node.to(DBSPSourceTableOperator.class));
    }

    public void postorder(DBSPViewDeclarationOperator node) {
        this.postorder(node.to(DBSPSourceTableOperator.class));
    }

    public void postorder(DBSPSourceMapOperator node) {
        this.postorder(node.to(DBSPSourceTableOperator.class));
    }

    public void postorder(DBSPPrimitiveAggregateOperator node) {
        this.postorder(node.to(DBSPBinaryOperator.class));
    }

    public void postorder(DBSPApplyOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    public void postorder(DBSPApply2Operator node) {
        this.postorder(node.to(DBSPBinaryOperator.class));
    }

    public void postorder(DBSPWindowOperator node) {
        this.postorder(node.to(DBSPBinaryOperator.class));
    }

    public void postorder(DBSPControlledKeyFilterOperator node) {
        this.postorder(node.to(DBSPOperatorWithError.class));
    }

    public void postorder(DBSPIntegrateTraceRetainKeysOperator node) {
        this.postorder(node.to(DBSPBinaryOperator.class));
    }

    public void postorder(DBSPIntegrateTraceRetainValuesOperator node) {
        this.postorder(node.to(DBSPBinaryOperator.class));
    }

    public void postorder(DBSPWaterlineOperator node) {
        this.postorder(node.to(DBSPUnaryOperator.class));
    }

    @Override
    public String toString() {
        return this.id + " " + this.getClass().getSimpleName();
    }

    public String getName() { return this.toString(); }

    @Override
    public long getId() {
        return this.id;
    }
}
