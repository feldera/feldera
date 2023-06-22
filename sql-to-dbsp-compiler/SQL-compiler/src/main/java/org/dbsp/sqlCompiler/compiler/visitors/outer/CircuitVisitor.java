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

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Depth-first traversal of an DBSPNode hierarchy.
 */
@SuppressWarnings({"SameReturnValue", "BooleanMethodIsAlwaysInverted"})
public abstract class CircuitVisitor
        implements CircuitTransform {
    /// If true each visit call will visit by default the superclass.
    final boolean visitSuper;
    @Nullable
    private DBSPCircuit circuit = null;
    public final IErrorReporter errorReporter;

    public CircuitVisitor(IErrorReporter errorReporter, boolean visitSuper) {
        assert visitSuper;
        this.visitSuper = true;
        this.errorReporter = errorReporter;
    }

    public DBSPCircuit getCircuit() {
        return Objects.requireNonNull(this.circuit);
    }

    /**
     * Override to initialize before visiting any node.
     */
    public void startVisit(IDBSPOuterNode node) {
        if (node.is(DBSPCircuit.class))
            this.setCircuit(node.to(DBSPCircuit.class));
    }

    /**
     * Override to finish after visiting all nodes.
     */
    public void endVisit() {
        this.circuit = null;
    }

    /**
     * Returns by default the input circuit unmodified.
     */
    @Override
    public DBSPCircuit apply(DBSPCircuit node) {
        this.startVisit(node);
        node.accept(this);
        this.endVisit();
        return node;
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

    public VisitDecision preorder(DBSPCircuit circuit) {
        return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPPartialCircuit circuit) {
        return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(IDBSPOuterNode ignoredNode) { return VisitDecision.CONTINUE; }

    public VisitDecision preorder(DBSPUnaryOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPIndexOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPNoopOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPSubtractOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPSumOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPJoinOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPAggregateOperatorBase node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPAggregateOperator node) {
        if (this.visitSuper) return this.preorder((DBSPAggregateOperatorBase) node);
        else return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPIncrementalAggregateOperator node) {
        if (this.visitSuper) return this.preorder((DBSPAggregateOperatorBase) node);
        else return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPWindowAggregateOperator node) {
        if (this.visitSuper) return this.preorder((DBSPAggregateOperatorBase) node);
        else return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPConstantOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPMapOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPMapIndexOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPDifferentialOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPIntegralOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPNegateOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPFlatMapOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPFilterOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPDistinctOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPIncrementalDistinctOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPSinkOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPSourceOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return VisitDecision.CONTINUE;
    }

    public VisitDecision preorder(DBSPIncrementalJoinOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return VisitDecision.CONTINUE;
    }

    ////////////////////////////////////

    @SuppressWarnings("EmptyMethod")
    public void postorder(DBSPCircuit ignored) {}

    public void postorder(DBSPPartialCircuit ignoredCircuit) {}

    public void postorder(DBSPOperator ignored) {}

    @SuppressWarnings("EmptyMethod")
    public void postorder(IDBSPOuterNode ignored) {}

    public void postorder(DBSPUnaryOperator node) {
        if (this.visitSuper) this.postorder((DBSPOperator) node);
    }

    public void postorder(DBSPIndexOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPSubtractOperator node) {
        if (this.visitSuper) this.postorder((DBSPOperator) node);
    }

    public void postorder(DBSPSumOperator node) {
        if (this.visitSuper) this.postorder((DBSPOperator) node);
    }

    public void postorder(DBSPJoinOperator node) {
        if (this.visitSuper) this.postorder((DBSPOperator) node);
    }

    public void postorder(DBSPIncrementalJoinOperator node) {
        if (this.visitSuper) this.postorder((DBSPOperator) node);
    }

    public void postorder(DBSPAggregateOperatorBase node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }


    public void postorder(DBSPAggregateOperator node) {
        if (this.visitSuper) this.postorder((DBSPAggregateOperatorBase) node);
    }

    public void postorder(DBSPIncrementalAggregateOperator node) {
        if (this.visitSuper) this.postorder((DBSPAggregateOperatorBase) node);
    }

    public void postorder(DBSPWindowAggregateOperator node) {
        if (this.visitSuper) this.postorder((DBSPAggregateOperatorBase) node);
    }

    public void postorder(DBSPConstantOperator node) {
        if (this.visitSuper) this.postorder((DBSPOperator) node);
    }

    public void postorder(DBSPMapOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPMapIndexOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPDifferentialOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPNoopOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPIntegralOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPNegateOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPFlatMapOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPFilterOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPDistinctOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPIncrementalDistinctOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPSinkOperator node) {
        if (this.visitSuper) this.postorder((DBSPOperator) node);
    }

    public void postorder(DBSPSourceOperator node) {
        if (this.visitSuper) this.postorder((DBSPOperator) node);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
