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

package org.dbsp.sqlCompiler.ir;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.IDBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.IDBSPOuterNode;
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.circuit.DBSPPartialCircuit;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.util.IdGen;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.function.Function;

/**
 * Depth-first traversal of an DBSPNode hierarchy.
 */
@SuppressWarnings({"SameReturnValue", "BooleanMethodIsAlwaysInverted"})
public abstract class CircuitVisitor extends IdGen
        implements Function<DBSPCircuit, DBSPCircuit> {
    /// If true each visit call will visit by default the superclass.
    final boolean visitSuper;
    @Nullable
    private DBSPCircuit circuit = null;
    public final IErrorReporter errorReporter;

    public CircuitVisitor(IErrorReporter errorReporter, boolean visitSuper) {
        this.visitSuper = visitSuper;
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
    public boolean preorder(DBSPOperator node) { return true; }

    public void setCircuit(DBSPCircuit circuit) {
        if (this.circuit != null)
            throw new RuntimeException("Circuit is already set");
        this.circuit = circuit;
    }

    public boolean preorder(DBSPCircuit circuit) {
        return true;
    }

    public boolean preorder(DBSPPartialCircuit circuit) {
        return true;
    }

    public boolean preorder(IDBSPOuterNode ignoredNode) { return true; }

    public boolean preorder(IDBSPDeclaration node) {
        if (this.visitSuper) return this.preorder((IDBSPOuterNode) node);
        else return true;
    }

    public boolean preorder(DBSPUnaryOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return true;
    }

    public boolean preorder(DBSPIndexOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPNoopOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPSubtractOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return true;
    }

    public boolean preorder(DBSPSumOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return true;
    }

    public boolean preorder(DBSPJoinOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return true;
    }

    public boolean preorder(DBSPAggregateOperatorBase node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPAggregateOperator node) {
        if (this.visitSuper) return this.preorder((DBSPAggregateOperatorBase) node);
        else return true;
    }

    public boolean preorder(DBSPIncrementalAggregateOperator node) {
        if (this.visitSuper) return this.preorder((DBSPAggregateOperatorBase) node);
        else return true;
    }

    public boolean preorder(DBSPWindowAggregateOperator node) {
        if (this.visitSuper) return this.preorder((DBSPAggregateOperatorBase) node);
        else return true;
    }

    public boolean preorder(DBSPConstantOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return true;
    }

    public boolean preorder(DBSPMapOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPMapIndexOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPDifferentialOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPIntegralOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPNegateOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPFlatMapOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPFilterOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPDistinctOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPIncrementalDistinctOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPSinkOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return true;
    }

    public boolean preorder(DBSPSourceOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return true;
    }

    public boolean preorder(DBSPIncrementalJoinOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return true;
    }

    ////////////////////////////////////

    @SuppressWarnings("EmptyMethod")
    public void postorder(DBSPCircuit ignored) {}

    public void postorder(DBSPPartialCircuit ignoredCircuit) {}

    public void postorder(DBSPOperator ignored) {}

    @SuppressWarnings("EmptyMethod")
    public void postorder(IDBSPOuterNode ignored) {}

    public void postorder(IDBSPDeclaration node) { if (this.visitSuper) this.postorder((IDBSPOuterNode)node); }

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
